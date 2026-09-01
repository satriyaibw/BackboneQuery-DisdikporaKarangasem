import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp
from prefect import flow, task
from prefect.logging import get_run_logger

from .api import BackboneAPI
from .config import load_settings
from .db import get_adapter
from .delta import resolve_npsn_list
from .puller import pull_paginated
from .ref_bulk import (
    REF_BULK_CHECKPOINT_KEY,
    download_referensi_zip,
    get_manifest_generated_at,
    parse_referensi_zip,
)
from .request_info import pick_active_request
from .routing import route_tables
from .throttle import RateLimiter

settings = load_settings()
rate_limiter = RateLimiter(settings.backbone_rate_limit)
api = BackboneAPI(settings.backbone_base_url, settings.backbone_api_key,
                  settings.backbone_auth_url, settings.backbone_username,
                  settings.backbone_password, rate_limiter=rate_limiter)
db = get_adapter(settings)
PER_PAGE = settings.backbone_per_page
CONCURRENCY = settings.backbone_concurrency


def assert_complete(n_failures: int):
    """Gagalkan run bila masih ada item yang belum lengkap."""
    if n_failures:
        raise RuntimeError(
            f"Penarikan tidak lengkap: {n_failures} item masih gagal — lihat tabel "
            "sync.pull_failures. Item ini akan dicoba lagi otomatis di run berikutnya.")


@task(name="backbone-get-token", log_prints=True, retries=2, retry_delay_seconds=10)
async def get_access_token():
    logger = get_run_logger()
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await api.fetch_token(session)
    logger.info("Access token diperoleh.")


@task(name="backbone-prepare-infrastructure", log_prints=True)
def prepare_infrastructure():
    logger = get_run_logger()
    logger.info(f"Cek / buat database [{settings.db_name}]...")
    db.ensure_database()
    db.ensure_checkpoint_table()
    db.ensure_failures_table()
    db.ensure_pull_log_table()
    db.ensure_pull_requests_table()
    purged = db.purge_old_pull_log(settings.pull_log_retention_days)
    if purged:
        logger.info(f"Log aktivitas lama dihapus: {purged} baris (retensi {settings.pull_log_retention_days} hari).")
    logger.info("Infrastruktur siap.")


async def _read_error_detail(resp) -> str:
    """Ekstrak pesan error dari body respons gagal ({"detail": ...} atau {"data": {"keterangan": ...}})."""
    try:
        body = await resp.json()
    except Exception:  # noqa: BLE001
        return f"HTTP {resp.status}"
    if isinstance(body, dict):
        if "detail" in body:
            return str(body["detail"])
        data = body.get("data")
        if isinstance(data, dict) and "keterangan" in data:
            return str(data["keterangan"])
    return f"HTTP {resp.status}: {body}"


@task(name="backbone-create-request", log_prints=True, retries=2, retry_delay_seconds=10)
async def create_request():
    """Buat request akses baru; bila gagal (mis. bukan jadwal akses hari ini,
    atau request sudah pernah dibuat), pakai request aktif yang sudah ada —
    dibaca lewat GET /user-info/request — selama belum kedaluwarsa."""
    logger = get_run_logger()
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(f"{api.base_url}/user-info/request",
                                headers=api.headers) as resp:
            if resp.status < 400:
                body = await resp.json()
                data = body.get("data", {})
                if "keterangan" in data:
                    raise RuntimeError(f"Gagal buat request: {data['keterangan']}")
                logger.info(f"Request baru dibuat, aktif hingga: {data.get('expired_date')}")
                return data
            create_err = await _read_error_detail(resp)

        logger.warning(f"Gagal buat request baru ({create_err}) — cek request aktif yang sudah ada...")
        async with session.get(f"{api.base_url}/user-info/request",
                               headers=api.headers) as info_resp:
            info_resp.raise_for_status()
            info_body = await info_resp.json()

    active = pick_active_request(info_body.get("data", []), datetime.now())
    if active:
        logger.info(f"Memakai request aktif yang sudah ada, berlaku s/d {active.get('expired_date')}")
        return active

    raise RuntimeError(f"Tidak ada request akses aktif dan gagal membuat baru: {create_err}")


async def _authenticate_and_get_metadata() -> Tuple[Dict[str, dict], Optional[str]]:
    """Ambil access-token, buat/pakai-ulang request akses (simpan sesinya ke
    sync.pull_requests), lalu ambil metadata tabel — urutan setup yang sama
    dibutuhkan flow utama maupun retry_failed_only sebelum bisa memanggil
    endpoint data. Kembalikan (tables, request_id)."""
    await get_access_token()
    request_data = await create_request()
    request_id = request_data.get("request_id") if isinstance(request_data, dict) else None
    if request_id:
        expired_date = None
        exp_raw = request_data.get("expired_date")
        if exp_raw:
            try:
                expired_date = datetime.fromisoformat(exp_raw)
            except (TypeError, ValueError):
                expired_date = None
        db.upsert_pull_request(request_id, expired_date, json.dumps(request_data, default=str))
    tables = await get_metadata()
    return tables, request_id


@task(name="backbone-get-metadata", log_prints=True, retries=2, retry_delay_seconds=5)
async def get_metadata() -> Dict[str, dict]:
    logger = get_run_logger()
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await api.get(session, "/metadata", logger=logger, label="metadata")
    tables: Dict[str, dict] = {}
    for col in result.get("data", []):
        tbl = col["tbl_name"]
        if tbl not in tables:
            param_types_raw = col.get("param_types_available") or col.get("param_type", "npsn")
            tables[tbl] = {
                "param_type": col.get("param_type", "npsn"),
                "param_types_available": {p.strip() for p in param_types_raw.split(",") if p.strip()},
                "schema_name": col.get("schema_name", "dbo"),
                "pk_columns": [], "col_defs": [],
            }
        tables[tbl]["col_defs"].append(col)
        if col.get("primary_key"):
            tables[tbl]["pk_columns"].append(col["name"])
    get_run_logger().info(f"Tabel tersedia ({len(tables)}): {list(tables.keys())}")
    return tables


@task(name="backbone-ensure-tables", log_prints=True)
def ensure_target_tables(tables: Dict[str, dict]):
    logger = get_run_logger()
    for schema_name in {m["schema_name"] for m in tables.values()}:
        db.ensure_schema(schema_name)
        logger.info(f"Schema [{schema_name}] siap.")
    for tbl_name, meta in tables.items():
        db.ensure_table(tbl_name, meta["col_defs"], meta["pk_columns"], meta["schema_name"])
        logger.info(f"  [{meta['schema_name']}].[{tbl_name}] siap ({len(meta['col_defs'])} kolom)")


@task(name="backbone-get-wilayah", log_prints=True, retries=2, retry_delay_seconds=5)
async def get_wilayah_list() -> List[str]:
    logger = get_run_logger()
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await api.get(session, "/wilayah-akses-kecamatan",
                               logger=logger, label="wilayah-akses-kecamatan")
    kode_list = [row["kode_wilayah"].strip() for row in result.get("data", [])]
    logger.info(f"Wilayah akses: {len(kode_list)} kecamatan")
    return kode_list


@task(name="backbone-pull-sekolah", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_sekolah(kode_wilayah_list: List[str], meta: dict,
                       batch_id: Optional[str] = None,
                       request_id: Optional[str] = None) -> Tuple[List[str], dict]:
    logger = get_run_logger()
    last_update = db.get_last_update("sekolah")
    schema_name = meta.get("schema_name", "dbo")
    try:
        npsn_list = db.fetch_existing_npsn(schema_name, "sekolah")
    except Exception:
        npsn_list = []
    logger.info(f"Sekolah: {len(npsn_list)} NPSN dari target DB")

    started_at = datetime.now()
    sem = asyncio.Semaphore(CONCURRENCY)
    write_lock = asyncio.Lock()
    total = 0
    failed = 0
    collected: List[str] = []
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def _one(kode):
            nonlocal total, failed
            async with sem:
                res = await pull_paginated(
                    api, db, session, "/data/by-wilayah",
                    {"tbl_name": "sekolah", "kode_wilayah": kode},
                    "sekolah", meta, logger, last_update, PER_PAGE,
                    collect=True, write_lock=write_lock)
                total += res.received
                async with write_lock:
                    if res.ok:
                        await asyncio.to_thread(db.clear_failure, "sekolah", "wilayah", kode)
                    else:
                        failed += 1
                        logger.warning(f"Sekolah wilayah {kode}: {res.reason} — {res.detail}")
                        await asyncio.to_thread(db.record_failure, "sekolah", "wilayah", kode,
                                                res.reason, res.detail, res.expected, res.received)
                collected.extend(r["npsn"] for r in res.rows if r.get("npsn"))
        await asyncio.gather(*[_one(k) for k in kode_wilayah_list])

    npsn_list = list(dict.fromkeys(npsn_list + collected))
    if total:
        db.set_last_update("sekolah", datetime.now())
        db.add_checkpoint_count("sekolah", total)
    db.log_pull_summary("sekolah", "wilayah", started_at, datetime.now(),
                        total, len(kode_wilayah_list), failed,
                        batch_id, request_id)
    logger.info(f"Sekolah: {len(npsn_list)} NPSN total")
    stats = {"tbl_name": "sekolah", "param_type": "wilayah", "run_started_at": started_at,
             "rows_received": total, "entities_failed": failed}
    return npsn_list, stats


@task(name="backbone-pull-npsn", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_by_npsn(tbl_name: str, npsn_list: List[str], meta: dict,
                       batch_id: Optional[str] = None,
                       request_id: Optional[str] = None) -> dict:
    logger = get_run_logger()
    last_update = db.get_last_update(tbl_name)
    started_at = datetime.now()
    sem = asyncio.Semaphore(CONCURRENCY)
    write_lock = asyncio.Lock()
    total = 0
    failed = 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        effective_npsn_list = await resolve_npsn_list(
            api, session, tbl_name, last_update, npsn_list, logger)

        async def _one(npsn):
            nonlocal total, failed
            async with sem:
                res = await pull_paginated(
                    api, db, session, "/data/by-npsn",
                    {"npsn": npsn, "tbl_name": tbl_name},
                    tbl_name, meta, logger, last_update, PER_PAGE,
                    write_lock=write_lock)
                total += res.received
                async with write_lock:
                    if res.ok:
                        await asyncio.to_thread(db.clear_failure, tbl_name, "npsn", npsn)
                    else:
                        failed += 1
                        await asyncio.to_thread(db.record_failure, tbl_name, "npsn", npsn,
                                                res.reason, res.detail, res.expected, res.received)
        await asyncio.gather(*[_one(n) for n in effective_npsn_list])
    if total:
        db.set_last_update(tbl_name, datetime.now())
        db.add_checkpoint_count(tbl_name, total)
    db.log_pull_summary(tbl_name, "npsn", started_at, datetime.now(),
                        total, len(effective_npsn_list), failed,
                        batch_id, request_id)
    logger.info(f"{tbl_name}: {total} baris diproses ({len(effective_npsn_list)}/{len(npsn_list)} NPSN diproses)")
    return {"tbl_name": tbl_name, "param_type": "npsn", "run_started_at": started_at,
            "rows_received": total, "entities_failed": failed}


@task(name="backbone-pull-wilayah", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_by_wilayah(tbl_name: str, kode_wilayah_list: List[str], meta: dict,
                          batch_id: Optional[str] = None,
                          request_id: Optional[str] = None,
                          checkpoint_key: Optional[str] = None) -> dict:
    
    logger = get_run_logger()
    ckpt = checkpoint_key or tbl_name
    last_update = db.get_last_update(ckpt)
    started_at = datetime.now()
    sem = asyncio.Semaphore(CONCURRENCY)
    write_lock = asyncio.Lock()
    total = 0
    failed = 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def _one(kode):
            nonlocal total, failed
            async with sem:
                res = await pull_paginated(
                    api, db, session, "/data/by-wilayah",
                    {"tbl_name": tbl_name, "kode_wilayah": kode},
                    tbl_name, meta, logger, last_update, PER_PAGE,
                    write_lock=write_lock)
                total += res.received
                async with write_lock:
                    if res.ok:
                        await asyncio.to_thread(db.clear_failure, tbl_name, "wilayah", kode)
                    else:
                        failed += 1
                        await asyncio.to_thread(db.record_failure, tbl_name, "wilayah", kode,
                                                res.reason, res.detail, res.expected, res.received)
        await asyncio.gather(*[_one(k) for k in kode_wilayah_list])
    if total:
        db.set_last_update(ckpt, datetime.now())
        db.add_checkpoint_count(ckpt, total)
    db.log_pull_summary(tbl_name, "wilayah", started_at, datetime.now(),
                        total, len(kode_wilayah_list), failed,
                        batch_id, request_id)
    logger.info(f"{tbl_name}: {total} baris diproses")
    return {"tbl_name": tbl_name, "param_type": "wilayah", "run_started_at": started_at,
            "rows_received": total, "entities_failed": failed}


@task(name="backbone-pull-ref", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_ref(tbl_name: str, meta: dict,
                   batch_id: Optional[str] = None,
                   request_id: Optional[str] = None) -> dict:
    logger = get_run_logger()
    last_update = db.get_last_update(tbl_name)
    started_at = datetime.now()
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        res = await pull_paginated(
            api, db, session, "/referensi", {"ref": tbl_name},
            tbl_name, meta, logger, last_update, PER_PAGE)
    if res.ok:
        db.clear_failure(tbl_name, "ref", "")
    else:
        db.record_failure(tbl_name, "ref", "", res.reason,
                          res.detail, res.expected, res.received)
    if res.received:
        db.set_last_update(tbl_name, datetime.now())
        db.add_checkpoint_count(tbl_name, res.received)
    db.log_pull_summary(tbl_name, "ref", started_at, datetime.now(),
                        res.received, 1, 0 if res.ok else 1,
                        batch_id, request_id)
    logger.info(f"{tbl_name}: {res.received} baris diproses")
    return {"tbl_name": tbl_name, "param_type": "ref", "run_started_at": started_at,
            "rows_received": res.received, "entities_failed": 0 if res.ok else 1}


@task(name="backbone-pull-ref-bulk", log_prints=True)
async def pull_ref_bulk(tbl_ref: Dict[str, dict],
                        batch_id: Optional[str] = None,
                        request_id: Optional[str] = None) -> Tuple[List[dict], set]:
    logger = get_run_logger()
    stats: List[dict] = []
    loaded: set = set()
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        zip_bytes = await download_referensi_zip(session, api, logger)
    if not zip_bytes:
        return stats, loaded

    try:
        tables_rows, manifest = parse_referensi_zip(zip_bytes)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Gagal membaca ZIP referensi: {e} — pakai jalur lama (per-tabel).")
        return stats, loaded

    generated_at = get_manifest_generated_at(manifest)
    if generated_at is not None:
        last_checkpoint = db.get_last_update(REF_BULK_CHECKPOINT_KEY)
        if last_checkpoint is not None and last_checkpoint == str(generated_at.date()):
            loaded = {tbl_name for tbl_name in tables_rows if tbl_name in tbl_ref}
            logger.info(
                f"Referensi belum berubah sejak {last_checkpoint} "
                f"(manifest generated_at={generated_at}) — skip import {len(loaded)} tabel."
            )
            return stats, loaded

    expected_counts = {t["tbl_name"]: t.get("row_count") for t in manifest.get("tables", [])}
    for tbl_name, rows in tables_rows.items():
        meta = tbl_ref.get(tbl_name)
        if meta is None:
            continue  # tabel ada di ZIP tapi bukan bagian dari metadata param_type='ref' saat ini
        expected = expected_counts.get(tbl_name)
        if expected is not None and len(rows) != expected:
            logger.warning(
                f"{tbl_name}: baris ZIP ({len(rows)}) != manifest ({expected}) — lewati, pakai jalur lama.")
            continue
        started_at = datetime.now()
        total = 0
        try:
            for i in range(0, len(rows), PER_PAGE):
                chunk = rows[i:i + PER_PAGE]
                total += await asyncio.to_thread(db.upsert_rows, tbl_name, chunk, meta)
        except Exception as e:  # noqa: BLE001
            # Mis. NOT NULL violation -- CSV tidak bisa bedakan None vs ''
            # (Python csv.writer menulis keduanya sebagai field kosong), jadi
            # kolom yang boleh string kosong (bukan NULL) bisa salah ke-parse
            # jadi None di parse_referensi_zip(). Daripada menebak nullable
            # per kolom di situ, lewati tabel ini di sini -- jatuh ke jalur
            # lama (pull_ref via JSON per-tabel) yang tidak ambigu soal
            # None vs '' sama sekali.
            logger.warning(f"{tbl_name}: gagal upsert dari ZIP referensi ({e}) — lewati, pakai jalur lama.")
            continue
        finished_at = datetime.now()
        db.clear_failure(tbl_name, "ref", "")
        if total:
            db.set_last_update(tbl_name, finished_at)
            db.add_checkpoint_count(tbl_name, total)
        db.log_pull_summary(tbl_name, "ref", started_at, finished_at, total, 1, 0,
                            batch_id, request_id)
        logger.info(f"{tbl_name}: {total} baris dimuat lewat download ZIP referensi")
        stats.append({"tbl_name": tbl_name, "param_type": "ref", "run_started_at": started_at,
                      "rows_received": total, "entities_failed": 0})
        loaded.add(tbl_name)

    if generated_at is not None:
        db.set_last_update(REF_BULK_CHECKPOINT_KEY, generated_at)

    return stats, loaded


@task(name="backbone-retry-failed", log_prints=True)
async def retry_failed(tables: Dict[str, dict]) -> Dict[str, int]:
    logger = get_run_logger()
    recovered_rows: Dict[str, int] = {}
    failures = db.list_failures()
    if not failures:
        return recovered_rows
    logger.info(f"Retry {len(failures)} item gagal (full pull, tanpa filter incremental)...")
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for f in failures:
            tbl, pt, ent = f["tbl_name"], f["param_type"], f["entity_id"]
            meta = tables.get(tbl)
            if not meta:
                continue
            if pt == "npsn":
                path, base = "/data/by-npsn", {"npsn": ent, "tbl_name": tbl}
            elif pt == "wilayah":
                path, base = "/data/by-wilayah", {"tbl_name": tbl, "kode_wilayah": ent}
            elif pt == "ref":
                path, base = "/referensi", {"ref": tbl}
            else:
                continue
            res = await pull_paginated(
                api, db, session, path, base, tbl, meta, logger,
                last_update=None, per_page=PER_PAGE, collect=(tbl == "sekolah"))
            if res.ok:
                db.clear_failure(tbl, pt, ent)
                logger.info(f"  pulih: {tbl}/{pt}/{ent} ({res.received} baris)")
                recovered_rows[tbl] = recovered_rows.get(tbl, 0) + res.received
            else:
                db.record_failure(tbl, pt, ent, res.reason, res.detail,
                                  res.expected, res.received)
    return recovered_rows


@flow(name="backbone-client-pull", log_prints=True)
async def backbone_client_pull():
    logger = get_run_logger()
    logger.info(f"▶ Mulai penarikan data Backbone — {datetime.now():%Y-%m-%d %H:%M}")
    batch_id = str(uuid.uuid4())
    prepare_infrastructure()
    tables, request_id = await _authenticate_and_get_metadata()
    if not tables:
        logger.warning("Metadata kosong, tidak ada tabel yang dapat ditarik.")
        return
    ensure_target_tables(tables)
    kode_wilayah_list = await get_wilayah_list()
    if not kode_wilayah_list:
        logger.warning("Tidak ada wilayah akses.")
        return
    sekolah_meta = tables.get("sekolah", {
        "param_type": "wilayah", "schema_name": "dbo",
        "pk_columns": ["sekolah_id"], "col_defs": [],
    })
    npsn_list, sekolah_stats = await pull_sekolah(kode_wilayah_list, sekolah_meta,
                                                  batch_id, request_id)
    run_stats: List[dict] = [sekolah_stats]
    tbl_npsn, tbl_wilayah, tbl_ref = route_tables(tables)
    if npsn_list:
        for tbl_name, meta in tbl_npsn.items():
            logger.info(f"Tarik (npsn) → [{meta['schema_name']}].[{tbl_name}]")
            run_stats.append(await pull_by_npsn(tbl_name, npsn_list, meta,
                                                batch_id, request_id))
    else:
        logger.warning("NPSN list kosong, lewati tabel param_type='npsn'.")
    for tbl_name, meta in tbl_wilayah.items():
        logger.info(f"Tarik (wilayah) → [{meta['schema_name']}].[{tbl_name}]")
        run_stats.append(await pull_by_wilayah(tbl_name, kode_wilayah_list, meta,
                                               batch_id, request_id))
    
    for tbl_name, meta in tbl_npsn.items():
        if "wilayah" not in meta.get("param_types_available", set()):
            continue
        logger.info(f"Tarik (wilayah, tambahan) → [{meta['schema_name']}].[{tbl_name}] "
                   "(param_types_available juga sebut wilayah -- jangkau baris tanpa npsn)")
        run_stats.append(await pull_by_wilayah(tbl_name, kode_wilayah_list, meta,
                                               batch_id, request_id,
                                               checkpoint_key=f"{tbl_name}__wilayah"))
    if settings.pull_ref:
        bulk_stats, bulk_loaded = await pull_ref_bulk(tbl_ref, batch_id, request_id)
        run_stats.extend(bulk_stats)
        for tbl_name, meta in tbl_ref.items():
            if tbl_name in bulk_loaded:
                continue
            logger.info(f"Tarik (ref) → [{meta['schema_name']}].[{tbl_name}]")
            run_stats.append(await pull_ref(tbl_name, meta, batch_id, request_id))

    
    recovered_rows = await retry_failed(tables)
    remaining_by_table: Dict[str, int] = {}
    for f in db.list_failures():
        remaining_by_table[f["tbl_name"]] = remaining_by_table.get(f["tbl_name"], 0) + 1
    for stat in run_stats:
        tbl = stat["tbl_name"]
        final_received = stat["rows_received"] + recovered_rows.get(tbl, 0)
        final_failed = remaining_by_table.get(tbl, 0)
        db.update_pull_log(tbl, stat["param_type"], stat["run_started_at"],
                           final_received, final_failed)

    n_failures = db.count_failures()
    assert_complete(n_failures)
    logger.info(f"✓ Selesai — 100% lengkap — {datetime.now():%Y-%m-%d %H:%M}")


@flow(name="backbone-client-retry-failed", log_prints=True)
async def retry_failed_only():
    logger = get_run_logger()
    prepare_infrastructure()

    failures_before = db.list_failures()
    if not failures_before:
        logger.info("Tidak ada item di sync.pull_failures — tidak ada yang perlu di-retry.")
        return

    by_key_before: Dict[Tuple[str, str], int] = {}
    for f in failures_before:
        key = (f["tbl_name"], f["param_type"])
        by_key_before[key] = by_key_before.get(key, 0) + 1
    logger.info(f"▶ Retry-only: {len(failures_before)} item gagal di "
                f"{len({k[0] for k in by_key_before})} tabel — {datetime.now():%Y-%m-%d %H:%M}")

    tables, _request_id = await _authenticate_and_get_metadata()
    if not tables:
        logger.warning("Metadata kosong, tidak bisa retry.")
        return

    recovered_rows = await retry_failed(tables)

    by_key_after: Dict[Tuple[str, str], int] = {}
    for f in db.list_failures():
        key = (f["tbl_name"], f["param_type"])
        by_key_after[key] = by_key_after.get(key, 0) + 1

    for (tbl_name, param_type), total_before in by_key_before.items():
        rows = recovered_rows.get(tbl_name, 0)
        still_failed = by_key_after.get((tbl_name, param_type), 0)
        db.update_latest_pull_log(tbl_name, param_type, rows, still_failed)
        logger.info(f"  {tbl_name}: {total_before - still_failed}/{total_before} pulih ({rows} baris)")

    n_failures = db.count_failures()
    assert_complete(n_failures)
    logger.info(f"✓ Retry-only selesai — {len(failures_before) - n_failures}/{len(failures_before)} pulih "
               f"— {datetime.now():%Y-%m-%d %H:%M}")
