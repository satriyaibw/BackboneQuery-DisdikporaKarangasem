from datetime import datetime
from typing import Dict, List

import aiohttp
from prefect import flow, task
from prefect.logging import get_run_logger

from .api import BackboneAPI
from .config import load_settings
from .db import get_adapter
from .puller import pull_paginated
from .routing import route_tables

settings = load_settings()
api = BackboneAPI(settings.backbone_base_url, settings.backbone_api_key,
                  settings.backbone_auth_url, settings.backbone_username,
                  settings.backbone_password)
db = get_adapter(settings)
PER_PAGE = settings.backbone_per_page


@task(name="backbone-get-token", log_prints=True, retries=2, retry_delay_seconds=10)
async def get_access_token():
    """Tukar username/password → access-token, dipakai untuk semua request run ini."""
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
    logger.info("Infrastruktur siap.")


@task(name="backbone-create-request", log_prints=True, retries=2, retry_delay_seconds=10)
async def create_request():
    logger = get_run_logger()
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(f"{api.base_url}/user-info/request",
                                headers=api.headers) as resp:
            resp.raise_for_status()
            body = await resp.json()
            data = body.get("data", {})
            if "keterangan" in data:
                raise RuntimeError(f"Gagal buat request: {data['keterangan']}")
            logger.info(f"Request aktif hingga: {data.get('expired_date')}")
            return data


@task(name="backbone-get-metadata", log_prints=True, retries=2, retry_delay_seconds=5)
async def get_metadata() -> Dict[str, dict]:
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await api.get(session, "/metadata")
    tables: Dict[str, dict] = {}
    for col in result.get("data", []):
        tbl = col["tbl_name"]
        if tbl not in tables:
            tables[tbl] = {
                "param_type": col.get("param_type", "npsn"),
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
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await api.get(session, "/wilayah-akses-kecamatan")
    kode_list = [row["kode_wilayah"].strip() for row in result.get("data", [])]
    get_run_logger().info(f"Wilayah akses: {len(kode_list)} kecamatan")
    return kode_list


@task(name="backbone-pull-sekolah", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_sekolah(kode_wilayah_list: List[str], meta: dict) -> List[str]:
    logger = get_run_logger()
    last_update = db.get_last_update("sekolah")
    schema_name = meta.get("schema_name", "dbo")
    try:
        npsn_list = db.fetch_existing_npsn(schema_name, "sekolah")
    except Exception:
        npsn_list = []
    logger.info(f"Sekolah: {len(npsn_list)} NPSN dari target DB")

    total = 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for kode in kode_wilayah_list:
            res = await pull_paginated(
                api, db, session, "/data/by-wilayah",
                {"tbl_name": "sekolah", "kode_wilayah": kode},
                "sekolah", meta, logger, last_update, PER_PAGE, collect=True)
            total += res.received
            if res.ok:
                db.clear_failure("sekolah", "wilayah", kode)
            else:
                logger.warning(f"Sekolah wilayah {kode}: {res.reason} — {res.detail}")
                db.record_failure("sekolah", "wilayah", kode, res.reason,
                                  res.detail, res.expected, res.received)
            npsn_list += [r["npsn"] for r in res.rows if r.get("npsn")]

    if total:
        db.set_last_update("sekolah", datetime.now())
        db.add_checkpoint_count("sekolah", total)
    npsn_list = list(dict.fromkeys(npsn_list))
    logger.info(f"Sekolah: {len(npsn_list)} NPSN total")
    return npsn_list


@task(name="backbone-pull-npsn", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_by_npsn(tbl_name: str, npsn_list: List[str], meta: dict):
    logger = get_run_logger()
    last_update = db.get_last_update(tbl_name)
    total = 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for npsn in npsn_list:
            res = await pull_paginated(
                api, db, session, "/data/by-npsn",
                {"npsn": npsn, "tbl_name": tbl_name},
                tbl_name, meta, logger, last_update, PER_PAGE)
            total += res.received
            if res.ok:
                db.clear_failure(tbl_name, "npsn", npsn)
            else:
                db.record_failure(tbl_name, "npsn", npsn, res.reason,
                                  res.detail, res.expected, res.received)
    if total:
        db.set_last_update(tbl_name, datetime.now())
        db.add_checkpoint_count(tbl_name, total)
    logger.info(f"{tbl_name}: {total} baris diproses")


@task(name="backbone-pull-wilayah", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_by_wilayah(tbl_name: str, kode_wilayah_list: List[str], meta: dict):
    logger = get_run_logger()
    last_update = db.get_last_update(tbl_name)
    total = 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for kode in kode_wilayah_list:
            res = await pull_paginated(
                api, db, session, "/data/by-wilayah",
                {"tbl_name": tbl_name, "kode_wilayah": kode},
                tbl_name, meta, logger, last_update, PER_PAGE)
            total += res.received
            if res.ok:
                db.clear_failure(tbl_name, "wilayah", kode)
            else:
                db.record_failure(tbl_name, "wilayah", kode, res.reason,
                                  res.detail, res.expected, res.received)
    if total:
        db.set_last_update(tbl_name, datetime.now())
        db.add_checkpoint_count(tbl_name, total)
    logger.info(f"{tbl_name}: {total} baris diproses")


@task(name="backbone-pull-ref", log_prints=True, retries=2, retry_delay_seconds=10)
async def pull_ref(tbl_name: str, meta: dict):
    logger = get_run_logger()
    last_update = db.get_last_update(tbl_name)
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
    logger.info(f"{tbl_name}: {res.received} baris diproses")


@flow(name="backbone-client-pull", log_prints=True)
async def backbone_client_pull():
    logger = get_run_logger()
    logger.info(f"▶ Mulai penarikan data Backbone — {datetime.now():%Y-%m-%d %H:%M}")
    prepare_infrastructure()
    await get_access_token()
    await create_request()
    tables = await get_metadata()
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
    npsn_list = await pull_sekolah(kode_wilayah_list, sekolah_meta)
    tbl_npsn, tbl_wilayah, tbl_ref = route_tables(tables)
    if npsn_list:
        for tbl_name, meta in tbl_npsn.items():
            logger.info(f"Tarik (npsn) → [{meta['schema_name']}].[{tbl_name}]")
            await pull_by_npsn(tbl_name, npsn_list, meta)
    else:
        logger.warning("NPSN list kosong, lewati tabel param_type='npsn'.")
    for tbl_name, meta in tbl_wilayah.items():
        logger.info(f"Tarik (wilayah) → [{meta['schema_name']}].[{tbl_name}]")
        await pull_by_wilayah(tbl_name, kode_wilayah_list, meta)
    if settings.pull_ref:
        for tbl_name, meta in tbl_ref.items():
            logger.info(f"Tarik (ref) → [{meta['schema_name']}].[{tbl_name}]")
            await pull_ref(tbl_name, meta)
    logger.info(f"✓ Selesai — {datetime.now():%Y-%m-%d %H:%M}")
