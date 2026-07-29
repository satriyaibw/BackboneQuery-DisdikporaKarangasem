"""Helper penarikan berpaginasi dengan verifikasi kelengkapan.

Menarik seluruh halaman satu query (per NPSN / wilayah / ref), meng-upsert
tiap halaman, lalu memverifikasi jumlah baris diterima terhadap `total_rows`
yang dilaporkan API. Hasilnya (`PullResult`) dipakai flow untuk memutuskan
apakah entity itu lengkap, atau perlu dicatat ke dead-letter.
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PullResult:
    received: int
    expected: Optional[int]
    ok: bool
    reason: str          # "" bila ok; else "error" | "sp_error" | "incomplete"
    detail: str
    rows: List[dict] = field(default_factory=list)


async def pull_paginated(api, db, session, path, base_params, tbl_name, meta, logger,
                         last_update=None, per_page=500, collect=False,
                         write_lock=None) -> PullResult:
    received = 0
    expected: Optional[int] = None
    rows: List[dict] = []
    page = 1
    label = (base_params.get("npsn") or base_params.get("kode_wilayah")
             or base_params.get("ref") or tbl_name)

    while True:
        params = {**base_params, "page": page, "per_page": per_page}
        if last_update:
            params["last_update"] = last_update

        result = await api.get_with_retry(
            session, path, params, f"{tbl_name}/{label} hal {page}", logger)
        if result is None:
            return PullResult(received, expected, False, "error",
                              f"gagal setelah retry di halaman {page}", rows)

        data = result.get("data", [])
        if api.is_sp_error(data):
            return PullResult(received, expected, False, "sp_error",
                              data[0].get("keterangan", ""), rows)

        if expected is None:
            expected = result.get("total_rows")

        if data:
            if write_lock is not None:
                # Tulis diserialisasi (anti-deadlock) + non-blocking terhadap event loop.
                async with write_lock:
                    await asyncio.to_thread(db.upsert_rows, tbl_name, data, meta)
            else:
                db.upsert_rows(tbl_name, data, meta)
            received += len(data)
            if collect:
                rows.extend(data)

        if page >= result.get("total_pages", 1):
            break
        page += 1

    if expected is not None and received < expected:
        return PullResult(received, expected, False, "incomplete",
                          f"diterima {received} dari {expected}", rows)
    return PullResult(received, expected, True, "", f"{received} baris", rows)
