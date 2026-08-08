"""Download & parsing prefill tabel referensi lewat GET /referensi/download.

Backbone menyediakan (opsional, tergantung versi server) satu ZIP berisi
snapshot seluruh tabel referensi (satu CSV per tabel + manifest.json) sebagai
alternatif yang jauh lebih hemat request dibanding menarik tiap tabel
referensi satu per satu lewat endpoint /referensi berpaginasi. Modul ini
murni untuk mengunduh + mem-parsing ZIP tersebut — pemanggil (flow.py)
yang memutuskan tabel mana yang dipakai dari hasilnya dan mana yang perlu
jatuh ke jalur lama (per-tabel) bila tidak tersedia di ZIP.
"""

import csv
import io
import json
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

import aiohttp


async def download_referensi_zip(session, api, logger=None) -> Optional[bytes]:
    """Unduh ZIP prefill referensi. Kembalikan None (bukan raise) bila server
    belum mendukung fitur ini (404/503) atau gagal karena sebab lain —
    pemanggil diharapkan jatuh ke jalur lama (per-tabel) pada kasus ini."""
    url = f"{api.base_url}/referensi/download"
    try:
        async with session.get(url, headers=api.headers) as resp:
            if resp.status == 404:
                if logger:
                    logger.info("Prefill referensi belum pernah di-generate di server — pakai jalur lama (per-tabel).")
                return None
            if resp.status == 503:
                if logger:
                    logger.info("Download referensi belum dikonfigurasi di server — pakai jalur lama (per-tabel).")
                return None
            if resp.status >= 400:
                if logger:
                    logger.warning(f"Download referensi gagal (HTTP {resp.status}) — pakai jalur lama (per-tabel).")
                return None
            return await resp.read()
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        if logger:
            logger.warning(f"Download referensi gagal ({e}) — pakai jalur lama (per-tabel).")
        return None


def parse_referensi_zip(zip_bytes: bytes) -> Tuple[Dict[str, List[dict]], dict]:
    """Baca ZIP prefill referensi: {tbl_name}.csv per tabel + manifest.json
    opsional ({"tables": [{"tbl_name", "row_count"}, ...]}). String kosong
    (representasi NULL dari export CSV di sisi server) dikonversi ke None."""
    tables: Dict[str, List[dict]] = {}
    manifest: dict = {}
    with ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if "manifest.json" in names:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        for name in names:
            if not name.endswith(".csv"):
                continue
            tbl_name = name[:-len(".csv")]
            content = zf.read(name).decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))
            tables[tbl_name] = [
                {k: (v if v != "" else None) for k, v in row.items()}
                for row in reader
            ]
    return tables, manifest
