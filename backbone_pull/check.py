import asyncio

import aiohttp
from sqlalchemy import text

from .api import BackboneAPI
from .config import load_settings
from .db import get_adapter


def _ok(label, detail=""):
    print(f"  [ OK ] {label} {detail}".rstrip())


def _fail(label, err):
    print(f"  [GAGAL] {label}: {err}")


async def _check_api(api: BackboneAPI) -> bool:
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{api.base_url}/user-info/request",
                                    headers=api.headers) as resp:
                body = await resp.json()
        data = body.get("data", {})
        if "keterangan" in data:
            _fail("API Backbone", data["keterangan"])
            return False
        _ok("API Backbone", f"(request aktif s/d {data.get('expired_date')})")
        return True
    except Exception as e:  # noqa: BLE001
        _fail("API Backbone", e)
        return False


def main():
    s = load_settings()
    print("=== Konfigurasi ===")
    print(f"  Dialect   : {s.db_dialect}")
    print(f"  DB        : {s.db_user}@{s.db_host}:{s.db_port}/{s.db_name}")
    print(f"  Base URL  : {s.backbone_base_url}")
    print("=== Uji Koneksi ===")

    db = get_adapter(s)
    db_ok = False
    try:
        with db.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        _ok("Koneksi database")
        db_ok = True
    except Exception as e:  # noqa: BLE001
        _fail("Koneksi database", e)

    api = BackboneAPI(s.backbone_base_url, s.backbone_api_key, s.backbone_service_jwt)
    api_ok = asyncio.run(_check_api(api))

    print("=== Ringkasan ===")
    print("  Semua OK ✓" if (db_ok and api_ok) else "  Ada yang GAGAL ✗ — perbaiki .env lalu ulangi.")


if __name__ == "__main__":
    main()
