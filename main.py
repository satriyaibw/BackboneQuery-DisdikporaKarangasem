from dotenv import load_dotenv

# Muat .env lebih dulu — sebelum modul Prefect diimpor lewat backbone_pull.flow —
# agar variabel Prefect (mis. PREFECT_API_URL / PREFECT_API_KEY) dari .env sudah
# tersedia saat Prefect membaca konfigurasinya.
load_dotenv()

import argparse  # noqa: E402
import asyncio  # noqa: E402

from prefect.schedules import Cron  # noqa: E402

from backbone_pull.config import load_settings  # noqa: E402
from backbone_pull.flow import backbone_client_pull  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Penarikan data Backbone (Prefect serve).")
    p.add_argument("--run-once", action="store_true",
                   help="Jalankan flow sekali lalu keluar (tanpa scheduler).")
    return p


def main():
    args = build_parser().parse_args()
    if args.run_once:
        asyncio.run(backbone_client_pull())
        return
    s = load_settings()
    backbone_client_pull.serve(
        name=s.deployment_name,
        schedule=Cron(s.schedule_cron, timezone=s.schedule_timezone),
    )


if __name__ == "__main__":
    main()
