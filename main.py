import argparse
import asyncio

from backbone_pull.config import load_settings
from backbone_pull.flow import backbone_client_pull


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
        cron=s.schedule_cron,
        timezone=s.schedule_timezone,
    )


if __name__ == "__main__":
    main()
