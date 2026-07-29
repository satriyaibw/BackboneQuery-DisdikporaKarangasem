from dotenv import load_dotenv

# Muat .env lebih dulu — sebelum modul Prefect diimpor lewat backbone_pull.flow —
# agar variabel Prefect (mis. PREFECT_API_URL / PREFECT_API_KEY) dari .env sudah
# tersedia saat Prefect membaca konfigurasinya.
load_dotenv()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import time
from datetime import datetime, timedelta

from prefect.schedules import Cron  # noqa: E402

from backbone_pull.config import load_settings  # noqa: E402
from backbone_pull.flow import backbone_client_pull  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Penarikan data Backbone (Prefect serve).")
    p.add_argument("--run-once", action="store_true",
                   help="Jalankan flow sekali lalu keluar (tanpa scheduler).")
    p.add_argument("--loop", action="store_true",
                   help="Jalankan flow dalam loop terus-menerus.")
    p.add_argument("--interval", type=int, default=3600,
                   help="Jeda waktu loop dalam detik (default: 3600 detik / 1 jam).")
    return p

def get_seconds_until_next_run(target_hour: int = 2) -> float:
    """Menghitung sisa detik sampai jam target berikutnya."""
    now = datetime.now()
    target_time = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)

    # Jika jam 02:00 hari ini sudah lewat, set target ke jam 02:00 besok
    if now >= target_time:
        target_time += timedelta(days=1)

    return (target_time - now).total_seconds()

def main():
    args = build_parser().parse_args()

    # 1. Mode Run Once
    if args.run_once:
        asyncio.run(backbone_client_pull())
        return

    # 2. Mode Loop (Khusus Jam 02:00)
    if args.loop:
        print("Mode Loop aktif: Program akan berjalan otomatis setiap jam 02:00.")
        while True:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Eksekusi dimulaii...")
            asyncio.run(backbone_client_pull())
            print("Eksekusi selesai.\n")

            # Hitung jeda waktu menuju jam 02:00 berikutnya
            sleep_seconds = get_seconds_until_next_run(target_hour=2)
            next_run = datetime.now() + timedelta(seconds=sleep_seconds)
            
            print(f"[*] Menunggu... Eksekusi berikutnya pada: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(sleep_seconds)

    # 3. Mode Default (Prefect Serve)
    s = load_settings()
    backbone_client_pull.serve(
        name=s.deployment_name,
        schedule=Cron(s.schedule_cron, timezone=s.schedule_timezone),
    )


if __name__ == "__main__":
    main()
