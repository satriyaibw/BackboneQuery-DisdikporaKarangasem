from dotenv import load_dotenv

# Muat .env lebih dulu — sebelum modul Prefect diimpor lewat backbone_pull.flow —
# agar variabel Prefect (mis. PREFECT_API_URL / PREFECT_API_KEY) dari .env sudah
# tersedia saat Prefect membaca konfigurasinya.
load_dotenv()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from prefect.schedules import Cron  # noqa: E402

from backbone_pull.config import load_settings  # noqa: E402
from backbone_pull.flow import backbone_client_pull  # noqa: E402
from backbone_pull.scheduler import next_run_time  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Penarikan data Backbone (Prefect serve).")
    p.add_argument("--run-once", action="store_true",
                   help="Jalankan flow sekali lalu keluar (tanpa scheduler).")
    p.add_argument("--loop", action="store_true",
                   help="Jalankan flow berulang sesuai SCHEDULE_CRON/SCHEDULE_TIMEZONE "
                        "di .env, tanpa perlu Prefect Server (cocok utk NSSM/service biasa).")
    return p


def run_loop(s):
    """Loop internal: hitung jadwal berikutnya (SCHEDULE_CRON/SCHEDULE_TIMEZONE),
    tunggu sampai jadwal itu tiba, jalankan flow, ulangi. Jadwal yang terlewat
    (mis. komputer mati) otomatis dilewati — tunggu kejadian berikutnya, bukan
    langsung dieksekusi saat proses baru start."""
    tz = ZoneInfo(s.schedule_timezone)
    # flush=True wajib: saat stdout diarahkan ke file (mis. NSSM AppStdout, §5.1),
    # Python memakai block-buffering, bukan line-buffering — tanpa flush, pesan
    # bisa tertahan di buffer dan tidak muncul di file log secara realtime.
    print(f"Mode loop internal aktif — jadwal '{s.schedule_cron}' ({s.schedule_timezone}). "
          "Tidak perlu Prefect Server.", flush=True)
    while True:
        now = datetime.now(tz)
        nxt = next_run_time(s.schedule_cron, s.schedule_timezone, now)
        wait_seconds = (nxt - now).total_seconds()
        print(f"[*] Menunggu... eksekusi berikutnya pada: {nxt:%Y-%m-%d %H:%M:%S %Z} "
              f"({wait_seconds:.0f} detik lagi)", flush=True)
        time.sleep(max(wait_seconds, 0))

        print(f"\n[{datetime.now(tz):%Y-%m-%d %H:%M:%S}] Eksekusi dimulai...", flush=True)
        try:
            asyncio.run(backbone_client_pull())
            print("Eksekusi selesai.\n", flush=True)
        except Exception as e:  # noqa: BLE001
            # Jangan biarkan satu kegagalan menghentikan proses — lanjut ke jadwal berikutnya.
            print(f"Eksekusi GAGAL: {e}\n", flush=True)


def main():
    args = build_parser().parse_args()
    s = load_settings()

    if args.run_once:
        asyncio.run(backbone_client_pull())
        return

    if args.loop:
        run_loop(s)
        return

    backbone_client_pull.serve(
        name=s.deployment_name,
        schedule=Cron(s.schedule_cron, timezone=s.schedule_timezone),
    )


if __name__ == "__main__":
    main()
