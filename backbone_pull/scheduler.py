"""Penghitung jadwal cron berikutnya — dipakai mode `--loop` di main.py.

Dipisah dari main.py agar bisa diuji tanpa perlu benar-benar menunggu
(sleep) atau menjalankan flow. `main.py` yang menjalankan loop
(hitung next_run_time -> sleep -> jalankan flow -> ulangi).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter


def next_run_time(cron_expr: str, tz_name: str, now: datetime) -> datetime:
    """Kembalikan waktu (timezone-aware) kapan `cron_expr` berikutnya jatuh,
    strictly setelah `now`. Jadwal yang sudah lewat otomatis dilewati —
    hasilnya selalu kejadian berikutnya di masa depan, bukan yang terlewat."""
    tz = ZoneInfo(tz_name)
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)
    return croniter(cron_expr, now).get_next(datetime)
