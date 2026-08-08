"""Jadwal penarikan otomatis dari GET /user-info/schedule Backbone.

Admin Backbone menentukan (`tanggal`) kapan akun boleh membuat
request akses baru. Modul ini mengambil daftar tanggal dan menggabungkannya
dengan jam:menit dari SCHEDULE_CRON (mis. "0 2 * * *" -> jam 02:00) untuk
membentuk cron dinamis, mis. "0 2,6,10 14,28 * *" — sehingga
`main.py --loop` mencoba beberapa kali (retry_count, berjeda
retry_interval_hours) di tiap hari yang benar-benar dijadwalkan Backbone,
tanpa client perlu tahu/hardcode pola tanggalnya secara manual.
"""

from typing import List, Tuple


def extract_time_of_day(cron_expr: str) -> Tuple[int, int]:
    """Ekstrak (jam, menit) dari field menit & jam SCHEDULE_CRON.

    Hanya mendukung nilai tunggal sederhana (mis. "0 2 * * *") — bukan
    daftar/range/step (mis. "0 2,6 * * *" atau "*/5 2 * * *"), karena field
    itu akan digantikan sepenuhnya oleh percobaan-berjeda yang dihitung dari
    retry_count/retry_interval_hours.
    """
    parts = cron_expr.strip().split()
    if len(parts) < 2:
        raise ValueError(f"SCHEDULE_CRON tidak valid: '{cron_expr}'")
    minute_field, hour_field = parts[0], parts[1]
    try:
        minute, hour = int(minute_field), int(hour_field)
    except ValueError:
        raise ValueError(
            f"SCHEDULE_CRON='{cron_expr}' harus berupa jam:menit tunggal "
            "(mis. '0 2 * * *') untuk dipakai mode jadwal otomatis — "
            "daftar/range/step pada field menit atau jam tidak didukung."
        )
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"SCHEDULE_CRON='{cron_expr}' punya jam/menit di luar rentang valid.")
    return hour, minute


def build_cron_from_dates(dates: List[int], hour: int, minute: int,
                          retry_count: int, retry_interval_hours: int) -> str:
    """Bangun ekspresi cron dari daftar tanggal Backbone + jam basis +
    beberapa kali percobaan berjeda (tanpa membungkus lewat tengah malam —
    retry_count dipangkas otomatis bila jamnya akan melebihi 23)."""
    if not dates:
        raise ValueError("Daftar tanggal kosong — tidak bisa membangun jadwal.")

    hours = []
    for i in range(max(1, retry_count)):
        h = hour + i * retry_interval_hours
        if h > 23:
            break
        hours.append(h)
    if not hours:
        hours = [hour]

    dates_str = ",".join(str(d) for d in sorted(set(dates)))
    hours_str = ",".join(str(h) for h in hours)
    return f"{minute} {hours_str} {dates_str} * *"


async def fetch_schedule_dates(api, session) -> List[int]:
    """Ambil daftar tanggal (hari-dalam-bulan) unik dari /user-info/schedule."""
    result = await api.get(session, "/user-info/schedule", {"page": 1, "per_page": 100})
    dates = {row["tanggal"] for row in result.get("data", []) if "tanggal" in row}
    return sorted(dates)
