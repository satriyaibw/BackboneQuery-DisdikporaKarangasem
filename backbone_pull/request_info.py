"""Pemilihan request akses aktif dari respons GET /user-info/request.

Backbone membatasi pembuatan request baru sesuai jadwal — di luar hari
itu, `POST /user-info/request` menolak. Namun request yang SUDAH dibuat tetap
berlaku sampai `expired_date`, dan bisa dibaca ulang lewat GET tanpa membuat
yang baru. `pick_active_request` memilih request tersebut dari daftar.
"""

from datetime import datetime
from typing import List, Optional


def pick_active_request(items: List[dict], now: datetime) -> Optional[dict]:
    """Kembalikan request dengan `expired_date` > now yang paling lama berlaku.

    Item tanpa `expired_date`, atau dengan nilai yang tidak bisa diparse, atau
    yang sudah kedaluwarsa, diabaikan. Kembalikan None bila tidak ada yang aktif.
    """
    active = []
    for item in items:
        expired_str = item.get("expired_date")
        if not expired_str:
            continue
        try:
            expired_dt = datetime.fromisoformat(expired_str)
        except (TypeError, ValueError):
            continue
        if expired_dt > now:
            active.append((expired_dt, item))
    if not active:
        return None
    active.sort(key=lambda pair: pair[0], reverse=True)
    return active[0][1]
