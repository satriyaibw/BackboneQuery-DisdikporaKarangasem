"""Rate limiter async sederhana — membatasi laju MULAI request secara global."""

import asyncio


class RateLimiter:
    """Beri jarak minimal ``1/rate_per_sec`` detik antar acquire.

    Aman dipakai banyak coroutine sekaligus: tiap acquire memesan "slot"
    di bawah lock (cepat), lalu tidur di luar lock sampai slotnya tiba —
    sehingga total laju mulai request tidak melebihi ``rate_per_sec``.
    ``rate_per_sec <= 0`` menonaktifkan throttle.
    """

    def __init__(self, rate_per_sec: float):
        self._min_interval = 1.0 / rate_per_sec if rate_per_sec and rate_per_sec > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def acquire(self):
        if self._min_interval <= 0:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            slot = self._next if self._next > now else now
            self._next = slot + self._min_interval
            wait = slot - now
        if wait > 0:
            await asyncio.sleep(wait)
