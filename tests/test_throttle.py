import asyncio

from backbone_pull.throttle import RateLimiter


def test_rate_limiter_spaces_requests():
    # rate=50/detik → jarak 20ms; 5 acquire ≈ 4×20ms = 80ms
    async def run():
        rl = RateLimiter(rate_per_sec=50)
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(5):
            await rl.acquire()
        return loop.time() - start
    elapsed = asyncio.run(run())
    assert elapsed >= 0.05  # minimal ~4 interval (toleransi penjadwalan)


def test_rate_limiter_zero_disables():
    async def run():
        rl = RateLimiter(rate_per_sec=0)
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(20):
            await rl.acquire()
        return loop.time() - start
    elapsed = asyncio.run(run())
    assert elapsed < 0.02  # praktis instan (tanpa throttle)
