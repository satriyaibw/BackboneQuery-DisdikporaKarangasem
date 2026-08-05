import asyncio

import pytest

from backbone_pull.schedule_source import (
    build_cron_from_dates,
    extract_time_of_day,
    fetch_schedule_dates,
)


# ── extract_time_of_day ──────────────────────────────────────────────────
def test_extract_time_of_day_simple():
    assert extract_time_of_day("0 2 * * *") == (2, 0)


def test_extract_time_of_day_other_values():
    assert extract_time_of_day("30 14 * * *") == (14, 30)


def test_extract_time_of_day_rejects_list():
    with pytest.raises(ValueError):
        extract_time_of_day("0 2,6 * * *")


def test_extract_time_of_day_rejects_step():
    with pytest.raises(ValueError):
        extract_time_of_day("*/5 2 * * *")


# ── build_cron_from_dates ────────────────────────────────────────────────
def test_build_cron_single_attempt():
    cron = build_cron_from_dates([14, 28], hour=2, minute=0,
                                 retry_count=1, retry_interval_hours=4)
    assert cron == "0 2 14,28 * *"


def test_build_cron_multi_attempt():
    cron = build_cron_from_dates([14, 28], hour=2, minute=0,
                                 retry_count=3, retry_interval_hours=4)
    assert cron == "0 2,6,10 14,28 * *"


def test_build_cron_dates_sorted_and_deduped():
    cron = build_cron_from_dates([28, 14, 14], hour=2, minute=0,
                                 retry_count=1, retry_interval_hours=4)
    assert cron == "0 2 14,28 * *"


def test_build_cron_clamps_retry_count_to_avoid_wraparound():
    # hour=22, interval=4, count=4 -> 22,26,30,34 -> melebihi 23, harus di-clamp
    cron = build_cron_from_dates([14], hour=22, minute=0,
                                 retry_count=4, retry_interval_hours=4)
    hours = cron.split(" ")[1].split(",")
    assert all(0 <= int(h) <= 23 for h in hours)
    assert len(hours) < 4  # jumlah percobaan dipangkas, bukan wrap salah ke jam kecil


def test_build_cron_empty_dates_raises():
    with pytest.raises(ValueError):
        build_cron_from_dates([], hour=2, minute=0, retry_count=1, retry_interval_hours=4)


# ── fetch_schedule_dates ──────────────────────────────────────────────────
class _FakeResp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, body):
        self._body = body

    def get(self, url, params=None, headers=None):
        return _FakeResp(self._body)


class _FakeAPI:
    base_url = "https://x/v3"
    headers = {}

    async def get(self, session, path, params=None):
        async with session.get(f"{self.base_url}{path}", params=params) as r:
            return await r.json()


def test_fetch_schedule_dates_extracts_and_dedups():
    body = {"data": [{"tanggal": 28}, {"tanggal": 14}, {"tanggal": 14}]}
    session = _FakeSession(body)
    dates = asyncio.run(fetch_schedule_dates(_FakeAPI(), session))
    assert dates == [14, 28]


def test_fetch_schedule_dates_empty():
    session = _FakeSession({"data": []})
    dates = asyncio.run(fetch_schedule_dates(_FakeAPI(), session))
    assert dates == []
