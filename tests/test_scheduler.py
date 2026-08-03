from datetime import datetime
from zoneinfo import ZoneInfo

from backbone_pull.scheduler import next_run_time

TZ = "Asia/Jakarta"


def test_next_run_later_today():
    now = datetime(2026, 7, 30, 1, 0, 0, tzinfo=ZoneInfo(TZ))
    nxt = next_run_time("0 2 * * *", TZ, now)
    assert nxt == datetime(2026, 7, 30, 2, 0, 0, tzinfo=ZoneInfo(TZ))


def test_next_run_strictly_after_now_when_now_matches_exactly():
    # bila now PERSIS di slot cron, next harus besok (bukan waktu yang sama)
    now = datetime(2026, 7, 30, 2, 0, 0, tzinfo=ZoneInfo(TZ))
    nxt = next_run_time("0 2 * * *", TZ, now)
    assert nxt == datetime(2026, 7, 31, 2, 0, 0, tzinfo=ZoneInfo(TZ))


def test_missed_schedule_skips_to_next_occurrence():
    # now sudah lewat jauh dari jadwal hari ini -> lewati, tunggu besok
    now = datetime(2026, 7, 30, 5, 0, 0, tzinfo=ZoneInfo(TZ))
    nxt = next_run_time("0 2 * * *", TZ, now)
    assert nxt == datetime(2026, 7, 31, 2, 0, 0, tzinfo=ZoneInfo(TZ))


def test_naive_now_is_localized_to_given_timezone():
    now = datetime(2026, 7, 30, 1, 0, 0)  # naive, tanpa tzinfo
    nxt = next_run_time("0 2 * * *", TZ, now)
    assert nxt.tzinfo is not None
    assert nxt.astimezone(ZoneInfo(TZ)) == datetime(2026, 7, 30, 2, 0, 0, tzinfo=ZoneInfo(TZ))


def test_hourly_cron():
    now = datetime(2026, 7, 30, 1, 15, 0, tzinfo=ZoneInfo(TZ))
    nxt = next_run_time("0 * * * *", TZ, now)
    assert nxt == datetime(2026, 7, 30, 2, 0, 0, tzinfo=ZoneInfo(TZ))
