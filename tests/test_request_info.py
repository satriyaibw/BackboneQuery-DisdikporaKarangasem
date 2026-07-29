from datetime import datetime

from backbone_pull.request_info import pick_active_request


def test_picks_request_with_future_expiry():
    now = datetime(2026, 7, 29, 12, 0, 0)
    items = [{"request_id": "a", "expired_date": "2026-07-30T00:00:00"}]
    assert pick_active_request(items, now)["request_id"] == "a"


def test_ignores_expired_requests():
    now = datetime(2026, 7, 29, 12, 0, 0)
    items = [{"request_id": "old", "expired_date": "2026-07-28T00:00:00"}]
    assert pick_active_request(items, now) is None


def test_picks_latest_when_multiple_active():
    now = datetime(2026, 7, 29, 0, 0, 0)
    items = [
        {"request_id": "a", "expired_date": "2026-07-30T00:00:00"},
        {"request_id": "b", "expired_date": "2026-08-01T00:00:00"},
    ]
    assert pick_active_request(items, now)["request_id"] == "b"


def test_ignores_items_without_expired_date():
    now = datetime(2026, 7, 29, 0, 0, 0)
    assert pick_active_request([{"request_id": "x"}], now) is None


def test_ignores_items_with_unparseable_expired_date():
    now = datetime(2026, 7, 29, 0, 0, 0)
    items = [{"request_id": "bad", "expired_date": "bukan-tanggal"}]
    assert pick_active_request(items, now) is None


def test_empty_list_returns_none():
    assert pick_active_request([], datetime(2026, 7, 29)) is None
