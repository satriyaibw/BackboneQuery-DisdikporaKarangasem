import asyncio
import logging

from backbone_pull.puller import pull_paginated, PullResult


class FakeAPI:
    def __init__(self, responses):
        self._responses = list(responses)  # tiap elemen = return get_with_retry (dict / None)
        self.calls = []

    async def get_with_retry(self, session, path, params, label, logger):
        self.calls.append(params)
        return self._responses.pop(0) if self._responses else None

    @staticmethod
    def is_sp_error(data):
        return bool(data and "keterangan" in data[0])


class FakeDB:
    def __init__(self, raise_on_upsert=None):
        self.upserted = []
        self._raise_on_upsert = raise_on_upsert

    def upsert_rows(self, tbl_name, data, meta):
        if self._raise_on_upsert is not None:
            raise self._raise_on_upsert
        self.upserted.append((tbl_name, list(data)))
        return len(data)


def _run(api, db, **kw):
    logger = logging.getLogger("t")
    return asyncio.run(pull_paginated(
        api, db, session=None, path="/data/by-npsn",
        base_params={"npsn": "1", "tbl_name": "guru"},
        tbl_name="guru", meta={}, logger=logger, **kw))


def test_complete_single_page():
    api = FakeAPI([{"total_rows": 2, "total_pages": 1, "data": [{"a": 1}, {"a": 2}]}])
    db = FakeDB()
    res = _run(api, db)
    assert isinstance(res, PullResult)
    assert res.ok is True and res.received == 2 and res.expected == 2 and res.reason == ""
    assert db.upserted == [("guru", [{"a": 1}, {"a": 2}])]


def test_incomplete_detected_via_total_rows():
    api = FakeAPI([{"total_rows": 5, "total_pages": 1, "data": [{"a": 1}, {"a": 2}]}])
    res = _run(api, FakeDB())
    assert res.ok is False and res.reason == "incomplete"
    assert res.received == 2 and res.expected == 5


def test_error_when_get_returns_none():
    res = _run(FakeAPI([None]), FakeDB())
    assert res.ok is False and res.reason == "error"


def test_sp_error():
    api = FakeAPI([{"data": [{"keterangan": "SP kosong"}]}])
    res = _run(api, FakeDB())
    assert res.ok is False and res.reason == "sp_error" and "SP kosong" in res.detail


def test_multipage_accumulates():
    api = FakeAPI([
        {"total_rows": 3, "total_pages": 2, "data": [{"a": 1}, {"a": 2}]},
        {"total_rows": 3, "total_pages": 2, "data": [{"a": 3}]},
    ])
    res = _run(api, FakeDB())
    assert res.ok is True and res.received == 3 and res.expected == 3
    assert len(api.calls) == 2 and api.calls[1]["page"] == 2


def test_collect_returns_rows():
    api = FakeAPI([{"total_rows": 2, "total_pages": 1, "data": [{"npsn": "x"}, {"npsn": "y"}]}])
    res = _run(api, FakeDB(), collect=True)
    assert [r["npsn"] for r in res.rows] == ["x", "y"]


def test_last_update_passed_and_empty_is_complete():
    api = FakeAPI([{"total_rows": 0, "total_pages": 1, "data": []}])
    db = FakeDB()
    res = _run(api, db, last_update="2026-01-01")
    assert api.calls[0]["last_update"] == "2026-01-01"
    assert res.ok is True and res.received == 0 and db.upserted == []


def test_unknown_total_rows_ok_when_no_error():
    api = FakeAPI([{"total_pages": 1, "data": [{"a": 1}]}])  # tanpa total_rows
    res = _run(api, FakeDB())
    assert res.ok is True and res.expected is None and res.received == 1


def test_db_write_error_becomes_pull_result_not_exception():
    # Baris NOT NULL violation dkk -- HARUS jadi PullResult(ok=False), bukan
    # exception yang merambat lewat asyncio.gather() dan menjatuhkan seluruh
    # batch tabel itu (lihat backbone-pull-ref-bulk crash: IntegrityError dari
    # db.upsert_rows tidak tertangani sebelumnya).
    api = FakeAPI([{"total_rows": 2, "total_pages": 1, "data": [{"a": 1}, {"a": 2}]}])
    db = FakeDB(raise_on_upsert=RuntimeError("Cannot insert the value NULL into column 'x'"))
    res = _run(api, db)
    assert res.ok is False and res.reason == "error"
    assert "Cannot insert the value NULL" in res.detail
    assert res.received == 0  # baris yg gagal ditulis tidak dihitung diterima


def test_db_write_error_via_write_lock_path_also_safe():
    api = FakeAPI([{"total_rows": 2, "total_pages": 1, "data": [{"a": 1}, {"a": 2}]}])
    db = FakeDB(raise_on_upsert=RuntimeError("deadlock"))

    async def run():
        lock = asyncio.Lock()
        return await pull_paginated(
            api, db, session=None, path="/data/by-npsn",
            base_params={"npsn": "1", "tbl_name": "guru"},
            tbl_name="guru", meta={}, logger=logging.getLogger("t"),
            write_lock=lock)

    res = asyncio.run(run())
    assert res.ok is False and res.reason == "error" and "deadlock" in res.detail


def test_write_lock_path_still_upserts():
    # Jalur konkuren: upsert lewat write_lock + asyncio.to_thread
    api = FakeAPI([{"total_rows": 2, "total_pages": 1, "data": [{"a": 1}, {"a": 2}]}])
    db = FakeDB()

    async def run():
        lock = asyncio.Lock()
        return await pull_paginated(
            api, db, session=None, path="/data/by-npsn",
            base_params={"npsn": "1", "tbl_name": "guru"},
            tbl_name="guru", meta={}, logger=logging.getLogger("t"),
            write_lock=lock)

    res = asyncio.run(run())
    assert res.ok is True and res.received == 2
    assert db.upserted == [("guru", [{"a": 1}, {"a": 2}])]
