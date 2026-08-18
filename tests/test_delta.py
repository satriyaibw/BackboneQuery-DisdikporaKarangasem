import asyncio
import logging

from backbone_pull.delta import resolve_npsn_list


class FakeAPI:
    def __init__(self, response):
        self._response = response
        self.calls = []

    async def get_with_retry(self, session, path, params, label, logger):
        self.calls.append((path, params))
        return self._response

    @staticmethod
    def is_sp_error(data):
        return bool(data and data[0].get("keterangan")) and set(data[0].keys()) <= {"total_rows", "keterangan"}


def _run(api, tbl_name="ptk", last_update=None, npsn_list=None):
    logger = logging.getLogger("t")
    return asyncio.run(resolve_npsn_list(
        api, session=None, tbl_name=tbl_name, last_update=last_update,
        npsn_list=npsn_list if npsn_list is not None else ["1", "2", "3"], logger=logger))


def test_no_last_update_returns_full_list_untouched():
    api = FakeAPI(None)
    res = _run(api, last_update=None, npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]
    assert api.calls == []  # first-ever pull: never even calls the endpoint


def test_narrows_to_changed_npsn():
    api = FakeAPI({"data": [{"npsn": "1"}, {"npsn": "3"}]})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "3"]
    assert api.calls == [("/data/npsn-changed", {"tbl_name": "ptk", "last_update": "2026-08-01"})]


def test_empty_changed_list_returns_empty():
    api = FakeAPI({"data": []})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == []


def test_none_response_falls_back_to_full_list():
    api = FakeAPI(None)
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]


def test_sp_error_falls_back_to_full_list():
    api = FakeAPI({"data": [{"total_rows": 0, "keterangan": "Tabel ini di-refresh mingguan..."}]})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]


def test_preserves_original_order_not_response_order():
    api = FakeAPI({"data": [{"npsn": "3"}, {"npsn": "1"}]})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "3"]
