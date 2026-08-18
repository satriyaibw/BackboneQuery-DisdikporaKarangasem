import asyncio
import logging

from backbone_pull.api import BackboneAPI
from backbone_pull.delta import resolve_npsn_list


class FakeAPI:
    def __init__(self, response):
        self._response = response
        self.calls = []

    async def get_with_retry(self, session, path, params, label, logger):
        self.calls.append((path, params))
        return self._response

    # Delegasi langsung ke implementasi asli, bukan salinan tangan -- kalau
    # BackboneAPI.is_sp_error berubah, test di file ini ikut menangkapnya.
    is_sp_error = staticmethod(BackboneAPI.is_sp_error)


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


def test_ptk_rejection_payload_falls_back_to_full_list():
    api = FakeAPI({"data": [{"total_rows": 0, "keterangan": "Delta discovery untuk peserta_didik/ptk belum didukung -- perlu verifikasi struktur v_peserta_didik/v_ptk terlebih dahulu"}]})
    res = _run(api, tbl_name="ptk", last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]


def test_single_column_keterangan_rejection_falls_back_to_full_list():
    api = FakeAPI({"data": [{"keterangan": "Tabel tidak ditemukan dalam metadata"}]})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]


def test_non_list_data_falls_back_to_full_list():
    api = FakeAPI({"data": None})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]


def test_nonempty_response_with_no_local_match_falls_back_to_full_list():
    # Server reports changes, but none of the returned NPSN are in our local roster --
    # a scope/format mismatch, not "nothing changed" -- must not silently narrow to [].
    api = FakeAPI({"data": [{"npsn": "999"}, {"npsn": "888"}]})
    res = _run(api, last_update="2026-08-01", npsn_list=["1", "2", "3"])
    assert res == ["1", "2", "3"]
