import asyncio
import logging

import pytest

from backbone_pull.api import BackboneAPI


def _api():
    return BackboneAPI("https://x/v3", "KEY", "https://auth/access-token", "user", "pass")


class _Resp:
    def __init__(self, body, status=200):
        self._body = body
        self.status = status

    def raise_for_status(self):
        pass

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, body):
        self._body = body
        self.last = None

    def post(self, url, data=None, headers=None):
        self.last = {"method": "POST", "url": url, "data": data, "headers": headers}
        return _Resp(self._body)

    def get(self, url, params=None, headers=None):
        self.last = {"method": "GET", "url": url, "params": params, "headers": headers}
        return _Resp(self._body)


class _FakeLogger:
    def __init__(self):
        self.infos = []

    def info(self, msg):
        self.infos.append(msg)

    def warning(self, msg):
        pass


def test_base_headers_before_token():
    api = _api()
    assert api.headers["X-API-Key"] == "KEY"
    assert api.headers["Accept"] == "application/json"
    assert "Authorization" not in api.headers  # belum fetch token


def test_fetch_token_sets_bearer():
    api = _api()
    session = _Session({"access_token": "TOK", "token_type": "bearer"})
    token = asyncio.run(api.fetch_token(session))
    assert token == "TOK"
    assert api.headers["Authorization"] == "Bearer TOK"
    assert api.headers["X-API-Key"] == "KEY"  # API key tetap dikirim
    assert session.last["url"] == "https://auth/access-token"
    assert session.last["data"] == {"username": "user", "password": "pass"}


def test_fetch_token_missing_access_token_raises():
    api = _api()
    session = _Session({"token_type": "bearer"})  # tanpa access_token
    with pytest.raises(RuntimeError):
        asyncio.run(api.fetch_token(session))


def test_is_sp_error_true_sentinel_row():
    # Baris sentinel error asli dari SP: cuma total_rows + keterangan, pesan terisi.
    assert BackboneAPI.is_sp_error([{"total_rows": 0, "keterangan": "SP gagal"}]) is True
    assert BackboneAPI.is_sp_error([{"keterangan": "SP gagal"}]) is True


def test_is_sp_error_false_no_keterangan():
    assert BackboneAPI.is_sp_error([{"npsn": "123"}]) is False
    assert BackboneAPI.is_sp_error([]) is False


def test_is_sp_error_false_for_table_with_real_keterangan_column():
    # registrasi_peserta_didik (dan tabel lain) punya kolom bisnis BERNAMA
    # "keterangan" sendiri -- baris data asli (banyak kolom lain) tidak boleh
    # dianggap sp_error walau field "keterangan"-nya ada, baik NULL...
    row_null = {
        "total_rows": 17, "registrasi_id": "21cdde0d-3295-470e-a658-d5c7c9425ff9",
        "peserta_didik_id": "5b89afc2-b3f7-4be1-a83b-17b49277486c",
        "sekolah_id": "a6642a22-00d9-49b2-b404-003630c35406",
        "tanggal_masuk_sekolah": "2018-11-24", "keterangan": None,
        "create_date": "2018-11-25T08:49:51.210000",
    }
    assert BackboneAPI.is_sp_error([row_null]) is False
    # ...maupun terisi teks asli (bukan sinyal error, cuma kebetulan field yang sama).
    row_filled = {**row_null, "keterangan": "pindah sekolah"}
    assert BackboneAPI.is_sp_error([row_filled]) is False


def test_get_logs_success_line_when_logger_given():
    api = _api()
    session = _Session({"data": [{"a": 1}, {"a": 2}], "page": 1, "total_pages": 1})
    logger = _FakeLogger()
    result = asyncio.run(api.get(session, "/data/by-npsn", {"npsn": "1"},
                                 logger=logger, label="guru/1"))
    assert result["data"] == [{"a": 1}, {"a": 2}]
    assert len(logger.infos) == 1
    msg = logger.infos[0]
    assert "guru/1" in msg and "200" in msg and "2 baris" in msg


def test_get_no_logging_when_logger_omitted():
    api = _api()
    session = _Session({"data": []})
    result = asyncio.run(api.get(session, "/metadata"))  # tanpa logger — tidak boleh error
    assert result == {"data": []}


def test_get_logs_non_list_data_without_count():
    api = _api()
    session = _Session({"access_token": "x"})  # data bukan list
    logger = _FakeLogger()
    asyncio.run(api.get(session, "/whatever", logger=logger, label="x"))
    assert len(logger.infos) == 1 and "200" in logger.infos[0]


def test_get_with_retry_returns_none_after_failures(monkeypatch):
    api = _api()

    async def boom(session, path, params=None, logger=None, label=None):
        raise RuntimeError("network down")

    monkeypatch.setattr(api, "get", boom)

    async def _no_sleep(*_a, **_k):
        return None
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    logger = logging.getLogger("test")
    result = asyncio.run(
        api.get_with_retry(session=None, path="/data", params={},
                           label="x", logger=logger, max_retries=3)
    )
    assert result is None
