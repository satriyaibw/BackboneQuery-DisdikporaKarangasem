import asyncio
import logging

import pytest

from backbone_pull.api import BackboneAPI


def _api():
    return BackboneAPI("https://x/v3", "KEY", "https://auth/access-token", "user", "pass")


class _Resp:
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


class _Session:
    def __init__(self, body):
        self._body = body
        self.last = None

    def post(self, url, data=None, headers=None):
        self.last = {"url": url, "data": data, "headers": headers}
        return _Resp(self._body)


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


def test_is_sp_error():
    assert BackboneAPI.is_sp_error([{"keterangan": "SP gagal"}]) is True
    assert BackboneAPI.is_sp_error([{"npsn": "123"}]) is False
    assert BackboneAPI.is_sp_error([]) is False


def test_get_with_retry_returns_none_after_failures(monkeypatch):
    api = _api()

    async def boom(session, path, params=None):
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
