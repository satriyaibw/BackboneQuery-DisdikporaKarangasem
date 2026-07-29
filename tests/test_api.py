import asyncio
import logging
from backbone_pull.api import BackboneAPI

def test_headers_built():
    api = BackboneAPI("https://x/v3", "KEY", "JWT")
    assert api.headers["Authorization"] == "Bearer JWT"
    assert api.headers["X-API-Key"] == "KEY"
    assert api.headers["Accept"] == "application/json"

def test_is_sp_error():
    assert BackboneAPI.is_sp_error([{"keterangan": "SP gagal"}]) is True
    assert BackboneAPI.is_sp_error([{"npsn": "123"}]) is False
    assert BackboneAPI.is_sp_error([]) is False

def test_get_with_retry_returns_none_after_failures(monkeypatch):
    api = BackboneAPI("https://x/v3", "KEY", "JWT")

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
