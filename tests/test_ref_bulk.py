import asyncio
import csv
import io
import json
from datetime import datetime
from zipfile import ZIP_DEFLATED, ZipFile

import aiohttp
import pytest

from backbone_pull.ref_bulk import download_referensi_zip, get_manifest_generated_at, parse_referensi_zip


def _build_zip(tables: dict, manifest=None) -> bytes:
    buf = io.BytesIO()
    with ZipFile(buf, mode="w", compression=ZIP_DEFLATED) as zf:
        for tbl_name, rows in tables.items():
            fieldnames = list(rows[0].keys()) if rows else []
            out = io.StringIO()
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            zf.writestr(f"{tbl_name}.csv", out.getvalue())
        if manifest is not None:
            zf.writestr("manifest.json", json.dumps(manifest))
    return buf.getvalue()


# ── parse_referensi_zip ──────────────────────────────────────────────────
def test_parse_referensi_zip_basic():
    zip_bytes = _build_zip(
        {"jenis_kelamin": [{"kode": "1", "nama": "Laki-Laki"}, {"kode": "2", "nama": "Perempuan"}]},
        manifest={"generated_at": "2026-08-01T00:00:00", "tables": [{"tbl_name": "jenis_kelamin", "row_count": 2}]},
    )
    tables, manifest = parse_referensi_zip(zip_bytes)
    assert tables["jenis_kelamin"] == [{"kode": "1", "nama": "Laki-Laki"}, {"kode": "2", "nama": "Perempuan"}]
    assert manifest["tables"][0]["tbl_name"] == "jenis_kelamin"


def test_parse_referensi_zip_empty_string_becomes_none():
    zip_bytes = _build_zip({"agama": [{"kode": "1", "nama": "Islam", "keterangan": ""}]})
    tables, _ = parse_referensi_zip(zip_bytes)
    assert tables["agama"][0]["keterangan"] is None


def test_parse_referensi_zip_no_manifest_defaults_empty():
    zip_bytes = _build_zip({"agama": [{"kode": "1", "nama": "Islam"}]}, manifest=None)
    tables, manifest = parse_referensi_zip(zip_bytes)
    assert manifest == {}
    assert "agama" in tables


def test_parse_referensi_zip_multiple_tables():
    zip_bytes = _build_zip({
        "agama": [{"kode": "1", "nama": "Islam"}],
        "jenis_kelamin": [{"kode": "1", "nama": "Laki-Laki"}],
    })
    tables, _ = parse_referensi_zip(zip_bytes)
    assert set(tables.keys()) == {"agama", "jenis_kelamin"}


def test_parse_referensi_zip_ignores_non_csv_entries():
    zip_bytes = _build_zip({"agama": [{"kode": "1", "nama": "Islam"}]},
                           manifest={"generated_at": "x", "tables": []})
    tables, _ = parse_referensi_zip(zip_bytes)
    assert list(tables.keys()) == ["agama"]


# ── download_referensi_zip ───────────────────────────────────────────────
class _FakeResp:
    def __init__(self, status, body=b""):
        self.status = status
        self._body = body

    async def read(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, resp=None, exc=None):
        self._resp = resp
        self._exc = exc

    def get(self, url, headers=None):
        if self._exc:
            raise self._exc
        return self._resp


class _FakeAPI:
    base_url = "https://x/v3"
    headers = {"Authorization": "Bearer t"}


def test_download_referensi_zip_ok():
    session = _FakeSession(resp=_FakeResp(200, b"PKZIPDATA"))
    result = asyncio.run(download_referensi_zip(session, _FakeAPI()))
    assert result == b"PKZIPDATA"


def test_download_referensi_zip_404_returns_none():
    session = _FakeSession(resp=_FakeResp(404))
    result = asyncio.run(download_referensi_zip(session, _FakeAPI()))
    assert result is None


def test_download_referensi_zip_503_returns_none():
    session = _FakeSession(resp=_FakeResp(503))
    result = asyncio.run(download_referensi_zip(session, _FakeAPI()))
    assert result is None


def test_download_referensi_zip_other_http_error_returns_none():
    session = _FakeSession(resp=_FakeResp(500))
    result = asyncio.run(download_referensi_zip(session, _FakeAPI()))
    assert result is None


def test_download_referensi_zip_network_error_returns_none():
    session = _FakeSession(exc=aiohttp.ClientConnectionError("boom"))
    result = asyncio.run(download_referensi_zip(session, _FakeAPI()))
    assert result is None


# ── get_manifest_generated_at ────────────────────────────────────────────
def test_get_manifest_generated_at_parses_iso8601():
    result = get_manifest_generated_at({"generated_at": "2026-08-08T18:49:12.858879+00:00"})
    assert result == datetime.fromisoformat("2026-08-08T18:49:12.858879+00:00")


def test_get_manifest_generated_at_missing_field_returns_none():
    assert get_manifest_generated_at({}) is None


def test_get_manifest_generated_at_invalid_value_returns_none():
    assert get_manifest_generated_at({"generated_at": "not-a-date"}) is None
