from datetime import datetime
from backbone_pull.db.base import DatabaseAdapter


def test_coerce_int_from_string():
    assert DatabaseAdapter.coerce_value("5", "int") == 5
    assert DatabaseAdapter.coerce_value("1", "bit") == 1


def test_coerce_datetime_from_iso():
    v = DatabaseAdapter.coerce_value("2026-01-02T03:04:05", "datetime2")
    assert isinstance(v, datetime) and v.year == 2026


def test_coerce_decimal_from_string():
    assert DatabaseAdapter.coerce_value("3.5", "decimal") == 3.5


def test_coerce_none_passthrough():
    assert DatabaseAdapter.coerce_value(None, "int") is None


def test_coerce_unknown_type_passthrough():
    assert DatabaseAdapter.coerce_value("abc", "nvarchar") == "abc"
