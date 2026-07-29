import pytest
from types import SimpleNamespace
from backbone_pull.db import get_adapter
from backbone_pull.db.sqlserver import SqlServerAdapter
from backbone_pull.db.postgres import PostgresAdapter

def _settings(dialect):
    return SimpleNamespace(db_dialect=dialect, db_host="h", db_port=1, db_user="u",
                           db_password="p", db_name="db", db_auto_create_database=False,
                           db_maintenance_db="postgres")

def test_returns_sqlserver():
    assert isinstance(get_adapter(_settings("sqlserver")), SqlServerAdapter)

def test_returns_postgres():
    assert isinstance(get_adapter(_settings("postgres")), PostgresAdapter)

def test_unknown_raises():
    with pytest.raises(ValueError):
        get_adapter(_settings("oracle"))
