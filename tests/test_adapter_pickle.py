"""Adapter harus bisa di-pickle — dibutuhkan Prefect saat mengirim flow ke
subprocess terpisah (DirectSubprocessStarter, dipakai flow.serve() saat
tersambung Prefect Server). Engine SQLAlchemy memegang lock native
(_thread.RLock) yang tidak bisa diserialisasi; harus dibuang saat pickle
dan dibangun ulang saat unpickle."""

import pickle
from types import SimpleNamespace

from sqlalchemy.engine import Engine

from backbone_pull.db.postgres import PostgresAdapter
from backbone_pull.db.sqlserver import SqlServerAdapter


def _settings(**overrides):
    base = dict(db_host="h", db_port=1, db_user="u", db_password="p", db_name="db",
               db_auto_create_database=False, db_maintenance_db="postgres")
    base.update(overrides)
    return SimpleNamespace(**base)


def test_sqlserver_adapter_picklable_and_rebuilds_engines():
    a = SqlServerAdapter(_settings())
    original_engine_id = id(a._engine)
    original_master_id = id(a._master)

    restored = pickle.loads(pickle.dumps(a))

    assert isinstance(restored, SqlServerAdapter)
    assert isinstance(restored._engine, Engine)
    assert isinstance(restored._master, Engine)
    # Engine baru dibangun ulang, bukan objek yang sama (tidak mungkin lolos
    # pickle kalau memang objek yang sama - ini pastikan benar-benar dibangun ulang)
    assert id(restored._engine) != original_engine_id
    assert id(restored._master) != original_master_id
    assert restored.settings.db_host == "h"
    assert restored.schema_ctrl == "sync"


def test_postgres_adapter_picklable_and_rebuilds_engines():
    a = PostgresAdapter(_settings())
    restored = pickle.loads(pickle.dumps(a))

    assert isinstance(restored, PostgresAdapter)
    assert isinstance(restored._engine, Engine)
    assert isinstance(restored._maint, Engine)
    assert restored.settings.db_host == "h"


def test_getstate_excludes_engine_instances():
    a = SqlServerAdapter(_settings())
    state = a.__getstate__()
    assert not any(isinstance(v, Engine) for v in state.values())
    assert "settings" in state and "schema_ctrl" in state
