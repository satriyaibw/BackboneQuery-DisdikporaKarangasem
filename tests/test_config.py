import pytest
from backbone_pull.config import load_settings, MissingConfigError

BASE_ENV = {
    "DB_DIALECT": "postgres", "DB_HOST": "h", "DB_PORT": "5432",
    "DB_USER": "u", "DB_PASSWORD": "p", "DB_NAME": "db",
    "BACKBONE_API_KEY": "k", "BACKBONE_SERVICE_JWT": "j",
}

def _set(monkeypatch, env):
    for k in ("DB_DIALECT","DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME",
              "BACKBONE_API_KEY","BACKBONE_SERVICE_JWT","DB_AUTO_CREATE_DATABASE",
              "BACKBONE_PER_PAGE","PULL_REF"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

def test_loads_and_types(monkeypatch):
    _set(monkeypatch, {**BASE_ENV, "DB_AUTO_CREATE_DATABASE": "true",
                       "BACKBONE_PER_PAGE": "250", "PULL_REF": "false"})
    s = load_settings()
    assert s.db_dialect == "postgres"
    assert s.db_port == 5432
    assert s.backbone_per_page == 250
    assert s.db_auto_create_database is True
    assert s.pull_ref is False
    # default base url terisi walau env kosong
    assert s.backbone_base_url.startswith("https://")

def test_missing_required_raises(monkeypatch):
    env = dict(BASE_ENV); del env["DB_PASSWORD"]
    _set(monkeypatch, env)
    with pytest.raises(MissingConfigError) as e:
        load_settings()
    assert "DB_PASSWORD" in str(e.value)

def test_invalid_dialect_raises(monkeypatch):
    _set(monkeypatch, {**BASE_ENV, "DB_DIALECT": "oracle"})
    with pytest.raises(MissingConfigError) as e:
        load_settings()
    assert "DB_DIALECT" in str(e.value)
