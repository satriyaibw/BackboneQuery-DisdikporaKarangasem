import pytest
from backbone_pull.config import load_settings, MissingConfigError

BASE_ENV = {
    "DB_DIALECT": "postgres", "DB_HOST": "h", "DB_PORT": "5432",
    "DB_USER": "u", "DB_PASSWORD": "p", "DB_NAME": "db",
    "BACKBONE_API_KEY": "k", "BACKBONE_USERNAME": "user", "BACKBONE_PASSWORD": "pass",
}

def _set(monkeypatch, env):
    for k in ("DB_DIALECT","DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME",
              "BACKBONE_API_KEY","BACKBONE_USERNAME","BACKBONE_PASSWORD","BACKBONE_AUTH_URL",
              "DB_AUTO_CREATE_DATABASE","BACKBONE_PER_PAGE","BACKBONE_RATE_LIMIT",
              "BACKBONE_CONCURRENCY","PULL_REF"):
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
    assert s.backbone_username == "user"
    assert s.backbone_password == "pass"
    assert s.backbone_rate_limit == 20.0  # default
    assert s.backbone_concurrency == 16    # default
    # default base & auth url terisi walau env kosong
    assert s.backbone_base_url.startswith("https://")
    assert s.backbone_auth_url.startswith("https://") and "access-token" in s.backbone_auth_url

def test_missing_required_raises(monkeypatch):
    env = dict(BASE_ENV); del env["DB_PASSWORD"]
    _set(monkeypatch, env)
    with pytest.raises(MissingConfigError) as e:
        load_settings()
    assert "DB_PASSWORD" in str(e.value)

def test_missing_backbone_password_raises(monkeypatch):
    env = dict(BASE_ENV); del env["BACKBONE_PASSWORD"]
    _set(monkeypatch, env)
    with pytest.raises(MissingConfigError) as e:
        load_settings()
    assert "BACKBONE_PASSWORD" in str(e.value)

def test_invalid_dialect_raises(monkeypatch):
    _set(monkeypatch, {**BASE_ENV, "DB_DIALECT": "oracle"})
    with pytest.raises(MissingConfigError) as e:
        load_settings()
    assert "DB_DIALECT" in str(e.value)

def test_non_numeric_db_port_raises_friendly_error(monkeypatch):
    _set(monkeypatch, {**BASE_ENV, "DB_PORT": "abc"})
    with pytest.raises(MissingConfigError) as e:
        load_settings()
    assert "DB_PORT" in str(e.value)
