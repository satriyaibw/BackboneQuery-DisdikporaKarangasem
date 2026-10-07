import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(".") / ".env")

DEFAULT_BASE_URL = "https://api.data.kemendikdasmen.go.id/svc/satu-data/pendidikan/v3"
DEFAULT_AUTH_URL = "https://api.data.kemendikdasmen.go.id/svc/satu-data/auth/v1/access-token"
VALID_DIALECTS = ("sqlserver", "postgres", "mysql")


class MissingConfigError(Exception):
    """Konfigurasi wajib kosong atau tidak valid di .env."""


def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


def _int(name: str, default) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        val = default
    try:
        return int(val)
    except (TypeError, ValueError):
        raise MissingConfigError(f"{name} harus berupa angka, bukan '{val}'")


def _float(name: str, default) -> float:
    val = os.getenv(name)
    if val is None or val == "":
        val = default
    try:
        return float(val)
    except (TypeError, ValueError):
        raise MissingConfigError(f"{name} harus berupa angka, bukan '{val}'")


@dataclass
class Settings:
    db_dialect: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    db_auto_create_database: bool
    db_maintenance_db: str
    backbone_base_url: str
    backbone_auth_url: str
    backbone_api_key: str
    backbone_username: str
    backbone_password: str
    backbone_per_page: int
    backbone_rate_limit: float
    backbone_concurrency: int
    schedule_cron: str
    schedule_timezone: str
    schedule_auto_from_api: bool
    schedule_retry_count: int
    schedule_retry_interval_hours: int
    deployment_name: str
    pull_ref: bool
    pull_ref_use_bulk_zip: bool
    pull_log_retention_days: int
    backbone_access_token: Optional[str] = None


def load_settings() -> Settings:
    dialect = (os.getenv("DB_DIALECT") or "sqlserver").strip().lower()
    if dialect not in VALID_DIALECTS:
        raise MissingConfigError(
            f"DB_DIALECT='{dialect}' tidak valid. Pilih salah satu: {VALID_DIALECTS}"
        )

    required = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_NAME": os.getenv("DB_NAME"),
        "BACKBONE_API_KEY": os.getenv("BACKBONE_API_KEY"),
        "BACKBONE_USERNAME": os.getenv("BACKBONE_USERNAME"),
        "BACKBONE_PASSWORD": os.getenv("BACKBONE_PASSWORD"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise MissingConfigError(
            "Konfigurasi wajib berikut kosong di .env: "
            + ", ".join(missing)
            + ". Salin .env.example ke .env lalu isi nilainya."
        )

    default_port = {"sqlserver": 1433, "postgres": 5432, "mysql": 3306}[dialect]
    return Settings(
        db_dialect=dialect,
        db_host=required["DB_HOST"],
        db_port=_int("DB_PORT", default_port),
        db_user=required["DB_USER"],
        db_password=required["DB_PASSWORD"],
        db_name=required["DB_NAME"],
        db_auto_create_database=_bool("DB_AUTO_CREATE_DATABASE", True),
        db_maintenance_db=os.getenv("DB_MAINTENANCE_DB") or "postgres",
        backbone_base_url=os.getenv("BACKBONE_BASE_URL") or DEFAULT_BASE_URL,
        backbone_auth_url=os.getenv("BACKBONE_AUTH_URL") or DEFAULT_AUTH_URL,
        backbone_api_key=required["BACKBONE_API_KEY"],
        backbone_username=required["BACKBONE_USERNAME"],
        backbone_password=required["BACKBONE_PASSWORD"],
        backbone_access_token=os.getenv("BACKBONE_ACCESS_TOKEN") or None,
        backbone_per_page=_int("BACKBONE_PER_PAGE", 500),
        backbone_rate_limit=_float("BACKBONE_RATE_LIMIT", 20),
        backbone_concurrency=_int("BACKBONE_CONCURRENCY", 16),
        schedule_cron=os.getenv("SCHEDULE_CRON") or "0 2 * * *",
        schedule_timezone=os.getenv("SCHEDULE_TIMEZONE") or "Asia/Jakarta",
        schedule_auto_from_api=_bool("SCHEDULE_AUTO_FROM_API", True),
        schedule_retry_count=_int("SCHEDULE_RETRY_COUNT", 3),
        schedule_retry_interval_hours=_int("SCHEDULE_RETRY_INTERVAL_HOURS", 4),
        deployment_name=os.getenv("DEPLOYMENT_NAME") or "backbone-client-pull",
        pull_ref=_bool("PULL_REF", True),
        pull_ref_use_bulk_zip=_bool("PULL_REF_USE_BULK_ZIP", False),
        pull_log_retention_days=_int("PULL_LOG_RETENTION_DAYS", 60),
    )
