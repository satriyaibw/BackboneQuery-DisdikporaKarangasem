import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(".") / ".env")

DEFAULT_BASE_URL = "https://api.data.kemendikdasmen.go.id/svc/satu-data/pendidikan/v3"
VALID_DIALECTS = ("sqlserver", "postgres")


class MissingConfigError(Exception):
    """Konfigurasi wajib kosong atau tidak valid di .env."""


def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


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
    backbone_api_key: str
    backbone_service_jwt: str
    backbone_per_page: int
    schedule_cron: str
    schedule_timezone: str
    deployment_name: str
    pull_ref: bool


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
        "BACKBONE_SERVICE_JWT": os.getenv("BACKBONE_SERVICE_JWT"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise MissingConfigError(
            "Konfigurasi wajib berikut kosong di .env: "
            + ", ".join(missing)
            + ". Salin .env.example ke .env lalu isi nilainya."
        )

    default_port = 1433 if dialect == "sqlserver" else 5432
    return Settings(
        db_dialect=dialect,
        db_host=required["DB_HOST"],
        db_port=int(os.getenv("DB_PORT") or default_port),
        db_user=required["DB_USER"],
        db_password=required["DB_PASSWORD"],
        db_name=required["DB_NAME"],
        db_auto_create_database=_bool("DB_AUTO_CREATE_DATABASE", True),
        db_maintenance_db=os.getenv("DB_MAINTENANCE_DB") or "postgres",
        backbone_base_url=os.getenv("BACKBONE_BASE_URL") or DEFAULT_BASE_URL,
        backbone_api_key=required["BACKBONE_API_KEY"],
        backbone_service_jwt=required["BACKBONE_SERVICE_JWT"],
        backbone_per_page=int(os.getenv("BACKBONE_PER_PAGE") or 500),
        schedule_cron=os.getenv("SCHEDULE_CRON") or "0 2 * * *",
        schedule_timezone=os.getenv("SCHEDULE_TIMEZONE") or "Asia/Jakarta",
        deployment_name=os.getenv("DEPLOYMENT_NAME") or "backbone-client-pull",
        pull_ref=_bool("PULL_REF", False),
    )
