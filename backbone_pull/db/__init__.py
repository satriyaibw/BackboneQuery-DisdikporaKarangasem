from .base import DatabaseAdapter


def get_adapter(settings) -> DatabaseAdapter:
    dialect = settings.db_dialect
    if dialect == "sqlserver":
        from .sqlserver import SqlServerAdapter
        return SqlServerAdapter(settings)
    if dialect == "postgres":
        from .postgres import PostgresAdapter
        return PostgresAdapter(settings)
    if dialect == "mysql":
        from .mysql import MySQLAdapter
        return MySQLAdapter(settings)
    raise ValueError(f"DB_DIALECT tidak dikenal: {dialect}")


__all__ = ["DatabaseAdapter", "get_adapter"]
