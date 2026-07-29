from types import SimpleNamespace
from backbone_pull.db.sqlserver import SqlServerAdapter

def _adapter():
    s = SimpleNamespace(db_host="h", db_port=1433, db_user="u",
                        db_password="p", db_name="db",
                        db_auto_create_database=True, db_maintenance_db="master")
    return SqlServerAdapter(s)

def test_dsn_url_encodes_special_char_password():
    s = SimpleNamespace(db_host="h", db_port=1433, db_user="u",
                        db_password="p@ss:w/rd%x", db_name="db",
                        db_auto_create_database=True, db_maintenance_db="master")
    a = SqlServerAdapter(s)
    assert a._engine.url.password == "p@ss:w/rd%x"
    assert a._engine.url.database == "db"

def test_col_type_nvarchar_bounded():
    a = _adapter()
    assert a.build_col_type({"type_name": "nvarchar", "type_length": 50}) == "NVARCHAR(50)"
    assert a.build_col_type({"type_name": "nvarchar", "type_length": -1}) == "NVARCHAR(MAX)"
    assert a.build_col_type({"type_name": "nvarchar", "type_length": None}) == "NVARCHAR(500)"

def test_col_type_specials():
    a = _adapter()
    assert a.build_col_type({"type_name": "bit"}) == "BIT"
    assert a.build_col_type({"type_name": "datetime2"}) == "DATETIME2"
    assert a.build_col_type({"type_name": "uniqueidentifier"}) == "UNIQUEIDENTIFIER"
    assert a.build_col_type({"type_name": "decimal", "type_precision": 10, "type_scale": 2}) == "DECIMAL(10,2)"

def test_quote_and_unbounded():
    a = _adapter()
    assert a.quote("nama") == "[nama]"
    assert a.is_unbounded("NVARCHAR(MAX)") is True
    assert a.is_unbounded("NVARCHAR(50)") is False

def test_upsert_sql_is_merge():
    a = _adapter()
    cols = ["npsn", "nama"]
    col_to_param = {"npsn": "p0", "nama": "p1"}
    sql = a.build_upsert_sql("sekolah", cols, ["npsn"], "dbo", col_to_param)
    assert "MERGE [dbo].[sekolah]" in sql
    assert ":p0 AS [npsn]" in sql
    assert "t.[npsn] = s.[npsn]" in sql
    assert "UPDATE SET t.[nama] = s.[nama]" in sql

def test_upsert_sql_has_no_null_unsafe_change_guard():
    a = _adapter()
    cols = ["npsn", "nama"]
    col_to_param = {"npsn": "p0", "nama": "p1"}
    sql = a.build_upsert_sql("sekolah", cols, ["npsn"], "dbo", col_to_param)
    assert "WHEN MATCHED AND" not in sql

def test_create_table_sql_has_guard_and_pk():
    a = _adapter()
    col_defs = [{"name": "npsn", "type_name": "nvarchar", "type_length": 20, "nullable": False},
                {"name": "nama", "type_name": "nvarchar", "type_length": -1}]
    sql = a.build_create_table_sql("sekolah", col_defs, ["npsn"], "dbo")
    assert "OBJECT_ID('[dbo].[sekolah]', 'U') IS NULL" in sql
    assert "[npsn] NVARCHAR(20) NOT NULL" in sql
    assert "PRIMARY KEY ([npsn])" in sql
