from types import SimpleNamespace
from backbone_pull.db.mysql import MySQLAdapter

def _adapter():
    s = SimpleNamespace(db_host="h", db_port=3306, db_user="u",
                        db_password="p", db_name="db",
                        db_auto_create_database=True, db_maintenance_db="mysql")
    return MySQLAdapter(s)

def test_dsn_url_encodes_special_char_password():
    s = SimpleNamespace(db_host="h", db_port=3306, db_user="u",
                        db_password="p@ss:w/rd%x", db_name="db",
                        db_auto_create_database=True, db_maintenance_db="mysql")
    a = MySQLAdapter(s)
    assert a._engine.url.password == "p@ss:w/rd%x"
    assert a._engine.url.database == "db"

def test_col_type_mapping():
    a = _adapter()
    assert a.build_col_type({"type_name": "nvarchar", "type_length": 50}) == "VARCHAR(50)"
    assert a.build_col_type({"type_name": "nvarchar", "type_length": -1}) == "TEXT"
    assert a.build_col_type({"type_name": "nvarchar", "type_length": None}) == "VARCHAR(500)"
    assert a.build_col_type({"type_name": "bit"}) == "SMALLINT"
    assert a.build_col_type({"type_name": "datetime2"}) == "DATETIME"
    assert a.build_col_type({"type_name": "uniqueidentifier"}) == "VARCHAR(36)"
    assert a.build_col_type({"type_name": "decimal", "type_precision": 10, "type_scale": 2}) == "DECIMAL(10,2)"
    assert a.build_col_type({"type_name": "varbinary", "type_length": -1}) == "LONGBLOB"

def test_quote_and_unbounded():
    a = _adapter()
    assert a.quote("nama") == "`nama`"
    assert a.is_unbounded("TEXT") is True
    assert a.is_unbounded("LONGBLOB") is True
    assert a.is_unbounded("VARCHAR(50)") is False

def test_table_name_is_schema_prefixed():
    a = _adapter()
    col_defs = [{"name": "npsn", "type_name": "nvarchar", "type_length": 20, "nullable": False}]
    sql = a.build_create_table_sql("sekolah", col_defs, ["npsn"], "dbo")
    assert "`dbo_sekolah`" in sql
    sql2 = a.build_create_table_sql("v_ptk", col_defs, ["npsn"], "vld")
    assert "`vld_v_ptk`" in sql2

def test_create_table_sql_if_not_exists_and_pk():
    a = _adapter()
    col_defs = [{"name": "npsn", "type_name": "nvarchar", "type_length": 20, "nullable": False},
                {"name": "nama", "type_name": "nvarchar", "type_length": -1}]
    sql = a.build_create_table_sql("sekolah", col_defs, ["npsn"], "dbo")
    assert "CREATE TABLE IF NOT EXISTS `dbo_sekolah`" in sql
    assert "`npsn` VARCHAR(20) NOT NULL" in sql
    assert "PRIMARY KEY (`npsn`)" in sql

def test_add_column_sql():
    a = _adapter()
    sql = a.build_add_column_sql("sekolah", {"name": "nama", "type_name": "nvarchar", "type_length": 100}, "dbo")
    assert "ALTER TABLE `dbo_sekolah`" in sql
    assert "ADD COLUMN `nama` VARCHAR(100) NULL" in sql

def test_upsert_sql_is_on_duplicate_key():
    a = _adapter()
    cols = ["npsn", "nama"]
    col_to_param = {"npsn": "p0", "nama": "p1"}
    sql = a.build_upsert_sql("sekolah", cols, ["npsn"], "dbo", col_to_param)
    assert "INSERT INTO `dbo_sekolah`" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "`nama`=VALUES(`nama`)" in sql

def test_upsert_sql_no_non_pk_cols_is_safe_noop():
    a = _adapter()
    cols = ["npsn"]
    col_to_param = {"npsn": "p0"}
    sql = a.build_upsert_sql("sekolah", cols, ["npsn"], "dbo", col_to_param)
    assert "ON DUPLICATE KEY UPDATE `npsn`=`npsn`" in sql

def test_insert_sql():
    a = _adapter()
    cols = ["npsn", "nama"]
    col_to_param = {"npsn": "p0", "nama": "p1"}
    sql = a.build_insert_sql("sekolah", cols, "dbo", col_to_param)
    assert "INSERT INTO `dbo_sekolah` (`npsn`, `nama`) VALUES (:p0, :p1)" in sql
