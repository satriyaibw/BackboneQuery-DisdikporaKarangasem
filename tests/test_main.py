from main import build_parser, parse_tables_arg

def test_parser_run_once_flag():
    p = build_parser()
    assert p.parse_args(["--run-once"]).run_once is True
    assert p.parse_args([]).run_once is False

def test_parser_tables_flag_default_and_value():
    p = build_parser()
    assert p.parse_args([]).tables is None
    assert p.parse_args(["--tables", "guru,ats"]).tables == "guru,ats"

def test_parse_tables_arg_none_or_empty():
    assert parse_tables_arg(None) is None
    assert parse_tables_arg("") is None
    assert parse_tables_arg("  ,  ,") is None

def test_parse_tables_arg_splits_and_trims():
    assert parse_tables_arg("guru,ats") == ["guru", "ats"]
    assert parse_tables_arg(" guru , ats ,,") == ["guru", "ats"]
    assert parse_tables_arg("guru") == ["guru"]
