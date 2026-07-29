from main import build_parser

def test_parser_run_once_flag():
    p = build_parser()
    assert p.parse_args(["--run-once"]).run_once is True
    assert p.parse_args([]).run_once is False
