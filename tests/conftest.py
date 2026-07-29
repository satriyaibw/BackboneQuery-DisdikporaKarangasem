import os

# Dummy defaults so importing modules that build settings at import time
# (main.py -> flow.py -> load_settings()) works during tests without a real .env.
# Uses setdefault so tests that set their own env via monkeypatch still win.
os.environ.setdefault("DB_DIALECT", "sqlserver")
os.environ.setdefault("DB_HOST", "test-host")
os.environ.setdefault("DB_USER", "test-user")
os.environ.setdefault("DB_PASSWORD", "test-pass")
os.environ.setdefault("DB_NAME", "test-db")
os.environ.setdefault("BACKBONE_API_KEY", "test-key")
os.environ.setdefault("BACKBONE_USERNAME", "test-user")
os.environ.setdefault("BACKBONE_PASSWORD", "test-pass")
