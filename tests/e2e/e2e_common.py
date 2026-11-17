"""
Shared helpers for the end-to-end test scripts.

PROD = the database in .env (POSTGRES_DSN). The e2e scripts only ever READ from
it (read-only session).
TEST = a separate database on the same server (default TenderIntelligence_test).
Everything the pipeline writes during the test goes there.
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "research"))     # gem_probe

from dotenv import dotenv_values                # noqa: E402
import psycopg                                  # noqa: E402
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

E2E_DIR = Path(__file__).resolve().parent
MANIFEST = E2E_DIR / "seed_manifest.json"
DEFAULT_TEST_DB = "TenderIntelligence_test"
PG_BIN = Path(os.environ.get("PG_BIN", r"C:\Program Files\PostgreSQL\18\bin"))


def prod_dsn() -> str:
    """Production DSN read straight from .env (never from the environment, so a
    POSTGRES_DSN pointed at the test DB can't be mistaken for production)."""
    dsn = dotenv_values(ROOT / ".env").get("POSTGRES_DSN")
    if not dsn:
        sys.exit("POSTGRES_DSN missing from .env")
    return dsn


def test_dsn(test_db: str = DEFAULT_TEST_DB) -> str:
    d = conninfo_to_dict(prod_dsn())
    if test_db == d.get("dbname"):
        sys.exit(f"Refusing: test database '{test_db}' is the production database.")
    d["dbname"] = test_db
    return make_conninfo(**d)


def assert_is_test(dsn: str):
    if conninfo_to_dict(dsn).get("dbname") == conninfo_to_dict(prod_dsn()).get("dbname"):
        sys.exit("Refusing: this DSN points at the production database.")


def connect_prod_ro():
    c = psycopg.connect(prod_dsn(), autocommit=True)
    c.execute("SET default_transaction_read_only = on")
    c.execute("SET statement_timeout = '300s'")
    return c


def connect_test(test_db: str = DEFAULT_TEST_DB):
    dsn = test_dsn(test_db)
    assert_is_test(dsn)
    return psycopg.connect(dsn, autocommit=True)


def pg_env() -> dict:
    """Environment for pg_dump/psql (password via PGPASSWORD, never on the command line)."""
    env = os.environ.copy()
    pw = conninfo_to_dict(prod_dsn()).get("password")
    if pw:
        env["PGPASSWORD"] = pw
    return env


def dsn_without_password(dsn: str) -> str:
    d = conninfo_to_dict(dsn)
    d.pop("password", None)
    return make_conninfo(**d)


def load_manifest() -> dict:
    if not MANIFEST.exists():
        sys.exit(f"{MANIFEST} not found - run tests/e2e/seed.py first")
    return json.loads(MANIFEST.read_text(encoding="utf-8"))
