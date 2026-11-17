"""
Create the end-to-end TEST database: same schema as production, no data.

Steps:
  1. (--create) CREATE DATABASE <test-db> on the production server
     (--drop-existing drops it first). Production's own database is never touched.
  2. Copy the schema only: pg_dump --schema-only of production (a read-only
     dump) piped into psql on the test database.
  3. Populate the (empty) materialized views once, so REFRESH ... CONCURRENTLY
     in pipeline4/sql/04_refresh_views.sql works.
  4. Run migration 001 (stage columns) and 002 (seller views) on the TEST db.

Usage:
    python tests/e2e/setup_test_db.py --create --drop-existing
"""
import argparse
import os
import subprocess
import sys

from e2e_common import (ROOT, PG_BIN, DEFAULT_TEST_DB, prod_dsn, test_dsn, connect_test,
                    pg_env, dsn_without_password, assert_is_test)
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def run(cmd, **kw):
    print("  $", " ".join(str(c) for c in cmd))
    subprocess.run(cmd, check=True, **kw)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--test-db", default=DEFAULT_TEST_DB)
    ap.add_argument("--create", action="store_true", help="CREATE DATABASE <test-db>")
    ap.add_argument("--drop-existing", action="store_true", help="DROP the test database first")
    args = ap.parse_args()

    tdsn = test_dsn(args.test_db)
    assert_is_test(tdsn)
    env = pg_env()

    if args.create:
        admin = conninfo_to_dict(prod_dsn())
        admin["dbname"] = "postgres"
        with psycopg.connect(make_conninfo(**admin), autocommit=True) as c:
            exists = c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (args.test_db,)).fetchone()
            if exists and args.drop_existing:
                print(f"Dropping {args.test_db} ...")
                c.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(args.test_db)))
                exists = None
            if not exists:
                print(f"Creating {args.test_db} ...")
                c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(args.test_db)))
            else:
                print(f"{args.test_db} already exists (use --drop-existing to recreate)")

    print("Copying schema (pg_dump --schema-only, read-only on production) ...")
    dump = subprocess.run(
        [str(PG_BIN / "pg_dump"), "--schema-only", "--no-owner", "--no-privileges",
         "-d", dsn_without_password(prod_dsn())],
        check=True, capture_output=True, env=env,
    ).stdout
    subprocess.run(
        [str(PG_BIN / "psql"), "-v", "ON_ERROR_STOP=1", "-q", "-d", dsn_without_password(tdsn)],
        input=dump, check=True, env=env,
    )

    print("Populating materialized views once ...")
    with connect_test(args.test_db) as c:
        for v in ("seller_categories", "seller_buyers"):
            c.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier(v)))

    test_env = dict(env, POSTGRES_DSN=tdsn)
    print("Migration 001 on TEST db ...")
    run([sys.executable, str(ROOT / "pipeline4/sql/migrations/run_001.py")], env=test_env, cwd=ROOT)
    print("Migration 002 on TEST db ...")
    run([str(PG_BIN / "psql"), "-v", "ON_ERROR_STOP=1", "-q", "-d", dsn_without_password(tdsn),
         "-f", str(ROOT / "pipeline4/sql/migrations/002_seller_views.sql")], env=env, cwd=ROOT)

    print(f"\nTest database ready: {args.test_db}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
