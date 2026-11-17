"""
Run the whole end-to-end test against the TEST database, step by step.

  1. setup     create TenderIntelligence_test with production's schema (+ migrations 001, 002)
  2. seed      seed cases (a)/(b)/(c) for the test day
  3. run1      pipeline_main (pipeline2 + pipeline3) for that one day
  4. staged    checks.py staged
  5. p4        pipeline4 (validate, merge, profiles, competitors, views, cleanup)
  6. merged    checks.py merged
  7. run2      pipeline_main again for the same day (duplicate / idempotency test)
  8. rerun     checks.py rerun
  9. p4b       pipeline4 again
 10. merged2   checks.py merged

Production is only ever read (schema dump, seed copies). POSTGRES_DSN is set to
the TEST database for every pipeline step.

Usage:
    python tests/e2e/run_e2e.py                    # all steps, day 2025-07-15
    python tests/e2e/run_e2e.py --from-step run1   # resume from a step
    python tests/e2e/run_e2e.py --day 2025-07-15
"""
import argparse
import subprocess
import sys
import time

from e2e_common import ROOT, E2E_DIR, PG_BIN, DEFAULT_TEST_DB, test_dsn, pg_env, dsn_without_password

STEPS = ["setup", "seed", "run1", "staged", "p4", "merged", "run2", "rerun", "p4b", "merged2"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", default="2025-07-15")
    ap.add_argument("--test-db", default=DEFAULT_TEST_DB)
    ap.add_argument("--from-step", choices=STEPS, default="setup")
    ap.add_argument("--only", choices=STEPS)
    args = ap.parse_args()

    tdsn = test_dsn(args.test_db)
    # PYTHONUNBUFFERED: child steps print progress live even when output goes to a file
    env = dict(pg_env(), POSTGRES_DSN=tdsn, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
    py = sys.executable
    month, dd = args.day[:7], int(args.day[8:10])
    p4 = [str(PG_BIN / "psql"), "-v", "ON_ERROR_STOP=1", "-d", dsn_without_password(tdsn),
          "-f", str(ROOT / "pipeline4/sql/00_run_all.sql")]
    pipeline = [py, "-X", "utf8", str(ROOT / "pipeline_main/run.py"), month, "--days", f"{dd}-{dd}"]

    cmds = {
        "setup":   [py, str(E2E_DIR / "setup_test_db.py"), "--create", "--drop-existing", "--test-db", args.test_db],
        "seed":    [py, "-X", "utf8", str(E2E_DIR / "seed.py"), "--day", args.day, "--test-db", args.test_db],
        "run1":    pipeline,
        "staged":  [py, "-X", "utf8", str(E2E_DIR / "checks.py"), "staged"],
        "p4":      p4,
        "merged":  [py, "-X", "utf8", str(E2E_DIR / "checks.py"), "merged"],
        "run2":    pipeline,
        "rerun":   [py, "-X", "utf8", str(E2E_DIR / "checks.py"), "rerun"],
        "p4b":     p4,
        "merged2": [py, "-X", "utf8", str(E2E_DIR / "checks.py"), "merged"],
    }

    steps = [args.only] if args.only else STEPS[STEPS.index(args.from_step):]
    failed = []
    for s in steps:
        print(f"\n{'#' * 70}\n# STEP {s}\n{'#' * 70}", flush=True)
        t0 = time.time()
        rc = subprocess.run(cmds[s], env=env, cwd=ROOT).returncode
        print(f"# STEP {s}: exit {rc} in {time.time() - t0:.0f}s", flush=True)
        if rc != 0:
            failed.append(s)
            if s in ("setup", "seed", "p4", "p4b"):
                sys.exit(f"Step {s} failed - stopping.")
    print(f"\nFailed steps: {failed}" if failed else "\nAll steps passed.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
