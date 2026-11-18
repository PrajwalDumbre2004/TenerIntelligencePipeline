"""
Run a memory test of the scraping pipeline in its Linux container.

The pipeline writes to the e2e TEST database (TenderIntelligence_test), never
production: the DSN comes from tests/e2e/e2e_common.test_dsn(), which refuses
the production database. It is passed to Docker through the environment, not
on the command line. Results go to memtest_results/<name>.{csv,log,json}.

Run from the repo root with the venv's Python and Docker Desktop running:

  # A: browser baseline, one day, default workers (build the image first time)
  python deploy/run_memtest.py --build --name A_day16 -- pipeline_main/run.py 2026-03 --days 16-16

  # B: pipeline2 only, a whole large month (its peak grows with month size)
  python deploy/run_memtest.py --name B_p2_full --phase pipeline2 -- pipeline2/run.py 2026-07

  # C: fewer workers, on a different day of the same month
  python deploy/run_memtest.py --name C_w6_day17 --p2-workers 6 --p3-workers 6 -- pipeline_main/run.py 2026-03 --days 17-17

Use a different day (or month) for every run: pipeline3 scrapes every staged,
unscraped bid of the month, and bids already scraped in an earlier test are
skipped. Don't run test B on the month used for A/C.
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests" / "e2e"))

from e2e_common import test_dsn, assert_is_test           # noqa: E402
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

IMAGE = "tender-pipeline:local"
OUT_DIR = ROOT / "memtest_results"


def container_dsn() -> str:
    """Test-DB DSN as seen from inside a container (localhost -> the host machine)."""
    dsn = test_dsn()
    assert_is_test(dsn)
    d = conninfo_to_dict(dsn)
    if d.get("host", "localhost") in ("localhost", "127.0.0.1", "::1", ""):
        d["host"] = "host.docker.internal"
    return make_conninfo(**d)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True, help="result file name, e.g. A_day16")
    ap.add_argument("--build", action="store_true", help="build the image first")
    # Docker Desktop's WSL2 VM gets ~50% of host RAM (~15.7 GB here), so keep the limit below that
    ap.add_argument("--memory", default="12g", help="container memory limit (no swap); default 12g")
    ap.add_argument("--cpus", default="4", help="CPU limit; default 4 (as on Azure)")
    ap.add_argument("--phase", default="start", help="initial phase label (use pipeline2 for pipeline2/run.py)")
    ap.add_argument("--p2-workers", type=int, help="pipeline2 NUM_WORKERS (default 10)")
    ap.add_argument("--p3-workers", type=int, help="pipeline3 NUM_WORKERS (default 10)")
    ap.add_argument("--p3-pages", type=int, help="pipeline3 PAGES_PER_BROWSER (default 3)")
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="-- script and arguments, run with python3 in the container")
    args = ap.parse_args()

    cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
    if not cmd:
        ap.error("no command given, e.g. -- pipeline_main/run.py 2026-03 --days 16-16")

    if args.build:
        subprocess.run(["docker", "build", "-f", "deploy/Dockerfile", "-t", IMAGE, "."], cwd=ROOT, check=True)

    OUT_DIR.mkdir(exist_ok=True)
    docker = [
        "docker", "run", "--rm", "--init", "--name", f"memtest-{args.name}",
        "--memory", args.memory, "--memory-swap", args.memory,      # no swap: hitting the limit shows as an OOM kill
        "--cpus", args.cpus,
        "--mount", f"type=bind,source={OUT_DIR},target=/out",
        "-e", "POSTGRES_DSN",                                        # value taken from the environment below
    ]
    for var, value in (("P2_NUM_WORKERS", args.p2_workers), ("P3_NUM_WORKERS", args.p3_workers),
                       ("P3_PAGES_PER_BROWSER", args.p3_pages)):
        if value is not None:
            docker += ["-e", f"{var}={value}"]
    docker += [IMAGE, "python3", "deploy/memprobe.py", "--out", f"/out/{args.name}",
               "--phase", args.phase, "--", "python3", *cmd]

    env = dict(os.environ, POSTGRES_DSN=container_dsn())
    sys.exit(subprocess.run(docker, env=env).returncode)


if __name__ == "__main__":
    main()
