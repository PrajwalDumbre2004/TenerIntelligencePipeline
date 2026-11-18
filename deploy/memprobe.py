"""
Memory probe: run a command inside the container and record the container's
memory every few seconds, labelled with the pipeline phase.

Writes three files next to --out:
  <out>.csv   one row per sample
  <out>.log   the command's full output
  <out>.json  summary (also printed at the end)

"working set" = cgroup memory usage minus inactive file cache. It is the
figure Azure Container Apps (and Kubernetes) report and act on.
Per-process RSS columns count shared pages once per process, so they overstate;
use them to see WHO uses memory, not as the total.

Usage (inside the container; deploy/run_memtest.py does this for you):
    python3 deploy/memprobe.py --out /out/A_day16 -- python3 pipeline_main/run.py 2026-03 --days 16-16
"""
import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

CGROUP = Path("/sys/fs/cgroup")
MIB = 1024 * 1024
# Lines pipeline_main/run.py prints when a phase starts
PHASE_MARKERS = (("[STEP 1] Pipeline2", "pipeline2"), ("[STEP 2] Pipeline3", "pipeline3"))
WORKER_ENV = ("P2_NUM_WORKERS", "P3_NUM_WORKERS", "P3_PAGES_PER_BROWSER")


def _read_int(path):
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def _mib(n):
    return None if n is None else round(n / MIB)


def cgroup_memory():
    """(working_set_bytes, usage_bytes) for this container, or (None, None)."""
    usage = _read_int(CGROUP / "memory.current")
    if usage is None:
        return None, None
    inactive_file = 0
    try:
        for line in (CGROUP / "memory.stat").read_text().splitlines():
            key, value = line.split()
            if key == "inactive_file":
                inactive_file = int(value)
                break
    except OSError:
        pass
    return max(usage - inactive_file, 0), usage


def rss_by_kind():
    """Resident memory summed per process kind, and the number of Chromium processes."""
    totals = {"chromium": 0, "python": 0, "other": 0}
    chromium_procs = 0
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            status = (proc_dir / "status").read_text()
        except OSError:
            continue
        fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
        rss = fields.get("VmRSS")
        if not rss:
            continue
        rss_bytes = int(rss.split()[0]) * 1024
        name = fields.get("Name", "").strip()
        if "chrom" in name or "headless" in name:      # chrome, chrome_crashpad, headless_shell
            totals["chromium"] += rss_bytes
            chromium_procs += 1
        elif name.startswith("python"):
            totals["python"] += rss_bytes
        else:
            totals["other"] += rss_bytes
    return totals, chromium_procs


def oom_kills():
    try:
        for line in (CGROUP / "memory.events").read_text().splitlines():
            key, value = line.split()
            if key == "oom_kill":
                return int(value)
    except OSError:
        pass
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="output path without extension, e.g. /out/A_day16")
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between samples")
    ap.add_argument("--phase", default="start",
                    help="phase label until a phase marker appears (use 'pipeline2' for pipeline2/run.py)")
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="-- command to run")
    args = ap.parse_args()

    cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
    if not cmd:
        ap.error("no command given (put it after --)")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    state = {"phase": args.phase}
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=dict(os.environ, PYTHONUNBUFFERED="1"),
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    # Pass Ctrl+C / docker stop on to the pipeline (it shuts its workers down itself)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda signum, _frame: proc.send_signal(signum))

    def pump_output():
        with open(out.with_suffix(".log"), "w", encoding="utf-8") as log:
            for line in proc.stdout:
                sys.stdout.write(line)
                log.write(line)
                for marker, phase in PHASE_MARKERS:
                    if marker in line:
                        state["phase"] = phase

    reader = threading.Thread(target=pump_output, daemon=True)
    reader.start()

    started = time.time()
    peak_by_phase = {}
    peak_rss_by_phase = {}
    max_chromium = 0
    with open(out.with_suffix(".csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["elapsed_s", "phase", "working_set_mib", "usage_mib",
                         "rss_chromium_mib", "rss_python_mib", "rss_other_mib", "chromium_procs"])
        finished = False
        while not finished:
            working_set, usage = cgroup_memory()
            rss, chromium_procs = rss_by_kind()
            phase = state["phase"]
            if working_set is not None:
                peak_by_phase[phase] = max(peak_by_phase.get(phase, 0), working_set)
            peak_rss_by_phase[phase] = max(peak_rss_by_phase.get(phase, 0), sum(rss.values()))
            max_chromium = max(max_chromium, chromium_procs)
            writer.writerow([round(time.time() - started), phase, _mib(working_set), _mib(usage),
                             _mib(rss["chromium"]), _mib(rss["python"]), _mib(rss["other"]),
                             chromium_procs])
            f.flush()
            try:
                proc.wait(timeout=args.interval)
                finished = True
            except subprocess.TimeoutExpired:
                pass
    reader.join(timeout=10)

    limit = _read_int(CGROUP / "memory.max")          # None when unlimited ("max")
    summary = {
        "command": cmd,
        "exit_code": proc.returncode,
        "duration_min": round((time.time() - started) / 60, 1),
        "workers": {k: os.environ.get(k, "default") for k in WORKER_ENV},
        "memory_limit_mib": _mib(limit),
        "peak_working_set_mib": _mib(max(peak_by_phase.values(), default=None)),
        "peak_working_set_mib_by_phase": {p: _mib(v) for p, v in peak_by_phase.items()},
        "peak_rss_sum_mib_by_phase": {p: _mib(v) for p, v in peak_rss_by_phase.items()},
        # True high-water mark incl. page cache; catches spikes between samples
        "cgroup_memory_peak_mib": _mib(_read_int(CGROUP / "memory.peak")),
        "max_chromium_procs": max_chromium,
        "oom_kills": oom_kills(),
    }
    out.with_suffix(".json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("\n" + "=" * 70 + "\n  MEMORY SUMMARY\n" + "=" * 70)
    print(json.dumps(summary, indent=2))
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
