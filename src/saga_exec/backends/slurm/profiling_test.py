#!/usr/bin/env python3
"""A quick first profiling test: time the same CPU work on each machine and compare.

SUPERSEDED by saga_exec.calibrate.cpu (`uv run saga-calibrate-cpu`), which times the
exact cpu-mode task loop and writes machines.yaml. Kept for reference.

Each machine compresses the same data with zlib a fixed number of times, first on
one core and then on all of Slurm's CPUs at once. A faster machine takes less time.
This is one simple workload, so the speeds are only a rough first estimate. Other
software can also use Slurm's cores, which can skew the times.

Runs the same way as the executor: sbatch reserves both machines, then a step on
lambda starts the work on both machines and saves the results where we can read them.

Usage:
    uv run saga-slurm-profile
    (originally: python executor/profiling_test.py)
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from saga_exec.backends.slurm.slurm_common import NODES

LOG_DIR = Path.home() / "slurm-logs" / "profiling"
RESULTS_NODE = "cse-lambda1"         # where the results file is written and read
CPUS = 28                            # Slurm's share of each machine (14 cores = 28 CPUs)

# Sent to each machine as text, because home folders aren't shared. Uses only the
# standard library, because the venv doesn't exist on isha.
WORKER = r"""
import json, multiprocessing, os, platform, random, socket, time, zlib

ROUNDS = 40                                                           # fixed work per process
DATA = bytes(b % 16 for b in random.Random(0).randbytes(2_000_000))   # same data on every machine

def work(_):
    for _ in range(ROUNDS):
        zlib.compress(DATA, 6)

def timed(processes):
    start = time.perf_counter()
    with multiprocessing.Pool(processes) as pool:
        pool.map(work, range(processes))
    return time.perf_counter() - start

cpus = len(os.sched_getaffinity(0))
print(json.dumps({
    "machine": socket.gethostname(),
    "python": platform.python_version(),
    "cpus": cpus,
    "one_core_s": timed(1),
    "all_cpus_s": timed(cpus),
}), flush=True)
"""


def submit() -> Path:
    """Reserve both machines with sbatch and wait for the job. Returns the results file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOG_DIR / f"{datetime.now():%Y%m%d-%H%M%S}.txt"
    # Slurm runs the batch script on isha, so it starts this file again as a step on lambda.
    step = (f"srun --overlap --nodes=1 --ntasks=1 --nodelist={RESULTS_NODE} --mem=512M "
            f"{sys.executable} {Path(__file__).resolve()} {out_path}")
    print("Submitted. Waiting for the profiling job...")
    result = subprocess.run([
        "sbatch", "--wait", "--parsable",
        "--job-name=saga-profiling",
        f"--nodes={len(NODES)}",
        f"--nodelist={','.join(NODES)}",
        "--ntasks-per-node=1",
        f"--cpus-per-task={CPUS}",
        "--mem=1536M",
        "--chdir=/tmp",
        "--output=/dev/null",
        f"--wrap={step}",
    ], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Job ended with an error: {result.stderr.strip()}")
    return out_path


def run_workers(out_path: Path) -> None:
    """Inside the job, on lambda: run the worker once on each machine, saving the output."""
    with out_path.open("w") as out:
        subprocess.run([
            "srun",
            f"--nodes={len(NODES)}", f"--ntasks={len(NODES)}",
            "--exact",
            "--cpu-bind=none",                   # same as driver.py
            f"--cpus-per-task={CPUS}",
            "--mem=1G",
            "python3", "-c", WORKER,
        ], stdout=out, stderr=subprocess.STDOUT)


def print_results(out_path: Path) -> None:
    """Print each machine's times and its speed relative to the fastest machine."""
    if not out_path.exists():
        sys.exit("No results file. The job may not have started, so check squeue.")
    rows = []
    for line in out_path.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            print(line)                          # anything else is an error message
    if not rows:
        sys.exit(f"No results in {out_path}")

    fastest = min(r["all_cpus_s"] for r in rows)
    print(f"\n{'machine':<14}{'python':<10}{'cpus':>5}{'one core':>10}{'all cpus':>10}{'speed':>7}")
    speeds = {}
    for r in sorted(rows, key=lambda r: r["all_cpus_s"]):
        speeds[r["machine"]] = round(fastest / r["all_cpus_s"], 2)
        print(f"{r['machine']:<14}{r['python']:<10}{r['cpus']:>5}{r['one_core_s']:>9.2f}s"
              f"{r['all_cpus_s']:>9.2f}s{speeds[r['machine']]:>7.2f}")
    print(f'\nRough "nodes" for a run file: {json.dumps(speeds)}')
    print(f"Saved {out_path}")


def main() -> None:
    if len(sys.argv) == 2:                       # started by the job, given the results file
        run_workers(Path(sys.argv[1]))
    else:
        print_results(submit())


if __name__ == "__main__":
    main()
