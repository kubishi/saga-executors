#!/usr/bin/env python3
"""Run a saga schedule inside one Slurm job.

allocation.py writes plan.json and job.sbatch into the run folder and submits
job.sbatch once. Slurm reserves both nodes, and job.sbatch starts this script as
a step on cse-lambda1 (Slurm itself runs the batch script on cse-isha, the first
node in its alphabetical order). The driver then starts every task as its own
`srun` job step on its assigned node as soon as
everything the task waits for has completed, and keeps going until all tasks
are done:

    loop:
        start every task whose dependencies have all COMPLETED   (srun -w <node> ...)
        when a step exits, record its result
    until nothing is pending or running

Each step's output (including steps on cse-isha) streams back through srun into
<run folder>/<task>.out on lambda. The task prints its own exact start/end times;
results.json collects them for allocation.py.

Uses only the Python standard library.

Usage (normally called by job.sbatch):
    python3 driver.py <run folder>/plan.json
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict

POLL = 0.005  # seconds between checks for finished steps


def log(t0: float, message: str) -> None:
    print(f"[{time.time() - t0:8.3f}s] {message}", flush=True)


def step_command(task: dict) -> list:
    """One srun job step: placeholder work that reports its own exact start and end,
    then which core Slurm gave it (done after "end" so it isn't part of the task's time)."""
    work = (f"echo start $(date +%s.%N) $(hostname); "
            f"sleep {task['duration']:.3f}; "
            f"echo end $(date +%s.%N); "
            # Linux CPUs this step may use (e.g. "5,37"), and the physical core the first one is on
            "cpus=$(grep Cpus_allowed_list /proc/self/status | cut -f2); "
            "echo core $(cat /sys/devices/system/cpu/cpu${cpus%%[,-]*}/topology/core_id) $cpus")
    return [
        "srun",
        "--nodes=1", "--ntasks=1",
        "--exact",                                  # use only the CPUs/memory asked for, so steps can share a node
        "--cpu-bind=none",                          # Slurm's pinning fails for a 2nd step on a node; cgroups still
                                                    # keep each step on its own core
        f"--nodelist={task['node']}",
        f"--cpus-per-task={task['cpus']}",
        f"--mem={task['mem']}",
        f"--job-name={task['name']}",
        "bash", "-c", work,
    ]


def parse_step_output(path: Path) -> dict:
    """Read the 'start <time> <host>', 'end <time>' and 'core <core> <cpus>' lines a step printed."""
    times: dict = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "core":
            times["core"] = parts[1]                                    # physical core number
            times["linux_cpus"] = parts[2] if len(parts) >= 3 else None
            continue
        if len(parts) >= 2 and parts[0] in ("start", "end"):
            try:
                times[parts[0]] = float(parts[1])
            except ValueError:
                continue
            if parts[0] == "start" and len(parts) >= 3:
                times["host"] = parts[2]
    return times


# ---------------------------------------------------------------------------
# Simulated data transfer
#
# No data actually moves. A task's data "arrives" once its parent has finished and
# the transfer time from plan.json has passed. Uses "finish" rather than "end",
# because "finish" and time.time() both come from this machine's clock.
# ---------------------------------------------------------------------------

def data_arrived(task: dict, results: Dict[str, dict]) -> bool:
    """True once the data from all of this task's parents would have arrived."""
    now = time.time()
    return all(now >= results[parent]["finish"] + seconds
               for parent, seconds in task["transfer_from"].items())


def main() -> None:
    plan_path = Path(sys.argv[1])
    plan = json.loads(plan_path.read_text())
    run_dir = plan_path.parent
    t0 = time.time()

    host = socket.gethostname()
    if host != plan["driver_node"]:
        sys.exit(f"driver is running on {host}, expected {plan['driver_node']}")

    tasks: Dict[str, dict] = {t["name"]: t for t in plan["tasks"]}
    pending = set(tasks)
    running: Dict[str, tuple] = {}       # name -> (process, launch time, output path)
    results: Dict[str, dict] = {}
    log(t0, f"driver on {host}: {len(tasks)} tasks, Slurm job {os.environ.get('SLURM_JOB_ID', '?')}")

    try:
        while pending or running:
            # 1. Collect steps that have exited.
            for name, (proc, launched, out_path) in list(running.items()):
                rc = proc.poll()
                if rc is None:
                    continue
                finished = time.time()
                del running[name]
                times = parse_step_output(out_path)
                state = "COMPLETED" if rc == 0 else "FAILED"
                results[name] = {
                    "node": times.get("host", tasks[name]["node"]),
                    "state": state, "exit_code": rc,
                    "launch": launched, "finish": finished,         # driver's clock (lambda)
                    "start": times.get("start"), "end": times.get("end"),   # the task's own clock
                    "core": times.get("core"), "linux_cpus": times.get("linux_cpus"),
                }
                log(t0, f"{name:<8} {state:<9} on {results[name]['node']} (exit {rc})")

            # 2. Never start tasks downstream of a failure.
            for name in sorted(pending):
                if any(results.get(d, {}).get("state") in ("FAILED", "SKIPPED")
                       for d in tasks[name]["waits_for"]):
                    pending.discard(name)
                    results[name] = {"node": tasks[name]["node"], "state": "SKIPPED"}
                    log(t0, f"{name:<8} SKIPPED (a dependency did not complete)")

            # 3. Start every task whose dependencies have all completed and whose data
            #    has arrived, earliest-planned first.
            deps_done = [n for n in pending
                         if all(results.get(d, {}).get("state") == "COMPLETED" for d in tasks[n]["waits_for"])]
            ready = [n for n in deps_done if data_arrived(tasks[n], results)]
            for name in sorted(ready, key=lambda n: (tasks[n]["predicted_start"], n)):
                out_path = run_dir / f"{name}.out"
                proc = subprocess.Popen(step_command(tasks[name]),
                                        stdout=out_path.open("w"), stderr=subprocess.STDOUT)
                running[name] = (proc, time.time(), out_path)
                pending.discard(name)
                log(t0, f"{name:<8} started   on {tasks[name]['node']}")

            if pending and not running and not deps_done:
                # Nothing can make progress (should not happen with a valid plan).
                # A task only waiting for its data isn't stuck, so it's in deps_done.
                for name in sorted(pending):
                    results[name] = {"node": tasks[name]["node"], "state": "SKIPPED"}
                log(t0, f"stuck: {sorted(pending)} can never start")
                break

            time.sleep(POLL)
    finally:
        for proc, _, _ in running.values():   # e.g. the job was cancelled
            proc.terminate()
        (run_dir / "results.json").write_text(json.dumps({
            "driver_node": host, "driver_start": t0, "tasks": results,
        }, indent=2))

    failed = [n for n, r in results.items() if r["state"] != "COMPLETED"]
    log(t0, f"done: {len(results) - len(failed)}/{len(tasks)} completed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
