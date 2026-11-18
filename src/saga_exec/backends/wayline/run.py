#!/usr/bin/env python3
"""Run converted templates on Wayline and collect the results, one run at a time.

Runs on lambda1 (needs kubectl and the venv). For each template folder made by
convert() (results/wayline/<graph>/<run name>/) and each repetition:

  1. apply network-profile.yml (the wl-network-profile ConfigMap from machines.yaml,
     so SAGA sees the bandwidths the offline prediction used) and template.yml
  2. start a run (ODAG) from it, the way `wayline run` does
  3. wait until it succeeds or fails, and until every task has its SDK times
  4. save runs/<odag>/status.json, runs/<odag>/logs.txt (incl. the task core's
     FID_TIMELINE lines) and runs/<odag>/clock.json (this machine's NTP state
     before and after; measurement only)
  5. delete the run and wait for its pods to go (Wayline does not clean up
     when profiling is off)
  6. analyze it: runs/<odag>/prediction.json (SAGA's, from status.scheduling),
     actual.json and gantt.png. A run SAGA did not place (the controller fell
     back to random placement) is logged as "Rejected" and not analyzed.

The cluster must run Wayline's controller from branch saga-executor (it records
SAGA's per-task prediction in status.scheduling) with a working saga-sidecar.

Each run adds one line to <out>/run-log.csv (default results/wayline/, relative to
the working directory; set SAGA_EXEC_WAYLINE_OUT to change it).

Usage (from the repo root):
  uv run saga-wayline-run results/wayline/smoke-diamond/heft-order-sleep-119bea [more folders] [--reps 20] [--shuffle SEED]
(originally, from fidelity/: python run.py out/smoke-diamond/heft-c8-sleep-a5faff ...)

--shuffle runs all (template, repetition) pairs in a random order, so slow
drifts on the shared machines do not line up with one template.
"""
import argparse
import csv
import os
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import subprocess
import time

import yaml

from saga_exec.backends.wayline import analyze
from saga_exec.clocks import local_clock

# Was HERE / "out" (next to the scripts); now under the working directory.
OUT = Path(os.environ.get("SAGA_EXEC_WAYLINE_OUT", "results/wayline")).resolve()
LOG = OUT / "run-log.csv"
POLL_SECONDS = 2
SDK_TIMES_GRACE = 30    # seconds to wait after success for the last task times
DELETE_TIMEOUT = 120


def kubectl(*args: str, stdin: str | None = None) -> str:
    result = subprocess.run(["kubectl", *args], input=stdin, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def unfinished_runs(namespace: str) -> list[str]:
    """Runs that are not done. CPU admission counts tasks across all runs, so
    a measurement is only clean when nothing else is running."""
    odags = json.loads(kubectl("get", "odag", "-n", namespace, "-o", "json"))["items"]
    return [
        o["metadata"]["name"]
        for o in odags
        if o.get("status", {}).get("phase") not in ("Succeeded", "Failed")
    ]


def start_run(template: dict) -> str:
    """Create an ODAG from the template, as `wayline run` does. Returns its name."""
    name = template["metadata"]["name"]
    namespace = template["metadata"]["namespace"]
    skip = ("profiling", "defaults", "retention", "description")   # template-only fields
    odag = {
        "apiVersion": "wl.io/v1",
        "kind": "ODAG",
        "metadata": {
            "generateName": f"{name}-run-",
            "namespace": namespace,
            "labels": {"wl.io/template": name},
        },
        "spec": {k: v for k, v in template["spec"].items() if k not in skip},
    }
    return kubectl("create", "-f", "-", "-o", "jsonpath={.metadata.name}", stdin=json.dumps(odag))


def wait_for(name: str, namespace: str, n_tasks: int, timeout: float) -> dict:
    """Poll the run until it is done. Returns its last status.

    "Succeeded" can arrive before the status worker has written the last
    task's SDK times, so after success wait (briefly) for those too.
    """
    deadline = time.monotonic() + timeout
    succeeded_at = None
    while True:
        status = json.loads(kubectl("get", "odag", name, "-n", namespace, "-o", "json"))
        phase = status.get("status", {}).get("phase")
        if phase == "Failed":
            return status
        if phase == "Succeeded":
            tasks = status["status"].get("tasks", [])
            if len(tasks) == n_tasks and all("taskCloseTime" in t for t in tasks):
                return status
            succeeded_at = succeeded_at or time.monotonic()
            if time.monotonic() - succeeded_at > SDK_TIMES_GRACE:
                return status
        if time.monotonic() > deadline:
            return status
        time.sleep(POLL_SECONDS)


def save_logs(name: str, namespace: str, path: Path) -> None:
    """Every task's log (the [fidelity] lines), each line prefixed with its pod."""
    try:
        logs = kubectl("logs", "-n", namespace, "-l", f"wl-odag={name}",
                       "--all-containers", "--prefix", "--tail=-1")
    except RuntimeError as e:
        logs = f"could not read logs: {e}\n"
    path.write_text(logs)


def delete_run(name: str, namespace: str) -> None:
    """Delete the run; its pods and data go with it. Waits for the pods."""
    kubectl("delete", "odag", name, "-n", namespace, "--wait=true")
    deadline = time.monotonic() + DELETE_TIMEOUT
    while kubectl("get", "pods", "-n", namespace, "-l", f"wl-odag={name}", "-o", "name").strip():
        if time.monotonic() > deadline:
            print(f"  warning: pods of {name} still present after {DELETE_TIMEOUT} s")
            return
        time.sleep(POLL_SECONDS)


def log_row(row: dict) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    log = LOG
    if log.exists() and log.read_text().split("\n", 1)[0] != ",".join(row):
        log = LOG.with_name("run-log.v2.csv")   # columns changed with the bridge workflow
    new = not log.exists()
    with log.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row))
        if new:
            writer.writeheader()
        writer.writerow(row)


def run_one(template_dir: Path, rep: int, seed: int | None) -> None:
    template = yaml.safe_load((template_dir / "template.yml").read_text())
    namespace = template["metadata"]["namespace"]
    offline = template_dir / "prediction.json"   # absent if the bridge could not run offline
    predicted = json.loads(offline.read_text())["makespan"] if offline.exists() else None

    busy = unfinished_runs(namespace)
    if busy:
        raise SystemExit(f"runs still active in {namespace}: {busy}. Runs must go one at a time; "
                         "wait for them or delete them, then start again.")

    if (template_dir / "network-profile.yml").exists():
        kubectl("apply", "-f", str(template_dir / "network-profile.yml"))
    kubectl("apply", "-f", str(template_dir / "template.yml"))
    clock_before = local_clock()     # measurement only: this machine's NTP state (docs/formats.md, Clocks)
    name = start_run(template)
    print(f"  {name}: started (offline prediction "
          f"{'none' if predicted is None else f'{predicted:.3f} s'})")
    try:
        status = wait_for(name, namespace, len(template["spec"]["tasks"]),
                          timeout=120 + 10 * (predicted or 60))
    except KeyboardInterrupt:
        print(f"\nstopped: {name} is still in the cluster; save and delete it by hand")
        raise

    phase = status.get("status", {}).get("phase") or "Pending"
    if phase not in ("Succeeded", "Failed"):
        phase = f"TimedOut ({phase})"

    clock_after = local_clock()
    run_dir = template_dir / "runs" / name
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "status.json").write_text(json.dumps(status, indent=2) + "\n")
    (run_dir / "clock.json").write_text(json.dumps({
        "how": "run.py on the submitting machine, just before the run was created and after it ended",
        "machines": {clock_before["host"]: {"before": clock_before, "after": clock_after}},
        "missing": "the other machines: read `chronyc tracking` there (docs/formats.md, Clocks)",
    }, indent=2) + "\n")
    save_logs(name, namespace, run_dir / "logs.txt")
    delete_run(name, namespace)

    makespan, saga_makespan, same = "", "", ""
    scheduling = status.get("status", {}).get("scheduling") or {}
    if phase == "Succeeded":
        try:
            comparison = analyze.analyze(run_dir / "status.json")
            makespan = json.loads((run_dir / "actual.json").read_text())["makespan"]
            saga_makespan = json.loads((run_dir / "prediction.json").read_text())["makespan"]
            same = comparison.get("same_schedule", "")
        except analyze.RejectedRun as e:
            phase = "Rejected"
            print(f"  {e}")
        except Exception as e:   # keep going: status.json is saved, analyze can be rerun
            print(f"  analyze failed: {e}")
    else:
        print(f"  {name}: {phase}; status and logs saved in {run_dir}")

    log_row({
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "template": template["metadata"]["name"],
        "rep": rep,
        "run": name,
        "phase": phase,
        "placed_by": scheduling.get("used", ""),
        "predicted": saga_makespan,
        "offline_predicted": "" if predicted is None else predicted,
        "same_as_offline": same,
        "makespan": makespan,
        "shuffle_seed": "" if seed is None else seed,
    })


def run_all(template_dirs: list[Path], reps: int = 1, shuffle: int | None = None) -> None:
    jobs = [(d, rep) for d in template_dirs for rep in range(1, reps + 1)]
    if shuffle is not None:
        random.Random(shuffle).shuffle(jobs)
    for i, (template_dir, rep) in enumerate(jobs, 1):
        try:
            label = template_dir.relative_to(OUT)
        except ValueError:
            label = template_dir
        print(f"[{i}/{len(jobs)}] {label} rep {rep}")
        run_one(template_dir, rep, shuffle)


def main() -> None:
    p = argparse.ArgumentParser(description="Run converted templates on Wayline, one at a time.")
    p.add_argument("templates", nargs="+", type=Path, help="template folders (out/<graph>/<run name>)")
    p.add_argument("--reps", type=int, default=1, help="runs per template")
    p.add_argument("--shuffle", type=int, metavar="SEED",
                   help="run all (template, rep) pairs in a random order from this seed")
    args = p.parse_args()
    run_all([d.resolve() for d in args.templates], args.reps, args.shuffle)


if __name__ == "__main__":
    main()
