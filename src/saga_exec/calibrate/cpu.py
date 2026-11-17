#!/usr/bin/env python3
"""CPU calibration for cpu mode (NOT RUN YET): sha256 hash rate per task slot, per machine.

What it measures: the exact loop cpu-mode tasks run (task/fidelity_core.hash_loop,
chained sha256 of 32 bytes), in task.pyz, under the same uv-managed Python the
tasks use (run setting task_python), inside a Slurm job that holds Slurm's whole
share of each machine (saga_nodes_per_machine cores = 2x Slurm CPUs):

  single  one process pinned to one physical core, the rest idle
  loaded  one process per physical core, all at once, each pinned to both
          hardware threads of its core (what a task with --cpus-per-task=2 gets
          when SAGA fills every core: contention for memory bandwidth, caches,
          power/turbo budget; not for the core itself)

How results land in testbed/machines.yaml (one block between the "BEGIN/END
calibration" marker comments, rewritten each time; everything else in the file
is kept as is):

  calibration.cpu.slurm.machines.<m>   raw rates (sha256/s per task slot), Python, OpenSSL
  calibration.cpu.slurm.basis          "loaded" (default) or "single": which rate counts
  calibration.cpu.slurm.reference      machine defined as speed 1.0 (default: first in the file)
  calibration.cpu.slurm.hash_rate      basis rate of the reference = FID_HASH_RATE: in cpu mode a
                                       task of runtime R (s at speed 1.0) does R * hash_rate hashes
  calibration.cpu.slurm.speed          basis rate / reference rate, per machine
  machines.<m>.speed                   set to that speed (the line gets a "# calibrated" comment),
                                       unless --no-set-speed. SAGA's runtimes use it, so the
                                       prediction assumes the speeds the cpu-mode work really has.

The Slurm backend reads hash_rate when a run file has "mode": "cpu" and no
"hash_rate" (Testbed.hash_rate("slurm")).

Kubernetes / Wayline (outline, not implemented): run the same `task.py calibrate`
in the wl-fidelity-task image as pods with cpu: "1" (one hardware thread, see
testbed/k8s/README.md) on each machine, 1 pod and then C pods at once, and write
calibration.cpu.k8s the same way (convert.Config.hash_rate). Until the k8s CPU
unit is a whole core, the k8s and Slurm rates are expected to differ.

Usage (on cse-lambda1; submits a Slurm job that holds both machines' Slurm share):
    uv run saga-calibrate-cpu [--seconds 10] [--reps 3] [--basis loaded] [--no-set-speed]
    uv run saga-calibrate-cpu --apply <results.jsonl>     # only (re)write machines.yaml
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml

from saga_exec.testbed import DEFAULT_MACHINES_YAML, load_testbed

LOG_DIR = Path.home() / "slurm-logs" / "calibrate"
DRIVER_NODE = "cse-lambda1"
BEGIN = "# BEGIN calibration (written by saga_exec.calibrate; do not edit by hand)"
END = "# END calibration"


# ---------------------------------------------------------------------------
# machines.yaml (pure; tested offline)
# ---------------------------------------------------------------------------

def summarize(rows: list[dict], basis: str = "loaded", reference: str | None = None,
              backend: str = "slurm", measured: str | None = None) -> dict:
    """Worker JSON lines -> calibration.cpu.<backend> dict."""
    if basis not in ("loaded", "single"):
        raise ValueError("basis must be loaded or single")
    by_machine = {r["machine"]: r for r in rows}
    reference = reference or next(iter(by_machine))
    if reference not in by_machine:
        raise ValueError(f"reference {reference!r} was not measured; have {sorted(by_machine)}")
    ref_rate = float(by_machine[reference][basis])
    return {
        "measured": measured or datetime.now().isoformat(timespec="seconds"),
        "method": ("chained sha256 (task/fidelity_core.hash_loop) in task.pyz; single = 1 process on 1 core, "
                   "loaded = 1 process per physical core of the allocation, each pinned to its 2 threads"),
        "backend": backend,
        "basis": basis,
        "reference": reference,
        "hash_rate": round(ref_rate, 1),
        "speed": {m: round(float(r[basis]) / ref_rate, 4) for m, r in by_machine.items()},
        "machines": {m: {"single": round(float(r["single"]), 1), "loaded": round(float(r["loaded"]), 1),
                         "loaded_min_process": round(float(r.get("loaded_min_process", 0.0)), 1),
                         "cores": r.get("cores"), "python": r.get("python"), "openssl": r.get("openssl")}
                     for m, r in by_machine.items()},
    }


def apply_to_yaml(text: str, cal: dict, set_speed: bool = True) -> str:
    """Write calibration.cpu.<backend> into machines.yaml text (marker block at the
    end, merged with what is already there) and, if set_speed, each machine's speed."""
    current = yaml.safe_load(text) or {}
    calibration = current.get("calibration") or {}
    calibration.setdefault("cpu", {})[cal["backend"]] = {k: v for k, v in cal.items() if k != "backend"}
    block = "\n".join([BEGIN, yaml.safe_dump({"calibration": calibration}, sort_keys=False).rstrip(), END])
    if BEGIN in text:
        text = re.sub(re.escape(BEGIN) + r".*?" + re.escape(END), lambda _: block, text, flags=re.S)
    elif re.search(r"^calibration:", text, flags=re.M):
        raise ValueError("machines.yaml has a calibration: key outside the marker block; move it into one")
    else:
        text = text.rstrip("\n") + "\n\n" + block + "\n"
    if set_speed:
        for m, s in cal["speed"].items():
            text = set_machine_speed(text, m, s, f"calibrated: calibration.cpu.{cal['backend']} "
                                                 f"({cal['basis']}, {cal['measured'][:10]})")
    return text


def set_machine_speed(text: str, machine: str, speed: float, note: str) -> str:
    """Replace `speed:` under machines.<machine> (4-space indent), keeping the layout."""
    lines = text.split("\n")
    in_machines = in_machine = done = False
    for i, line in enumerate(lines):
        if re.match(r"^\S", line):
            in_machines = line.startswith("machines:")
            in_machine = False
        elif in_machines and re.match(r"^  \S", line):
            in_machine = line.strip().rstrip(":") == machine
        elif in_machine and re.match(r"^    speed:", line):
            lines[i] = f"    speed: {speed}".ljust(25) + f"# {note}"
            done = True
    if not done:
        raise ValueError(f"no machines.{machine}.speed line in machines.yaml")
    return "\n".join(lines)


def update_machines_yaml(rows: list[dict], path: Path = DEFAULT_MACHINES_YAML, *, basis: str = "loaded",
                         set_speed: bool = True) -> dict:
    tb = load_testbed(path)
    missing = set(tb.machines) - {r["machine"] for r in rows}
    if missing:
        raise ValueError(f"no results for {sorted(missing)}")
    cal = summarize(rows, basis=basis, reference=tb.machines[0])
    path.write_text(apply_to_yaml(path.read_text(), cal, set_speed=set_speed))
    load_testbed(path)                                  # still valid
    return cal


# ---------------------------------------------------------------------------
# The Slurm job
# ---------------------------------------------------------------------------

def submit(args) -> Path:
    """Reserve Slurm's share of every machine, run the worker on each, wait. Returns results.jsonl."""
    from saga_exec.backends.slurm.allocation import build_task_pyz
    from saga_exec.backends.slurm.slurm_common import THREADS_PER_CORE

    tb = load_testbed()
    cores = min(tb.partition("slurm").cores.values())
    out_dir = LOG_DIR / datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    build_task_pyz(out_dir / "task.pyz")
    inside = (f"srun --overlap --nodes=1 --ntasks=1 --nodelist={DRIVER_NODE} --mem=256M "
              f"{sys.executable} -m saga_exec.calibrate.cpu --inside {out_dir} "
              f"--seconds {args.seconds} --reps {args.reps} --cpus {cores * THREADS_PER_CORE} "
              f"--task-python '{args.task_python}'")
    print(f"Submitting; results in {out_dir}. Waiting for the calibration job...")
    result = subprocess.run([
        "sbatch", "--wait", "--parsable", "--job-name=saga-calibrate-cpu",
        f"--nodes={len(tb.machines)}", f"--nodelist={','.join(tb.machines)}",
        "--ntasks-per-node=1", f"--cpus-per-task={cores * THREADS_PER_CORE}", "--mem=2G",
        "--chdir=/tmp", f"--output={out_dir}/job.out", f"--wrap={inside}",
    ], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Job ended with an error: {result.stderr.strip()}")
    return out_dir / "results.jsonl"


def run_inside(out_dir: Path, seconds: float, reps: int, task_python: str, cpus: int) -> None:
    """Inside the job, on lambda: sbcast task.pyz, run the worker once per machine at once."""
    remote = f"/tmp/saga-calibrate-{out_dir.name}.pyz"
    subprocess.run(["sbcast", "--force", str(out_dir / "task.pyz"), remote], check=True)
    nodes = int(os.environ.get("SLURM_JOB_NUM_NODES", "1"))
    work = f"exec {task_python} {remote} calibrate --seconds {seconds} --reps {reps}"
    with (out_dir / "results.jsonl").open("w") as out:
        subprocess.run(["srun", f"--nodes={nodes}", f"--ntasks={nodes}", "--exact", "--cpu-bind=none",
                        f"--cpus-per-task={cpus}", "--mem=1G", "bash", "-c", work],
                       stdout=out, stderr=subprocess.STDOUT)
    subprocess.run(["srun", f"--nodes={nodes}", f"--ntasks={nodes}", "--overlap", "--mem=64M",
                    "rm", "-f", remote])


def read_rows(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            print(line)                                # srun / Python errors
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(prog="saga-calibrate-cpu", description=__doc__.split("\n")[0])
    ap.add_argument("--seconds", type=float, default=10.0)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--basis", choices=("loaded", "single"), default="loaded")
    ap.add_argument("--no-set-speed", action="store_true", help="only write the calibration block")
    ap.add_argument("--task-python", default="$HOME/.local/bin/python3.12")
    ap.add_argument("--apply", type=Path, help="results.jsonl of an earlier job: only update machines.yaml")
    ap.add_argument("--inside", type=Path, help=argparse.SUPPRESS)
    ap.add_argument("--cpus", type=int, default=2, help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.inside:
        return run_inside(args.inside, args.seconds, args.reps, args.task_python, args.cpus)
    path = args.apply or submit(args)
    rows = read_rows(path) if path.exists() else []
    if not rows:
        sys.exit(f"No results in {path}")
    for r in rows:
        print(f"{r['machine']:<14} python {r['python']:<8} cores {r['cores']:>3}  "
              f"single {r['single']:>12,.0f}/s  loaded {r['loaded']:>12,.0f}/s")
    cal = update_machines_yaml(rows, basis=args.basis, set_speed=not args.no_set_speed)
    print(f"hash_rate (FID_HASH_RATE) {cal['hash_rate']:,.0f}/s, speeds {cal['speed']} -> {DEFAULT_MACHINES_YAML}")


if __name__ == "__main__":
    main()
