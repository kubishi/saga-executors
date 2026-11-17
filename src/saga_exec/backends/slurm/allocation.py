"""One sbatch for the whole schedule, driver.py runs the tasks.

This file runs before the job: it writes plan.json and job.sbatch into the run
folder, submits job.sbatch once, waits for the job, and reads driver.py's
results.json. driver.py runs inside the job and starts each task as an srun step.
"""

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional

from saga import ScheduledTask

from saga_exec.backends.slurm.slurm_common import NODES, RUNS_DIR, THREADS_PER_CORE, wait_for_jobs

DRIVER = Path(__file__).resolve().parent / "driver.py"
DRIVER_NODE = "cse-lambda1"          # where driver.py runs (has the run folder and venv)
DRIVER_MEM_MB = 512                  # extra memory reserved on each node for the driver step
MAX_CORES_PER_NODE = 14              # Slurm's share of each node (slurm.conf CpuSpecList)


def mem_mb(value: str) -> int:
    """'1G' -> 1024, '512M' -> 512, '2048' -> 2048 (Slurm's default unit is MB)."""
    units = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    suffix = value[-1].upper()
    if suffix in units:
        return int(float(value[:-1]) * units[suffix] + 0.999)
    return int(float(value))


def allocation_files(run_id: str, run_dir: Path, tasks: Dict[str, ScheduledTask],
                     deps: Dict[str, List[str]], transfers: Dict[str, Dict[str, float]],
                     config: SimpleNamespace) -> tuple:
    """Build plan.json (for driver.py) and job.sbatch (the one submission)."""
    plan = {
        "run_id": run_id,
        "driver_node": DRIVER_NODE,
        "tasks": [
            {
                "name": name, "node": t.node.split(":")[0],             # Slurm wants the machine, not saga's core
                "duration": (t.end - t.start) * config.time_scale,
                "predicted_start": t.start * config.time_scale,
                "cpus": config.cpus * THREADS_PER_CORE, "mem": config.mem,   # Slurm CPUs = threads
                "waits_for": deps[name],
                "transfer_from": transfers[name],                            # simulated data transfer
            }
            for name, t in tasks.items()
        ],
    }

    # Reserve enough room on each node that a task never waits for space inside the
    # reservation: room for every task assigned there, capped at Slurm's share.
    per_node: Dict[str, int] = {}
    for t in tasks.values():
        machine = t.node.split(":")[0]
        per_node[machine] = per_node.get(machine, 0) + 1
    nodes = [n for n in NODES if n in per_node or n == DRIVER_NODE]   # always include the driver's node
    slots = max(1, min(max(per_node.values()), MAX_CORES_PER_NODE // config.cpus))

    script = "\n".join([
        "#!/bin/bash",
        f"#SBATCH --job-name=saga-{run_id}",
        f"#SBATCH --nodes={len(nodes)}",
        f"#SBATCH --nodelist={','.join(nodes)}",
        f"#SBATCH --ntasks-per-node={slots}",
        f"#SBATCH --cpus-per-task={config.cpus * THREADS_PER_CORE}",
        f"#SBATCH --mem={slots * mem_mb(config.mem) + DRIVER_MEM_MB}M",
        "#SBATCH --chdir=/tmp",
        "#SBATCH --output=/dev/null",
        "",
        "# Slurm runs this batch script on the first node in its own (alphabetical) order,",
        "# which is cse-isha, where the run folder and venv don't exist. So the batch script",
        f"# only starts driver.py as a step on {DRIVER_NODE}; the driver then starts each task",
        "# as its own step. --overlap: the driver shares CPUs instead of taking a task's slot.",
        f"srun --overlap --nodes=1 --ntasks=1 --nodelist={DRIVER_NODE} --mem={DRIVER_MEM_MB}M \\",
        f"     bash -c 'exec > {run_dir}/driver.log 2>&1; exec {sys.executable} {DRIVER} {run_dir}/plan.json'",
        "",
    ])
    return plan, script


def run_allocation(run_id: str, tasks: Dict[str, ScheduledTask], deps: Dict[str, List[str]],
                   transfers: Dict[str, Dict[str, float]], config: SimpleNamespace) -> Optional[Dict[str, dict]]:
    """Submit job.sbatch once, wait for it, read driver.py's results.json."""
    run_dir = RUNS_DIR / run_id
    plan, script = allocation_files(run_id, run_dir, tasks, deps, transfers, config)

    if config.dry_run:
        print(f"# job.sbatch (dry run, nothing submitted; would be written to {run_dir}/):")
        print(script)
        return None

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "plan.json").write_text(json.dumps(plan, indent=2))
    job_path = run_dir / "job.sbatch"
    job_path.write_text(script)

    job_id = None
    try:
        result = subprocess.run(["sbatch", "--parsable", str(job_path)], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"sbatch failed: {result.stderr.strip()}")
        job_id = result.stdout.strip().split(";")[0]
        print(f"  submitted the whole schedule as job {job_id} ({job_path})")
        print(f"  driver log: {run_dir}/driver.log")
        print("Waiting for the job (Ctrl+C cancels it)...")
        final = wait_for_jobs({"job": job_id}, config.poll)
    except (KeyboardInterrupt, Exception) as e:
        if job_id:
            subprocess.run(["scancel", job_id])
            print(f"\nCancelled job {job_id}")
        sys.exit(str(e) or "Interrupted")

    results_path = run_dir / "results.json"
    if not results_path.exists():
        log_path = run_dir / "driver.log"
        tail = log_path.read_text()[-2000:] if log_path.exists() else "(no driver.log)"
        sys.exit(f"Job {job_id} ended {final['job'].get('JobState')} without results.json:\n{tail}")
    results = json.loads(results_path.read_text())["tasks"]

    return {
        name: {
            "node": results.get(name, {}).get("node", t.node),
            "state": results.get(name, {}).get("state", "MISSING"),
            "job_id": job_id,
            "start": results.get(name, {}).get("start"),           # exact, from the task itself
            "end": results.get(name, {}).get("end"),
            "core": results.get(name, {}).get("core"),             # physical core Slurm gave it
        }
        for name, t in tasks.items()
    }
