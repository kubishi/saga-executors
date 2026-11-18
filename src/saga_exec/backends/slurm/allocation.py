"""One sbatch for the whole schedule, driver.py runs the tasks.

This file runs before the job: it writes plan.json, task.pyz (the shared task
program, task/*.py, as one zipapp) and job.sbatch into the run folder, submits
job.sbatch once, waits for the job, and reads driver.py's results.json.
driver.py runs inside the job and starts each task as an srun step. With the
"direct" transport (default) it sbcasts task.pyz and starts a data server per
machine; with "shared_fs" task.pyz and the data live in the shared work dir.
"""

import getpass
import json
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Dict

from saga_exec.backends.slurm.slurm_common import RUNS_DIR, THREADS_PER_CORE, wait_for_jobs
from saga_exec.testbed import Testbed

DRIVER = Path(__file__).resolve().parent / "driver.py"
TASK_DIR = Path(__file__).resolve().parents[4] / "task"    # the shared task program (repo root)
TASK_FILES = ("task.py", "fidelity_core.py", "transport_direct.py", "transport_shared_fs.py", "calibrate_cpu.py")
DRIVER_NODE = "cse-lambda1"          # where driver.py runs (has the run folder and venv)
DRIVER_MEM_MB = 512                  # extra memory reserved on each node for the driver step
SERVER_MEM_MB = 512                  # per node for its data server (direct transport; sendfile, no buffering)


def mem_mb(value: str) -> int:
    """'1G' -> 1024, '512M' -> 512, '2048' -> 2048 (Slurm's default unit is MB)."""
    units = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    suffix = value[-1].upper()
    if suffix in units:
        return int(float(value[:-1]) * units[suffix] + 0.999)
    return int(float(value))


def build_task_pyz(dest: Path) -> Path:
    """task/*.py (stdlib only) as one zipapp: `python task.pyz [task|serve|calibrate]`."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        for name in TASK_FILES:
            z.write(TASK_DIR / name, name)
        z.writestr("__main__.py", "import task\ntask.main()\n")
    return dest


def default_scratch() -> str:
    """Node-local scratch root for the direct transport, per user (same path on every machine)."""
    return f"/tmp/saga-exec-{getpass.getuser()}"


def shared_run_dir(workdir: str, run_id: str) -> str:
    """This run's folder in the shared work dir: <workdir>/<user>/<run_id>."""
    return f"{workdir.rstrip('/')}/{getpass.getuser()}/{run_id}"


def allocation_files(run_id: str, run_dir: Path, tasks: Dict[str, dict],
                     config: SimpleNamespace, testbed: Testbed) -> tuple:
    """Build plan.json (for driver.py) and job.sbatch (the one submission).

    tasks: saga_slurm.build_plan() -- per task its machine (SAGA's core's machine),
    predicted start, runtime, inputs / outputs (objects, bytes, consumer machine),
    start_after (the machine's start order, when enacted) and SAGA's predicted
    transfer seconds (reporting only; transfers are real).
    """
    # Reserve enough room on each node that a task never waits for space inside the
    # reservation: room for every task assigned there, capped at Slurm's share
    # (machines.yaml partitions.slurm.saga_nodes_per_machine cores).
    max_cores = min(testbed.partition("slurm").cores.values())
    per_node: Dict[str, int] = {}
    for t in tasks.values():
        per_node[t["machine"]] = per_node.get(t["machine"], 0) + 1
    nodes = [n for n in testbed.machines if n in per_node or n == DRIVER_NODE]   # always include the driver's node
    slots = max(1, min(max(per_node.values()), max_cores // config.cpus))

    plan = {
        "run_id": run_id,
        "driver_node": DRIVER_NODE,
        "machines": nodes,
        "cpus_per_node": slots * config.cpus * THREADS_PER_CORE,   # the job's Slurm CPUs on each node
        "transport": config.transport,
        "shared_dir": shared_run_dir(config.workdir, run_id) if config.transport == "shared_fs" else None,
        "latency_wait": float(config.latency_wait),
        "keep_data": bool(config.keep_data),
        "clock_probe": bool(getattr(config, "clock_probe", True)),   # measurement only (driver.py)
        "mode": config.mode,
        "hash_rate": config.hash_rate,
        "speed": config.machine_speed,
        "task_python": config.task_python,
        "scratch": config.scratch or default_scratch(),    # direct only
        "pyz": str(run_dir / "task.pyz"),
        "server": {"mem": f"{SERVER_MEM_MB}M", "port": int(config.server_port),   # direct only
                   "bind": dict(config.server_bind)},
        "tasks": [
            {
                "name": name, "node": t["machine"],
                "seconds": t["seconds"],                                      # FID_SECONDS (speed 1.0)
                "duration": t["duration"],                                    # planned on this machine
                "predicted_start": t["start"],
                "cpus": config.cpus * THREADS_PER_CORE, "mem": config.mem,   # Slurm CPUs = threads
                "inputs": t["inputs"],
                "outputs": t["outputs"],
                "waits_for": t["waits_for"],
                "start_after": t["start_after"],
                "predicted_transfer": t["predicted_transfer"],               # SAGA's assumption, reporting only
            }
            for name, t in tasks.items()
        ],
    }

    server_mb = SERVER_MEM_MB if config.transport == "direct" else 0
    script = "\n".join([
        "#!/bin/bash",
        f"#SBATCH --job-name=saga-{run_id}",
        f"#SBATCH --nodes={len(nodes)}",
        f"#SBATCH --nodelist={','.join(nodes)}",
        f"#SBATCH --ntasks-per-node={slots}",
        f"#SBATCH --cpus-per-task={config.cpus * THREADS_PER_CORE}",
        f"#SBATCH --mem={slots * mem_mb(config.mem) + DRIVER_MEM_MB + server_mb}M",
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


def run_allocation(run_id: str, tasks: Dict[str, dict], config: SimpleNamespace,
                   testbed: Testbed) -> Dict[str, dict]:
    """Submit job.sbatch once, wait for it, read driver.py's results.json."""
    run_dir = RUNS_DIR / run_id
    plan, script = allocation_files(run_id, run_dir, tasks, config, testbed)

    run_dir.mkdir(parents=True, exist_ok=True)
    build_task_pyz(Path(plan["pyz"]))
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
    return measured_from_results(json.loads(results_path.read_text())["tasks"], tasks, job_id)


def measured_from_results(results: Dict[str, dict], tasks: Dict[str, dict], job_id) -> Dict[str, dict]:
    """driver.py's results.json "tasks" -> per task what saga_slurm.summarize() reads."""
    measured = {}
    for name, t in tasks.items():
        r = results.get(name, {})
        timings = r.get("timings") or {}
        measured[name] = {
            "node": r.get("node", t["machine"]),
            "state": r.get("state", "MISSING"),
            "job_id": job_id,
            "start": r.get("start"),               # taskStartUnix, from the task itself
            "end": r.get("end"),                   # closeUnix
            "core": r.get("core"),                 # physical core Slurm gave it
            "inputs_ready": r.get("inputs_ready"), # when its last input became available
            "input_read": timings.get("inputReadSeconds"),
            "compute": timings.get("computeSeconds"),
            "handoff": timings.get("handoffSeconds"),
            "timings": timings,
            "launch": r.get("launch"),             # driver's clock: step launched
            "finish": r.get("finish"),             # driver's clock: step exit noticed
        }
    return measured
