"""One sbatch per task, Slurm enforces the dependencies.

NOT CONNECTED: saga_slurm.py doesn't import or call this file for now. It's kept
here so it can be connected again later. It still takes the LEGACY inputs
(SAGA ScheduledTasks on core-nodes, time_scale, strict_order deps); adapt it to
saga_slurm.build_plan() (machine, duration, waits_for, start_after) first.

Each job is pinned to its scheduled node and uses --dependency=afterok on its
parents. A task logs to ~/slurm-logs/saga/<run_id>/<task>-<jobid>.out on the node
that ran it, and its start/end times come from Slurm in whole seconds.
"""

import subprocess
import sys
from datetime import datetime
from types import SimpleNamespace
from typing import Dict, List, Optional

from saga import ScheduledTask

from saga_exec.backends.slurm.slurm_common import THREADS_PER_CORE, wait_for_jobs


def submission_order(tasks: Dict[str, ScheduledTask], deps: Dict[str, List[str]]) -> List[str]:
    """Every task after all the tasks it waits for, so their job IDs exist at submit time.

    Uses `deps` rather than the task graph alone: with strict_order a task can also wait
    for an earlier task on its node that comes later in the task graph's own order.
    Among tasks that are ready, the one saga starts earliest goes first.
    """
    remaining = {name: set(waits_for) for name, waits_for in deps.items()}
    order: List[str] = []
    while remaining:
        ready = [name for name, waits_for in remaining.items() if not waits_for]
        if not ready:
            raise RuntimeError(f"Circular dependencies between tasks: {sorted(remaining)}")
        name = min(ready, key=lambda n: (tasks[n].start, n))
        order.append(name)
        del remaining[name]
        for waits_for in remaining.values():
            waits_for.discard(name)
    return order


def sbatch_command(run_id: str, task: ScheduledTask, parent_ids: List[str], config: SimpleNamespace) -> List[str]:
    duration = (task.end - task.start) * config.time_scale
    cmd = [
        "sbatch", "--parsable",
        f"--job-name=saga-{run_id}-{task.name}",
        f"--nodelist={task.node}",
        f"--cpus-per-task={config.cpus * THREADS_PER_CORE}",
        f"--mem={config.mem}",
        "--chdir=/tmp",                      # exists on every node (home folders aren't shared)
        "--output=/dev/null",                # the task writes its own log, below
        "--kill-on-invalid-dep=yes",         # if a parent fails, cancel instead of pending forever
    ]
    if parent_ids:
        cmd.append("--dependency=afterok:" + ":".join(parent_ids))
    # Slurm won't create missing folders for --output, and home folders differ per node,
    # so the job creates its run folder on whichever node it lands on and logs there.
    log_dir = f"$HOME/slurm-logs/saga/{run_id}"
    cmd.append(
        f"--wrap=mkdir -p {log_dir}; exec > {log_dir}/{task.name}-$SLURM_JOB_ID.out 2>&1; "
        f"echo start $(date +%s.%N) $(hostname); sleep {duration:.3f}; echo end $(date +%s.%N)"
    )
    return cmd


def parse_time(value: Optional[str]) -> Optional[float]:
    if not value or value in ("Unknown", "None"):
        return None
    return datetime.fromisoformat(value).timestamp()


def run_per_task(run_id: str, tasks: Dict[str, ScheduledTask], deps: Dict[str, List[str]],
                 config: SimpleNamespace) -> Optional[Dict[str, dict]]:
    """Submit and wait. Returns {task: {node, state, job_id, start, end}} with epoch times."""
    order = submission_order(tasks, deps)
    if config.dry_run:
        print("# sbatch commands (dry run, nothing submitted):")
        for name in order:
            parents = [f"$JOB_{d}" for d in deps[name]]
            cmd = sbatch_command(run_id, tasks[name], parents, config)
            cmd[-1] = f"'{cmd[-1]}'"
            print(f"JOB_{name}=$({' '.join(cmd)})")
        return None

    job_ids: Dict[str, str] = {}
    try:
        for name in order:
            parents = [job_ids[d] for d in deps[name]]
            cmd = sbatch_command(run_id, tasks[name], parents, config)
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"sbatch failed for task {name}: {result.stderr.strip()}")
            job_ids[name] = result.stdout.strip().split(";")[0]
            print(f"  submitted {name:<10} job {job_ids[name]} on {tasks[name].node}")
        print("Waiting for jobs (Ctrl+C cancels them)...")
        final = wait_for_jobs(job_ids, config.poll)
    except (KeyboardInterrupt, Exception) as e:   # never leave submitted jobs behind
        if job_ids:
            subprocess.run(["scancel", *job_ids.values()])
            print(f"\nCancelled jobs {', '.join(job_ids.values())}")
        sys.exit(str(e) or "Interrupted")

    return {
        name: {
            "node": final[name].get("NodeList", tasks[name].node),
            "state": final[name].get("JobState", "?"),
            "job_id": job_ids[name],
            "start": parse_time(final[name].get("StartTime")),   # whole seconds
            "end": parse_time(final[name].get("EndTime")),
        }
        for name in order
    }
