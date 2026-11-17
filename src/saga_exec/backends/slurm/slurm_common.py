"""Settings and job-watching code shared by the executor's modes."""

import subprocess
import time
from pathlib import Path
from typing import Dict, List

# TODO(shared): read machines from testbed/machines.yaml (saga_exec.network.load_machines).
NODES = ["cse-lambda1", "cse-isha"]
RUNS_DIR = Path.home() / "slurm-logs" / "saga"
THREADS_PER_CORE = 2                 # Slurm counts each hardware thread as a "CPU"; --cpus counts whole cores
FINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL"}


def job_info(job_ids: List[str]) -> Dict[str, Dict[str, str]]:
    """Parse `scontrol show job -o` into {job_id: {field: value}}.

    scontrol accepts only one job ID per call, so each job is queried separately.
    """
    info = {}
    for jid in job_ids:
        out = subprocess.run(
            ["scontrol", "show", "job", "-o", jid],
            capture_output=True, text=True,
        ).stdout
        for line in out.splitlines():
            fields = dict(f.split("=", 1) for f in line.split() if "=" in f)
            if "JobId" in fields:
                info[fields["JobId"]] = fields
    return info


def wait_for_jobs(job_ids: Dict[str, str], poll: float) -> Dict[str, Dict[str, str]]:
    """Poll until every job reaches a final state. Returns the last info per task.
       Checks if x/y jobs finished, where x = done, y = total. Polls every `poll` seconds.
    """
    last: Dict[str, Dict[str, str]] = {}
    while True:
        info = job_info(list(job_ids.values()))
        for task, jid in job_ids.items():
            if jid in info:
                last[task] = info[jid]
        states = {task: last.get(task, {}).get("JobState", "UNKNOWN") for task in job_ids}
        done = sum(s in FINAL_STATES for s in states.values())
        print(f"\r  {done}/{len(job_ids)} jobs finished", end="", flush=True)
        if done == len(job_ids):
            print()
            return last
        time.sleep(poll)
