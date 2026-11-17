"""Slurm backend (owner: Sebastian Lange).

Imported from github.com/MasterSebas04/saga-slurm (executor/) at e4f6de2. SAGA
scheduling now goes through the shared path (saga_exec.scheduling, i.e. Wayline's
bridge); this backend only enacts SAGA's mapping and order and measures:

  saga_slurm.py      entry point: run file -> bridge prediction -> plan -> one sbatch -> compare
  allocation.py      writes plan.json + task.pyz + job.sbatch, submits, waits, reads results.json
  driver.py          runs inside the job (stdlib only, run by path), one srun step per task
                     running the shared task program (task/), real work + real data movement
                     ("transport": direct = node-local scratch + per-machine data servers,
                     shared_fs = shared work dir; docs/data-movement.md)
  per_task.py        alternative: one sbatch per task (not connected; legacy inputs)
  slurm_common.py    node list (machines.yaml), log dir (~/slurm-logs/saga), scontrol polling
  profiling_test.py  rough per-machine speed test (zlib); superseded by saga_exec.calibrate.cpu

Sebastian's own problem construction (core-nodes "<m>:c<i>", run-file bandwidth,
time_scale) is kept in saga_exec.legacy.slurm_saga.

Run (on cse-lambda1):  uv run saga-slurm experiments/slurm/runs/diamond.json
Dry run (anywhere):    set "dry_run": true in the run file (still schedules with the bridge).
"""
