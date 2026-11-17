"""Slurm backend (owner: Sebastian Lange).

Imported as-is from github.com/MasterSebas04/saga-slurm (executor/) at
e4f6de2b5546fe54060c4f19bfc3d417fcdd1312; only imports, usage strings and TODO
comments were changed. See README.md "Provenance" and testbed/slurm/README.md.

  saga_slurm.py      entry point: run file -> SAGA schedule -> one sbatch -> compare
  allocation.py      writes plan.json + job.sbatch, submits, waits, reads results.json
  driver.py          runs inside the job (stdlib only, run by path), one srun step per task
  per_task.py        alternative: one sbatch per task (not connected)
  slurm_common.py    node list, log dir (~/slurm-logs/saga), scontrol polling
  profiling_test.py  rough per-machine speed test (zlib), see saga_exec.calibrate

Run (on cse-lambda1):  uv run saga-slurm experiments/slurm/runs/diamond.json
Dry run (anywhere):    set "dry_run": true in the run file.
"""
