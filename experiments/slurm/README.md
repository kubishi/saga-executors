# Slurm experiments

`runs/` holds Sebastian's run files (graph + execution settings in one JSON),
imported from github.com/MasterSebas04/saga-slurm `runs/` @ e4f6de2. The format
is documented in `testbed/slurm/README.md` ("Usage") and in `RUN_DEFAULTS` in
`src/saga_exec/backends/slurm/saga_slurm.py`.

```bash
# on cse-lambda1 (submits to the Slurm cluster!)
uv run saga-slurm experiments/slurm/runs/diamond.json
# offline: add "dry_run": true to a copy of a run file; prints job.sbatch only
```

`runs/tests/` are the four random-shape graphs behind
`results/examples/slurm/test_*.csv`. Run files with explicit `tasks` were also
converted to shared instances in `graphs/slurm-*.json` (see `graphs/README.md`).
Results are written to `~/slurm-logs/saga/<run_id>/` (unchanged from the original).
