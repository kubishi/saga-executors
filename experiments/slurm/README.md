# Slurm experiments

`runs/` holds Sebastian's run files, imported from github.com/MasterSebas04/saga-slurm
`runs/` @ e4f6de2 and converted to the shared format: an `"instance"` in `graphs/`
plus execution settings (`_note` keeps the original settings). The format is
`RUN_DEFAULTS` in `src/saga_exec/backends/slurm/saga_slurm.py` (and
`testbed/slurm/README.md`, "Usage"). SAGA runs through Wayline's bridge.

```bash
# on cse-lambda1 (submits to the Slurm cluster!)
uv run saga-slurm experiments/slurm/runs/diamond.json
# offline: add "dry_run": true to a copy of a run file; prints job.sbatch only
```

`runs/tests/` are the four random-shape graphs behind
`results/examples/slurm/test_*.csv` (made with the legacy model).
Results are written to `~/slurm-logs/saga/<run_id>/` (unchanged from the original).
