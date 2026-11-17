# experiments/

Experiment definitions (what to run), one folder per backend for now.

- `wayline/generate.py` — list of `convert()` calls. Writes templates (SAGA runs in
  the cluster) and offline bridge predictions to `results/wayline/`.
  `uv run python experiments/wayline/generate.py`
- `slurm/runs/` — Slurm run files (instance path + settings).
  `uv run saga-slurm experiments/slurm/runs/<x>.json`

An experiment is `graphs/<instance>.json` x scheduler x backend config; SAGA's
input comes from those plus `testbed/machines.yaml` (docs/formats.md).
