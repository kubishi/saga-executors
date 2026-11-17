# experiments/

Experiment definitions (what to run), one folder per backend for now.

- `wayline/generate.py` — Nick's list of `convert()` calls. Writes templates and
  predictions to `results/wayline/`.
  `PYTHONHASHSEED=0 uv run python experiments/wayline/generate.py`
- `slurm/runs/` — Sebastian's run files. `uv run saga-slurm experiments/slurm/runs/<x>.json`

Once formats are shared (docs/formats.md), an experiment should be
`graphs/<instance>.json` x scheduler x backend config.
