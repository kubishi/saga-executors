# analysis/

Cross-backend analysis (notebooks/scripts comparing predicted vs. actual across
Wayline and Slurm) goes here. Empty for now.

Existing per-backend analysis lives with the backends:
`saga_exec.backends.wayline.analyze` (actual.json + Gantt) and the comparison at
the end of `saga_exec.backends.slurm.saga_slurm.main` (summary.json + Gantt).
Shared draft: `saga_exec.compare`.
