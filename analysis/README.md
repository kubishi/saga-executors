# analysis/

Cross-backend analysis (notebooks/scripts comparing predicted vs. actual across
Wayline and Slurm) goes here. Empty for now.

Existing per-backend analysis lives with the backends:
`saga_exec.backends.wayline.analyze` (actual.json + Gantt) and the comparison at
the end of `saga_exec.backends.slurm.saga_slurm.main` (summary.json + Gantt).
Both write actual.json v2 (`saga_exec.actual`, docs/formats.md section 3) and
compare.json (`saga_exec.compare.side_by_side`): SAGA vs actual per task, per edge
and for the makespan, the common input for cross-backend analysis.
