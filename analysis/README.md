# analysis/

Cross-backend analysis (notebooks/scripts comparing predicted vs. actual across
Wayline and Slurm) goes here. Empty for now.

Existing per-backend analysis lives with the backends:
`saga_exec.backends.wayline.analyze` (actual.json + Gantt) and the comparison at
the end of `saga_exec.backends.slurm.saga_slurm.main` (summary.json + Gantt).
Both write actual.json v2 (`saga_exec.actual`, docs/formats.md section 3) and
compare.json (`saga_exec.compare.side_by_side`): SAGA vs actual per task, per edge
and for the makespan, the common input for cross-backend analysis.

Slurm E0-E2: `slurm_e0_overhead.py`, `slurm_e2_links.py` (see experiments/slurm/NOTES.md).
E3: `e3_model.py` (replay of SAGA's fixed mapping/order under model variants a-e,
re-planning under a variant, parameter fit from the E0-E2 logs) and
`e3_analyze.py` (per-run errors, aggregates, HEFT/CPOP rank agreement, figures to
`results/e3/`); see experiments/slurm/e3/README.md.
