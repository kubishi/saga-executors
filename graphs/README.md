# graphs/ — shared problem instances

Format: Nick's instance JSON (see `docs/formats.md`, "Instance"; loader
`saga_exec.instance.load_instance`):

```json
{"name": "...", "source": "...", "tasks": {"a": 2.0}, "edges": [["a", "b", 1.0]]}
```

| File | Origin |
|---|---|
| `smoke.json` | Nick, `eval/experiments/fidelity/graphs/smoke.json` (wayline fork `lab` @ aff6fa8) |
| `slurm-*.json` | Generated from Sebastian's run files (`experiments/slurm/runs/`, saga-slurm @ e4f6de2). Explicit `tasks` copied (missing edge sizes -> 0); random `graph` shapes materialized with his `build_task_graph` under anrg-saga 2.1.0. |

Caveats (see open questions in `docs/formats.md`):

- Units differ: Nick's sizes are bytes (with `bytes_per_size`) and costs seconds
  (with `sec_per_cost`); Sebastian's costs are "saga time units" multiplied by
  `time_scale`, and sizes are divided by the run file's `bandwidth` in the same units.
- Sebastian's task names are upper case (`A`, `T1`), which are **not** valid
  Kubernetes names, so the Wayline backend rejects them as-is.
- The backends do not read `slurm-*.json`; the Slurm backend still reads its run files.
