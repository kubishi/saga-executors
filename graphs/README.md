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

- Units: both backends now map costs to seconds with `sec_per_cost` and sizes to
  bytes with `bytes_per_size` before scheduling. Sebastian's graphs were written
  for `time_scale` and an abstract `bandwidth`; his converted run files set
  `sec_per_cost` = old `time_scale` (and, for transfer_test, `bytes_per_size` so the
  old transfer times are kept at the measured bandwidth).
- Sebastian's task names are upper case (`A`, `T1`), which are **not** valid
  Kubernetes names, so the Wayline backend rejects them as-is.
- The Slurm run files (`experiments/slurm/runs/`) reference `slurm-*.json` via `"instance"`.
