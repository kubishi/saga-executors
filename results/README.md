# results/

Run outputs go here and are **gitignored** (only this README, `.gitkeep` and
`examples/` are tracked).

Suggested layout: `results/<backend>/<graph>/<run name>/...`
(the Wayline backend writes to `results/wayline/` by default).

`examples/` holds small, curated outputs imported from the students' original
repos so the formats can be inspected without a cluster:

- `examples/wayline/smoke-diamond/` — Nick's smoke run (HEFT and CPOP, C=8, sleep mode):
  `prediction.json`, `config.json`, `instance.json`, `template.yml` and one run's
  `actual.json` + `gantt.png` (status.json and logs.txt omitted).
- `examples/slurm/` — Sebastian's `test_runs.csv` / `test_summary.csv`
  (3 repetitions each of chain/diamond/fork/branching/transfer_test, 2026-11-05).
