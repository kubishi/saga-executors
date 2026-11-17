# results/

Run outputs go here and are **gitignored** (only this README, `.gitkeep` and
`examples/` are tracked).

Suggested layout: `results/<backend>/<graph>/<run name>/...`
(the Wayline backend writes to `results/wayline/` by default).

`examples/` holds small, curated outputs imported from the students' original
repos so the formats can be inspected without a cluster:

- `examples/wayline/smoke-diamond/` — Nick's smoke run (HEFT and CPOP, C=8, sleep mode;
  LEGACY model: SAGA offline on core-nodes, reproduced by `saga_exec.legacy.wayline_convert`):
  `prediction.json`, `config.json`, `instance.json`, `template.yml` and one run's
  `status.json`, `logs.txt`, `actual.json`, `gantt.png`; plus `run-log.csv`.
- `examples/slurm/` — Sebastian's `test_runs.csv` / `test_summary.csv`
  (3 repetitions each of chain/diamond/fork/branching/transfer_test, 2026-11-05).

Note: the Slurm CSVs were produced at saga-slurm@3caa937 (2026-11-05), **before**
e4f6de2 changed the SAGA network to one node per core. Re-running the same run
files with the current code predicts shorter makespans for the 2-machine graphs
(e.g. tests/diamond 6.15 s vs 8.239 s in the CSV); chain (1 machine) is unchanged.
