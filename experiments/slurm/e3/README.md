# E3: realistic workflows on Slurm, and which model terms matter

**Hypothesis (PI).** For large, complex workflows, modelling fixed overheads
(per-task launch, latency) adds complexity for little benefit. E0-E2
(`../NOTES.md`) found that the term SAGA misses on Slurm is a per-task launch cost
of ~110-125 ms, so the benefit should depend on task granularity (runtime against
~0.1 s) more than on workflow size.

**Design.** SAGA (HEFT, CPOP) schedules of real WfCommons workflows run on Slurm
exactly as planned (mapping + per-machine order, `enact_order: order`, `cpu` mode,
`direct` transport, `mem: 2G`). Every model variant is evaluated **offline against
the same measured runs**. Execution never changes to fit a model.

## Factors

| factor | levels |
|---|---|
| workflow (all real WfInstances traces, Pegasus / chameleon-cloud) | montage (fan-in mosaic, depth 8), epigenomics (parallel pipelines, depth 9), 1000genome (wide, depth 3) |
| size | `s` ~50 tasks (58 / 41 / 52), `l` ~125-180 tasks (178 / 125 / 164) |
| granularity g (mean task runtime at speed 1.0) | 0.5 s, 2 s, 10 s |
| CCR | `lo` 0.1, `hi` 1.0 |
| scheduler | heft, cpop |

The traces' structure and **relative** runtimes and file sizes are kept. Costs are
scaled so the mean is exactly g. **CCR** = mean over edges of (bytes / 108 MB/s) /
mean task cost, where 108 MB/s is the mean measured inter-machine bandwidth (E2).
It depends only on the graph, not on the schedule. Edges are capped at 250 MB
(payloads are held in memory) and runs at 25 GB (node-local `/tmp`). A cell is
dropped when the caps keep its CCR below 0.9 × the target. That removes `hi` at
10 s, `hi` at 2 s for the large graphs, and montage-l at 10 s. Those cells would
need 50-200 GB per run. This leaves **26 graphs and 52 run files**. Task names are
`<category>-<n>` (`mproject-12`), Kubernetes DNS labels, so the graphs also work on
Wayline. `graphs.json` keeps the name mapping, the source file, the achieved
CCR, flags and stats for every cell (tasks, edges, depth, width, total bytes,
critical path).

| workflow | tasks | edges | depth | width | parallelism | CP / g |
|---|---|---|---|---|---|---|
| montage-s | 58 | 114 | 8 | 18 | 10.4 | 5.6 |
| montage-l | 178 | 444 | 8 | 108 | 22.0 | 8.1 |
| epigenomics-s | 41 | 48 | 9 | 9 | 5.1 | 8.0 |
| epigenomics-l | 125 | 153 | 9 | 30 | 18.0 | 7.0 |
| genome-s | 52 | 76 | 3 | 28 | 13.5 | 3.8 |
| genome-l | 164 | 212 | 3 | 104 | 34.2 | 4.8 |

## Model variants (`analysis/e3_model.py`)

Each run's schedule is replayed with SAGA's mapping (task -> SAGA core) and each
core's order held fixed: start = max(core predecessor's end, data arrivals). The
variants add terms cumulatively:

| | variant | terms |
|---|---|---|
| a | base | SAGA's own model (self/intra links 1e12, inter = min of the measured directions). **Reproduces SAGA's predicted start/end exactly** (all 52 E3 schedules and all 21 E0/E1 runs) |
| b | +links | measured data-path bandwidths: inter 110 MB/s lambda1->isha, 106 MB/s isha->lambda1; same machine 1.05 GB/s lambda1, 0.70 GB/s isha |
| c | +launch | + per-task launch overhead on the consumer's machine: **lambda1 108 ms, isha 126 ms** (median of consumer work start − last producer work end over 0-byte dependencies, E0 + E2; `python analysis/e3_model.py fit`) |
| d | +stagger | + launches issued one at a time, ≥ **7.7 ms** apart (E1, 14/28 simultaneous tasks) |
| e | +serial-io | (extra) the Slurm direct data path as it runs: launch after the producers' steps end, read inputs one by one, work, then generate (random bytes, **354 / 323 MB/s** lambda1 / isha, E2) and write each output, all on the core |

The question is how much each term improves the error, by granularity and CCR. If
c/d buy little at 2-10 s, the hypothesis holds. At high CCR, e (payload generation
plus serialized I/O) is expected to dominate, and link contention is not modelled
in any variant.

**Rankings.** For each cell, HEFT vs CPOP is compared as measured (mean over reps),
as predicted by each variant's replay of the two run schedules, and (`--replan`)
as predicted when SAGA itself plans under the variant. For the re-plan, b sets the
bridge's links, and c adds the mean launch overhead to every task's cost (keeping
cost/speed exact). d and e cannot be expressed in SAGA's model, so they re-plan
as c. Differences under 1% count as ties.

## How to run

```bash
uv run python experiments/slurm/e3/make_graphs.py              # (re)make graphs, run files, graphs.json
uv run python experiments/slurm/e3/run_matrix.py --validate    # offline dry run of every run file
uv run python experiments/slurm/e3/run_matrix.py --estimate --subset core --reps 3

# on cse-lambda1; submits one Slurm job at a time; resumable (rerun the same command)
uv run python experiments/slurm/e3/run_matrix.py --subset core --reps 3

uv run python analysis/e3_analyze.py --replan                  # -> results/e3/*.csv, fig_*.png
```

The runner logs each attempt (`run_file, rep, run_id, status`) to
`results/e3/runs.csv` and keeps stdout in `results/e3/logs/`. It skips
(run file, rep) pairs already marked `ok` that have an `actual.json`. The order
is shuffled with a fixed seed. `--only 'e3-genome-*'` selects run files by
pattern, and `--list` shows what is left to run.

## Time estimate

Estimate per run = max(SAGA makespan, replay e) + 8 s (bridge, job start,
sbcast, data servers, polling). Most runs take 10-40 s. The 10 s cells take
55-130 s.

| subset | run files | per rep | 3 reps |
|---|---|---|---|
| full | 52 | ~33 min | ~1.6 h |
| **core** (drops the large graphs at 10 s) | 48 | ~24 min | **~1.2 h** |

The high-CCR runs move 2.6-25 GB each. Before the first run, check free space in
`/tmp` on cse-isha (scratch is deleted at the end of each run).
