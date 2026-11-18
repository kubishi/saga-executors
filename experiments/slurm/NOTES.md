# Slurm preliminary results (2026-11-18, transport `direct`)

Raw runs: `~/slurm-logs/saga/` on cse-lambda1 (each with prediction.json,
actual.json, compare.json). Analysis scripts: `analysis/slurm_e0_overhead.py`,
`analysis/slurm_e2_links.py`.

**E0, overhead** (`e0/`, sleep mode, no data, 3 reps of diamond HEFT/CPOP and
chain). Mapping and order enacted correctly. Work durations match SAGA within
0.2 ms. Every dependency adds ~100-140 ms (Slurm step launch ~90 ms plus driver
noticing the producer's exit), so makespan error = that cost times the number of
dependencies on the critical path (+0.23 s diamond, +0.33 s chain).

**E1, compute** (`e1/`, cpu mode, 1/7/14/28 independent 3 s tasks, 3 reps).
Calibrated speeds: lambda1 1.0, isha 0.7734. Work/SAGA ratio 1.01 for a single
task, ~1.045 with 7-14 concurrent on lambda1 (contention SAGA doesn't model),
1.01 on isha. Step launches are staggered by up to ~110 ms (14 tasks) / ~220 ms
(28 tasks).

**E2, links** (2-task chains, 0-500 MB, 3 reps per case). Data movement
(producer write + consumer read) = ~0-3 ms + size / bandwidth:

| link | bandwidth |
|---|---|
| lambda1 -> isha | ~110 MB/s (iperf3: 117) |
| isha -> lambda1 | ~106 MB/s (iperf3: 117.6) |
| same machine, lambda1 (self = intra) | ~1.05 GB/s |
| same machine, isha (self = intra) | ~0.67-0.76 GB/s |

Separately, every dependency costs ~100-160 ms of task launch, independent of
size and link, depending on the consumer's machine (lower on lambda1, where the
driver runs). So the missing model term on Slurm is a per-task launch overhead,
not a per-transfer latency.

**Hypothesis for E3** (PI): for large, complex workflows modelling this
overhead adds complexity for little benefit. Expected to depend on task
granularity (runtime vs ~110 ms), not just workflow size.

**E3** (built, not run yet): WfCommons workflows x granularity x CCR x HEFT/CPOP, with
offline model variants (links, launch overhead, stagger, serialized I/O); see `e3/README.md`.
