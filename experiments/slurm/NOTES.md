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

**E3, WfCommons workflows** (`e3/`, round 2, 2026-11-18; 48 run files x 3 reps,
all ok; montage / epigenomics / 1000genome, 41-178 tasks; mean task 0.5 / 2 / 10 s;
CCR lo / hi; HEFT + CPOP). `analysis/e3_analyze.py --replan` -> `results/e3/`.
Round 1 (`results/e3/runs-round1-block64MiB.csv`) had a ~0.2 s per-task payload
artifact and is kept only for comparison.

Mean |makespan error| by model variant (a base, b +links, c +launch, d +stagger,
e +serial I/O); SAGA always under-predicts:

| granularity, CCR | a | b | c | d | e |
|---|---|---|---|---|---|
| 0.5 s, hi | 68% | 65% | 57% | 56% | 41% |
| 0.5 s, lo | 40% | 40% | 25% | 23% | 17% |
| 2 s, hi | 56% | 51% | 48% | 48% | 32% |
| 2 s, lo | 31% | 31% | 27% | 26% | 21% |
| 10 s, lo | 19% | 18% | 17% | 17% | 14% |

- The launch-overhead term (c - b) is worth ~15 pp at 0.5 s tasks, ~4 pp at 2 s
  and ~1 pp at 10 s, about the same for small and large workflows: what matters
  is task granularity, not workflow size.
- Measured link speeds (b - a) barely matter; serial reads/writes (e) matter at
  high CCR.
- The largest remaining error is network contention, which no variant models:
  e.g. genome-s, 10 s tasks, low CCR: 70 s actual vs 41 s predicted; 14
  concurrent pulls over the single lambda1-isha link at ~98 MB/s aggregate
  (8-124 MB/s each), where SAGA gives each transfer the full link. Worst for
  wide fan-in/fan-out workflows (genome, montage-l).
- Relative order of the HEFT and CPOP schedules (predicted vs measured makespan,
  ties within 1%), base model, 24 cells: 14 agree; 8 are predicted ties whose
  measured makespans differ (some by noise-level amounts with 3 reps); 2 are
  reversals: epigenomics-l 2 s lo (predicted HEFT 3% faster, measured 3% slower)
  and genome-l 0.5 s hi (predicted HEFT 8% faster, measured CPOP 28% faster,
  a contention case). Other variants are similar (11-14 of 24 agree).
