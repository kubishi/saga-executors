# Shared formats — DRAFT proposal

Status: **draft for discussion** (Nick, Sebastian, Jared). Nothing here is
enforced yet; both backends still use their own formats. Thin draft code that
follows this proposal: `saga_exec.instance`, `saga_exec.network`,
`saga_exec.schedule`, `saga_exec.compare`, `saga_exec.backends.base`.

Shared model (both backends agree on this already): each machine with C usable
cores becomes C SAGA nodes; links between cores of one machine are (effectively)
free; links across machines use the measured bandwidth; every task uses one core;
SAGA's placement fixes the **machine**, while the actual core is chosen by the
OS / Slurm.

## 1. Instance (`graphs/*.json`)

Proposal: adopt Nick's format as-is.

```json
{
  "name": "smoke-diamond",
  "source": "hand-written, for end-to-end testing",
  "tasks": {"a": 2.0, "b": 4.0},
  "edges": [["a", "b", 10000000]]
}
```

- `tasks`: task -> cost, in *cost units*; `edges`: `[producer, consumer, size]`, in *size units*.
- Backends map units to seconds (at speed 1.0) and bytes with a backend config
  (`sec_per_cost`, `bytes_per_size`).
- Graph `name` must be unique across datasets.
- Proposed additions (optional): `"units": {"cost": "s", "size": "B"}` when the
  instance is already in physical units.

Sebastian's run files (`experiments/slurm/runs/*.json`) contain an instance
(`tasks` + `dependencies`, or a random `graph` spec) *and* execution settings.
Their graphs have been converted to `graphs/slurm-*.json`.

## 2. Testbed (`testbed/machines.yaml`)

See the file. Holds machines (speed, cores, roles), per-system core partitions
(Slurm cores 4–17, k8s cores 18–31 intended, OS 0–3), and the measured network
(117000000 / 117625000 B/s, iperf3 2026-10-27), plus local bandwidth. Calibration
(`saga_exec.calibrate`) should write speeds and bandwidths here.

## 3. Prediction (`prediction.json`) and actual (`actual.json`)

Proposal: Nick's shape, which both files share, with a `meta` block added.

```json
{
  "makespan": 7.000015,
  "meta": {
    "backend": "wayline",
    "instance": "smoke-diamond",
    "scheduler": "heft",
    "saga_version": "2.1.0",
    "config": "heft-c8-sleep-a5faff"
  },
  "tasks": {
    "a": {"node": "cse-isha-c6", "machine": "cse-isha", "start": 0.0, "end": 2.0}
  }
}
```

`actual.json` adds run identity and optional per-phase times:

```json
{
  "run": "smoke-diamond-heft-c8-sleep-a5faff-run-hzjd6",
  "makespan": 9.21,
  "t0": "2026-11-05T22:58:59.819361448Z",
  "tasks": {
    "a": {"node": "cse-isha", "machine": "cse-isha", "start": 0.0, "end": 2.196,
          "input_read": 0, "compute": 2.031, "handoff": 0.164,
          "state": "COMPLETED"}
  }
}
```

- Times in **seconds**, relative to the first task's start (both backends already do this).
- `node`: SAGA's core in a prediction; in an actual, the most specific location
  observed (Wayline: machine only; Slurm: `"cse-isha core5"`-style physical core).
- `input_read` / `compute` / `handoff`: optional; Wayline gets them from the SDK.
  Slurm could report `compute` (sleep), and `input_read` = simulated transfer wait.
- Nick's actual.json also has `makespan_from_odag_start` (backend-specific; keep under `meta`?).
- Per-run folder (proposal, Nick's layout): `results/<backend>/<instance>/<config name>/`
  with `instance.json`, `config.json`, `prediction.json`, backend artifacts
  (`template.yml` / `plan.json` + `job.sbatch`), and `runs/<run id>/actual.json` + `gantt.png`.

## 4. Open questions — every current divergence between the backends

| # | Topic | Wayline backend (Nick) | Slurm backend (Sebastian) |
|---|---|---|---|
| 1 | SAGA core node names | `cse-isha-c6` (`<machine>-c<i>`) | `cse-isha:c6` (`<machine>:c<i>`) |
| 2 | Bandwidth between cores of one machine (incl. self-loops) | `LOCAL_BW = 1e12` ("finite on purpose: SAGA divides by it") | `float("inf")` (works with anrg-saga 2.1.0: tests pass) |
| 3 | Bandwidth across machines | measured, bytes/s, per direction; SAGA gets `min` of both = 117000000 | one run-file `bandwidth` for all cross links, default `1e9`, in abstract size units per SAGA time unit |
| 4 | Scaling to real time | before scheduling: `sec_per_cost`, `bytes_per_size` (SAGA works in s and B) | after scheduling: `time_scale` multiplies SAGA's times and transfer times (SAGA works in abstract units; default 10, run files use 1–10) |
| 5 | Cores per machine seen by SAGA | `Config.C` (8 in smoke runs) | run file `cores` (default 14 = Slurm's share) |
| 6 | Task CPU unit | `cpu: "1"` = 1 hardware thread | `cpus: 1` whole core = 2 Slurm CPUs (threads) |
| 7 | Task memory | computed: 512 MiB + 2x bytes held (or `Config.memory`) | fixed `mem` (default 1G) |
| 8 | Core partition enforcement | kubelet reservations (12 allocatable CPUs on lambda1), no CPU pinning to cores 18–31 | `CpuSpecList` + cgroups pin to cores 4–17 |
| 9 | Instance format | `graphs/*.json` (`tasks`, `edges` with sizes, `name`) | run file: `tasks` + `dependencies` (size optional) or random `graph` shape, mixed with settings |
| 10 | Task names | must be Kubernetes DNS labels (lower case) | anything (`A`, `T1`, `0`) |
| 11 | Scheduler selection | `heft`, `cpop` only | any `saga.schedulers` class by short name |
| 12 | Speeds | `Config.speed` must list exactly both machines; sleep mode emulates speed, cpu mode uses real speed | run file `nodes`, missing -> 1.0; always emulated (sleep) |
| 13 | What a task does | `sleep` (FID_SECONDS / speed) or `cpu` (sha256 loop, `hash_rate`), in a container with the Wayline SDK | `bash sleep` for SAGA's predicted duration on that core |
| 14 | Data transfers | real: random (incompressible) bytes through Wayline's data agent | simulated: driver delays a child by size / bandwidth x time_scale after the parent finishes; no data moves |
| 15 | Order enforcement | `schedulerConfig.nodeOrder` per **machine** (all cores merged, by predicted start) + `enactOrder: order` | `strict_order`: extra dependency on the previous task on the same **SAGA core**; driver launches ready tasks by predicted start |
| 16 | Placement | `constraints.nodeNames: [machine]` | `srun --nodelist=machine` (+ `--exact`, `--cpu-bind=none`) |
| 17 | Measured start / end | SDK `taskStartTime` / `taskCloseTime` (end includes output handoff) | task's own `date +%s.%N` around `sleep` (excludes srun launch/teardown) |
| 18 | Clocks | per-node clocks in SDK timestamps | per-node clocks for start/end; driver's (lambda) clock for transfer waits — both assume synced clocks |
| 19 | Measured makespan | last close − first start (also `makespan_from_odag_start`) | max end − min start |
| 20 | Actual `node` | machine only | host + physical core id + Linux CPUs |
| 21 | Phase breakdown | `input_read`, `compute`, `handoff` | `delay` (start − max(parent end + transfer)), `transfer_from` |
| 22 | Output files | `prediction.json`, `actual.json`, `config.json`, `instance.json`, `template.yml`, `status.json`, `logs.txt`, `gantt.png`, `run-log.csv` | `summary.json` (rows with predicted and actual), `plan.json`, `job.sbatch`, `results.json`, `driver.log`, `<task>.out`, `gantt.png` |
| 23 | Output location | results folder next to the code (now `results/wayline/`) | `~/slurm-logs/saga/<run_id>/` on lambda (outside the repo) |
| 24 | Run identity | config hash in name (`heft-c8-sleep-a5faff`) + generated ODAG name | timestamp `run_id` |
| 25 | Repetitions | `run.py --reps N --shuffle SEED`, `run-log.csv` | one run per invocation; `results/*.csv` assembled separately (script not in repo) |
| 26 | Reproducible scheduling | `PYTHONHASHSEED=0` required (SAGA tie-breaking follows str-hash order) | not set — placements of tied tasks can differ between invocations |
| 27 | Speed calibration | `hash_rate` (sha256/s), measured by hand | `profiling_test.py` (zlib, 1 core and all CPUs) |
| 28 | Gantt rows | predicted: SAGA core; actual: first-free "slot" per machine | predicted: SAGA core; actual: physical core Slurm gave |
| 29 | Python on the task side | `python:3.11-slim` image | none (bash); driver uses the submitting venv's Python |

Further questions not tied to one backend:

- Should prediction times be stored before or after scaling (both store seconds today)?
- One `bandwidth` for SAGA (min of directions) or directed links (SAGA networks are undirected)?
- How should the k8s and Slurm partitions be sized so both backends can use the same C?
- Where do calibration results go (machines.yaml vs per-run config)?
