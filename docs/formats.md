# Shared formats and the one SAGA path

Status: sections 1–4 are **in use** by both backends. Section 6 lists what is
still open. Code: `saga_exec.instance`, `saga_exec.testbed`,
`saga_exec.scheduling`, `saga_exec.schedule`, `saga_exec.compare`.

## 0. Architecture: SAGA only through Wayline's bridge

SAGA scheduling happens in exactly one place: Wayline's saga-sidecar bridge
(`saga-sidecar/bridge.py`, package `wl-saga-sidecar`). Both backends, and
Wayline's controller in the cluster, hand it the same JSON request:

```
graphs/<x>.json + testbed/machines.yaml + config
   -> saga_exec.scheduling.build_request()   == the body wayline/cmd/odag-controller/saga.go POSTs
   -> bridge.schedule_request()               (PYTHONHASHSEED=0, same as the sidecar deployment)
   -> prediction.json
```

- **Wayline**: the template asks for `spec.scheduler: saga/<alg>`; the controller
  builds the request from the template (runtimeSource `manual`, so it uses our
  runtimes, not profiler data) and the `wl-network-profile` ConfigMap, calls the
  sidecar, enacts the placement (and the per-machine order with
  `enactOrder: order`) and records SAGA's prediction in `status.scheduling`
  (Wayline branch `saga-executor`). We also run the identical request through the
  bridge offline at generate time (`prediction.json` next to `template.yml`) and
  compare after the run.
- **Slurm**: the request is built and scheduled offline with the same bridge;
  `allocation.py` / `driver.py` enact the mapping and order.

`tests/data/golden/` holds requests produced by the controller's own Go code;
`tests/test_scheduling.py` checks that `build_request` is identical.

### The SAGA model (settles former divergences 1–5)

Run config `model` (`saga_exec.scheduling.ModelConfig`) chooses the model variant;
it becomes the request's `model` (= the template's `schedulerConfig.model`, which
the controller forwards). Defaults in bold:

| `model.` | Values | Meaning |
|---|---|---|
| `nodes` | **`cores`**, `capacity` | `cores`: each machine is C SAGA nodes that each run ONE task at a time (the bridge's opt-in `nodeModel: cores`). `capacity` is the bridge's default (one node per machine, capacity = free CPU); not used by our experiments, needs SAGA `feature/capacity`. |
| `cores` | **machines.yaml** `partitions.<k8s\|slurm>.saga_nodes_per_machine` (k8s 8, Slurm 14), or an int / `{machine: C}` | C, sent explicitly as `model.cores`, so SAGA's problem does not depend on the cluster's free CPU |
| `links` | **`free`**, `measured` | self (same core) and intra-machine (core to core) links: `free` = 1e12 B/s; `measured` = `machines.yaml machines.<m>.self_bandwidth / intra_bandwidth` (currently UNMEASURED placeholders, 1e9; see `saga_exec.calibrate.links`) |
| `transfer_latency` | **`0`** | fixed seconds per transfer. Only 0 is accepted: SAGA's `Network` edges have a speed but no latency, so evaluating this variant needs a SAGA Network-edge extension (edge latency, comm time = latency + size / speed) and the matching bridge field. The config slot exists so runs can be labelled with it once that lands. |

- **Core names**: `<machine>/c<i>` everywhere (bridge, prediction.json `node`,
  Wayline status `externalAssignments[].core`, Slurm plan `core`). `/` cannot occur in
  a Kubernetes node name, so `machine_of` is unambiguous. (Legacy: Nick `<m>-c<i>`,
  Sebastian `<m>:c<i>`.)
- **Placement and order**: SAGA picks a core; the machine is enacted (Wayline: the
  controller places the pod on it; Slurm: `srun --nodelist`). The OS / Slurm picks the
  actual core. Per-machine order = the machine's tasks sorted by SAGA's predicted start
  (ties: predicted finish, then request order), all cores merged, as Nick did.
- **Links**: three kinds, all explicit in the request: self-loop of a core
  (`selfBandwidth`), between cores of one machine (`intraBandwidth`), across machines
  (`clusterState.bandwidth`, directed, from `machines.yaml network.links`; unlisted
  pairs `default_bandwidth`, Wayline's 125000000; SAGA networks are undirected, so
  the bridge uses **min of both directions**).
- **Runtimes**: related machines. `runtime(task, machine) = cost × sec_per_cost / speed[machine]`,
  sent as each task's `runtimeProfile` (and `runtime` = time at speed 1.0). The bridge
  fits cost/speed (rank 1, log space); by construction the fit is exact,
  `costModelFitRMSE ≈ 0` (`check_fit` warns and `meta.costModelExact` is false
  otherwise). With PyPI SAGA the bridge plans with that cost/speed (and refuses a
  non-exact fit); with `feature/capacity` it passes the exact runtimes, the same numbers.
- **Data**: one named output per edge (`to-<consumer>`, `int(size × bytes_per_size)` bytes);
  the consumer declares it in `inputs`, so each edge carries its own size.
- **Units**: seconds and bytes (`sec_per_cost`, `bytes_per_size` applied **before**
  scheduling; Sebastian's `time_scale` is gone).
- **Determinism**: SAGA breaks ties in str-hash order; the bridge always runs with
  `PYTHONHASHSEED=0` (`run_bridge` starts it in a subprocess with that seed; the
  sidecar deployment sets the same). The SAGA version must also match: responses
  carry `sagaVersion` (prediction meta, `status.scheduling.externalSagaVersion`).

### SAGA version

`bridge.py`'s default (capacity) model needs SAGA branch `feature/capacity`
(2.2.0.dev1: `TaskGraphNode.runtimes/demand/allowed_nodes`, `NetworkNode.capacity`,
`saga.schedulers.contention`), which is not on GitHub or PyPI and was not found on
cse-lambda1. Older SAGA silently ignores those fields, so the bridge now detects
this: without them it refuses the capacity model, and accepts `nodeModel: cores`
only when the runtimes fit cost/speed exactly — which ours do. So **our runs work
with PyPI `anrg-saga` 2.1.0** (locked here), and the sidecar image can be built from
the same wheel. Open: the `cse-lambda1:5000` registry has no `wl-saga-sidecar`
image at all (2026-11-17), so in-cluster `saga/<alg>` runs currently fall back to
random placement (and `analyze.py` rejects them) until one is built and deployed
together with the `saga-executor` controller. Build it from the same SAGA version
as `uv.lock` (2.1.0) so offline and in-cluster predictions match.

## 1. Instance (`graphs/*.json`)

Nick's format:

```json
{
  "name": "smoke-diamond",
  "source": "hand-written, for end-to-end testing",
  "tasks": {"a": 2.0, "b": 4.0},
  "edges": [["a", "b", 10000000]]
}
```

- `tasks`: task -> cost, in *cost units*; `edges`: `[producer, consumer, size]`, in *size units*;
  no duplicate edges.
- Config maps units to seconds (at speed 1.0) and bytes: `sec_per_cost`, `bytes_per_size`.
- Graph `name` must be unique across datasets. Wayline additionally needs task
  names that are Kubernetes DNS labels (lower case); Slurm does not.

Slurm run files now reference an instance (`"instance": "graphs/slurm-x.json"`)
instead of embedding one.

## 2. Testbed (`testbed/machines.yaml`)

Machines (speed, cores, roles), the network (directed links, `default_bandwidth`)
and per-backend partitions. What SAGA reads (`saga_exec.testbed.load_testbed`):

| Key | Used for |
|---|---|
| `machines.<m>.speed` | related-machines speed (a config's `speed` overrides it, e.g. emulated speeds) |
| `machines.<m>.self_bandwidth`, `intra_bandwidth` | `model.links: measured` (UNMEASURED placeholders for now) |
| `network.links`, `network.default_bandwidth` | inter-machine links; Wayline's ConfigMap is generated from them |
| `partitions.<p>.saga_nodes_per_machine` | default `model.cores` (k8s 8, Slurm 14) |
| `partitions.<p>.saga_cpu_millis` | node `cpuMillis` in the request (what the controller reports; only the capacity model uses it) |
| `partitions.<p>.task_cpu` | default task CPU request (pod / srun resources) |

`local_bandwidth` is only for the legacy core-node models.

k8s node CPU: in the cluster the controller sends each node's **allocatable CPU
minus the requests of non-Wayline pods** (`getNodeInfoMap`), which we cannot know
offline. The cores model does not use it (C is explicit), so offline and in-cluster
SAGA still see the same problem; `analyze.py` notes a mismatch with
`status.scheduling.externalNodeCPUMillis` and warns when a machine has fewer free
CPUs than SAGA cores (then k8s cannot run them all at once).

## 3. Prediction (`prediction.json`) and actual (`actual.json`)

```json
{
  "makespan": 7.000015,
  "meta": {"backend": "wayline", "instance": "smoke-diamond", "config": "heft-order-sleep-119bea",
           "enact_order": "order", "source": "bridge-offline", "scheduler": "heft",
           "saga_version": "2.1.0", "costModelFitRMSE": 0.0, "costModelExact": true,
           "capacityModel": "cores", "model": {"nodeModel": "cores", "cores": {"...": 8}, "...": "..."},
           "pythonHashSeed": "0"},
  "tasks": {"a": {"node": "cse-isha/c6", "machine": "cse-isha", "start": 0.0, "end": 2.0}},
  "order": {"cse-isha": ["a", "b", "c", "d"]},
  "request": {"algorithm": "heft", "dag": {"tasks": ["..."]}, "clusterState": {"...": "..."}},
  "response": {"assignments": ["..."], "estimatedMakespan": 7.000015, "...": "..."}
}
```

- `node` = SAGA's core `<machine>/c<i>`, `machine` = where it is placed. Legacy
  predictions have `cse-isha-c6`.
- `order`: per-machine start order exactly as the controller derives it (stable
  sort of the response by `(estimatedStart, estimatedFinish)`); what `enact_order: order` enacts.
- `request` / `response`: the exact bridge input and output, for reproducibility.
- Wayline also writes `runs/<odag>/prediction.json` from `status.scheduling`
  (`meta.source: "wayline-status"`, plus `run`, `nodeCPUMillis`, `constraintOverrides`
  and `meta.offline`: whether it equals the offline one).

`actual.json`:

```json
{
  "run": "smoke-diamond-heft-order-sleep-119bea-run-hzjd6",
  "makespan": 9.21,
  "t0": "2026-11-05T22:58:59.819361448Z",
  "tasks": {
    "a": {"node": "cse-isha", "machine": "cse-isha", "start": 0.0, "end": 2.196,
          "input_read": 0, "compute": 2.031, "handoff": 0.164}
  }
}
```

- Times in **seconds**, relative to the first task's start.
- `node`: the most specific location observed (Wayline: machine; Slurm: `"cse-isha core5"`).
- Optional per task: `input_read` / `compute` / `handoff` (Wayline SDK); `state`, `delay` (Slurm).
- Wayline also has `makespan_from_odag_start`; Slurm writes `summary.json` as before.
- Per-run folder: Wayline `results/wayline/<instance>/<config name>/` with `template.yml`,
  `network-profile.yml`, `request.json`, `prediction.json`, `instance.json`, `config.json`,
  `runs/<odag>/{status.json,logs.txt,prediction.json,actual.json,gantt.png}`;
  Slurm `~/slurm-logs/saga/<run_id>/` with `prediction.json`, `actual.json`,
  `summary.json`, `plan.json`, `job.sbatch`, `results.json`, `driver.log`, `gantt.png`.

## 4. Enactment: `enact_order` (both backends)

| Value | Wayline | Slurm |
|---|---|---|
| `none` | `schedulerConfig.enactOrder: none`: placement only, dispatch on data readiness | tasks start when their data has arrived |
| `order` (default) | `enactOrder: order`: a task's pod is created only after every earlier task in its machine's order has a pod | `start_after`: a step starts only after its machine-order predecessor has been started |

Placement is always enacted (Wayline: the controller places; Slurm: `srun --nodelist`).
Slurm's old `strict_order` (wait for the previous task on the same SAGA **core** to
finish) is accepted with a deprecation warning and mapped to `order` / `none`.
Fallbacks: the controller falls back to random placement when the sidecar fails and
records it (`status.scheduling.used != requested`, `fallbackError`); `analyze.py`
rejects such runs (`run-log.csv` phase `Rejected`).

## 5. Legacy vs bridge

The students' models are kept in `saga_exec.legacy`; `tests/test_legacy_vs_bridge.py`
runs the comparison (all with PyPI SAGA 2.1.0):

- **Legacy Wayline (Nick: C core-nodes, 1e12 B/s inside a machine) vs bridge
  (`cores`, links `free`)**: identical makespan and identical per-task start/end on
  smoke (HEFT and CPOP, C = 8 and 14), the same machine mapping up to swapping the
  two identical machines (ties follow str-hash order over different node names), and
  identical HEFT makespans on all 11 `graphs/slurm-*.json` (C = 14, isha at half speed).
  Expected: the bridge's rank-1 fit is exact and the network is the same.
- **Legacy Slurm (Sebastian: inf inside a machine)**: same mapping; makespan lower by
  exactly the same-machine transfer time at 1e12 B/s (smoke: 15 MB on the critical
  path = 1.5e-5 s), because inf makes those transfers free.
- The bridge's default capacity model (one node per machine) was **not** compared:
  it needs SAGA `feature/capacity`, and our experiments use `cores`.

## 6. Open questions

Settled by the bridge path: former 1 (node names: `<machine>/c<i>`), 2 (local
bandwidth: explicit `model.links`, self and intra separately), 3 (bandwidth:
measured, min of directions), 4 (scaling before scheduling), 5 (cores seen by SAGA:
`model.cores`, default machines.yaml), 9 (instance format), 11
(scheduler selection: any bridge algorithm or dotted path), 12 (speeds:
machines.yaml + override), 15 (order: `enact_order`), 26 (reproducibility:
`PYTHONHASHSEED=0` always). Still open:

| # | Topic | Wayline backend | Slurm backend |
|---|---|---|---|
| 6 | Task CPU unit | `cpu: "1"` = 1 hardware thread | `cpus: 1` = 1 core = 2 Slurm CPUs (threads). SAGA's "core" is one task slot in both, but different hardware |
| 7 | Task memory | 512 MiB + 2x bytes held (or `Config.memory`) | fixed `mem` (default 1G) |
| 8 | Core partition enforcement | kubelet reservations (12 allocatable CPUs on lambda1), no pinning to cores 18–31 | `CpuSpecList` + cgroups pin to cores 4–17 |
| 8b | Cores SAGA plans vs what runs concurrently | 8 SAGA cores; kubelet admits up to the free CPU (≈12 threads on lambda1, unverified on isha), so more than 8 tasks may overlap with `enact_order: none` | 14 SAGA cores = Slurm's 14-core share |
| 10 | Task names | must be Kubernetes DNS labels | anything |
| 13 | What a task does | `sleep` (FID_SECONDS / speed) or `cpu` (sha256 loop) in a container with the Wayline SDK | `bash sleep` for its runtime on that machine |
| 14 | Data transfers | real bytes through Wayline's data agent (also between tasks on one machine / core) | simulated: driver delays a child by bytes / SAGA's link speed (self / intra / inter as in the model) |
| 16 | Placement mechanism | the controller binds pods to SAGA's node | `srun --nodelist=machine` (+ `--exact`, `--cpu-bind=none`) |
| 17 | Measured start / end | SDK `taskStartTime` / `taskCloseTime` (end includes output handoff) | task's own `date +%s.%N` around `sleep` |
| 18 | Clocks | per-node clocks in SDK timestamps | per-node clocks; driver's clock for transfer waits |
| 19 | Measured makespan | last close − first start (also `makespan_from_odag_start`) | max end − min start |
| 20 | Actual `node` | machine only | host + physical core |
| 21 | Phase breakdown | `input_read`, `compute`, `handoff` | `delay`, `transfer_from` |
| 23 | Output location | `results/wayline/` in the repo | `~/slurm-logs/saga/<run_id>/` on lambda |
| 24 | Run identity | config hash in name + ODAG name | timestamp `run_id` |
| 25 | Repetitions | `run.py --reps N --shuffle SEED`, `run-log.csv` | one run per invocation |
| 27 | Speed calibration | `hash_rate` by hand | `profiling_test.py` (zlib) |
| 28 | Gantt rows | predicted: SAGA core; actual: first-free slot per machine | predicted: SAGA core; actual: physical core |
| 29 | Python on the task side | `python:3.11-slim` image | none (bash) |

Further questions:

- Should the per-task CPU unit be the same hardware on both (thread vs core)?
- How should the k8s and Slurm partitions be sized so both backends give SAGA the same capacity?
- Directed links: the bridge's min-of-directions is a conservative undirected model (bridge.py LIMITATION).
- Model variants to evaluate: `links` free vs measured (needs `calibrate.links` run
  first) and `transfer_latency` (needs the SAGA extension above). Is the latency term
  worth its complexity? Compare prediction error with and without it.
- Where do calibration results go (machines.yaml vs per-run config)?
- Wayline's own `status.predictedTasks` re-predicts SAGA's placement with Wayline's
  cost model; worth comparing with SAGA's prediction as a third data point.
