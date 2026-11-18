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
| `links` | **`free`**, `measured` | self (same core) and intra-machine (core to core) links: `free` = 1e12 B/s; `measured` = `machines.yaml machines.<m>.self_bandwidth / intra_bandwidth` (currently UNMEASURED placeholders, 1e9; see `saga_exec.calibrate.links`, which measures through each backend's and transport's real data path) |
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
| `machines.<m>.speed` | related-machines speed (a config's `speed` overrides it, e.g. emulated speeds); `saga-calibrate-cpu` can set it |
| `calibration.cpu.<backend>` | written by `saga_exec.calibrate.cpu` (marker block at the end of the file): per-machine sha256 rates, `hash_rate` (cpu mode's FID_HASH_RATE for a speed-1.0 slot) and the derived speeds |
| `partitions.slurm.shared_workdir` | the Slurm `shared_fs` transport's work dir (NFS, not set up yet; testbed/slurm/nfs.md) |
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

`actual.json` (format `saga-exec-actual/2`, `saga_exec.actual.build_actual`; both
backends write it, and the v1 fields keep their meaning):

```json
{
  "format": "saga-exec-actual/2", "run": "20261118-101500", "backend": "slurm", "transport": "direct",
  "t0": "2026-11-18T10:15:03.120Z", "t0_unix": 1795000503.12, "t0_task": "a", "t0_clock": "cse-lambda1",
  "t0_definition": "earliest task start (taskStartUnix) ...",
  "makespan": 9.21, "makespan_work": 8.95, "first_work_start": 0.004,
  "tasks": {"b": {
    "node": "cse-isha core5", "machine": "cse-isha", "core": "5", "host": "cse-isha", "state": "COMPLETED",
    "clock": "cse-isha",
    "start": 2.31, "end": 6.40, "input_read": 0.41, "compute": 3.62, "handoff": 0.05,
    "inputs_ready": 2.20, "delay": 0.11,
    "times": {"task_start": 2.31, "inputs_read_start": 2.31, "inputs_read_end": 2.72,
              "work_start": 2.72, "work_end": 6.32, "outputs_start": 6.32, "outputs_end": 6.39,
              "task_end": 6.40, "launched": 2.24, "finished": 6.45},
    "durations": {"total": 4.09, "before_work": 0.41, "start_to_first_read": 0.0, "input_read": 0.41,
                  "work": 3.60, "generate": 0.02, "send": 0.05, "outputs": 0.07, "after_outputs": 0.01},
    "inputs": [{"producer": "a", "object": "to-b", "peer": "a.to-b", "bytes": 10000000,
                "source_machine": "cse-lambda1", "read_start": 2.31, "read_end": 2.72, "read": 0.41,
                "via": "pull"}],
    "outputs": [{"object": "to-d", "consumer": "d", "bytes": 5000000, "generate_start": 6.32,
                 "generate_end": 6.34, "send_start": 6.34, "send_end": 6.39, "generate": 0.02, "send": 0.05}],
    "precise": true, "other_clocks": {"launched": "cse-lambda1", "finished": "cse-lambda1"}}},
  "edges": {"a->b": {
    "producer": "a", "consumer": "b", "object": "to-b", "bytes": 10000000,
    "src_machine": "cse-lambda1", "dst_machine": "cse-isha", "src_core": "3", "dst_core": "5", "kind": "inter",
    "times": {"producer_work_end": 1.98, "producer_output_generated": 2.02, "producer_output_send_start": 2.02,
              "producer_output_written": 2.05, "producer_task_end": 2.06, "transfer_start": 2.33,
              "transfer_end": 2.70, "consumer_input_available": 2.20, "consumer_task_start": 2.31,
              "consumer_read_start": 2.31, "consumer_read_end": 2.72, "consumer_work_start": 2.72},
    "transfer": {"mechanism": "pull from the producer machine's data server ...", "clock": "cse-lambda1",
                 "within_consumer_read": true, "server_received": 2.32, "server_body_start": 2.33},
    "available": {"source": "producer step exit noticed by the driver ...", "clock": "cse-lambda1"},
    "data_movement": 0.70, "work_gap": 0.74,
    "decomposition": {"producer_write": 0.03, "wait_launch": 0.26, "network": 0.37, "consumer_read": 0.04,
                      "network_note": "...", "wait_launch_detail": {"producer_end_to_consumer_start": 0.25, "...": 0},
                      "clocks": {"producer_write": ["cse-lambda1"], "network": ["cse-lambda1"], "...": []},
                      "cross_clock": true}}},
  "clocks": {"note": "...", "driver_node": "cse-lambda1", "start": {"...": {}}, "end": {"...": {}},
             "server_vs_driver": {"cse-isha": {"offset": 0.0004, "rtt": 0.0003}}}
}
```

**Time 0.** `t0_unix` is the earliest `taskStartUnix` of the run (the task program's
own start, after interpreter start-up), on the clock of that task's machine
(`t0_clock`). prediction.json's time 0 is the start of SAGA's first task, so both
records count from "the first task begins". Every relative time is
`absolute - t0_unix`. (Wayline's `status.makespan` and `actualNetworkFlows[].start`
use the first pod's start instead; they are not used.) `makespan` = last task end;
`makespan_work` = last work end; `first_work_start` shows how far the first task's
work starts after t0.

**Per task.** `start` / `end` (= `times.task_start` / `task_end`): the task
program's `taskStartUnix` / `closeUnix` (Wayline: the SDK's `taskStartTime` /
`taskCloseTime`). `times.work_start` / `work_end` are taken immediately around
`do_work()` (pure work: no input reads, no payload generation, no sends), and
`durations.work` is the compute to compare with SAGA's runtime. `inputs[]` /
`outputs[]` give each read (start/end) and each output's payload generation and
write/send, in the order the program did them; transport details are copied in
(`via`, `source_machine`; shared_fs `attempts`, `latencyWaited`, `opened_rel`,
`written_rel`, `renamed_rel`). `precise` is false when the task left no timeline
(Wayline images built before it), and then only the v1 fields are set.
`core` is the physical core on Slurm, `null` on Wayline. v1 fields are kept:
`input_read` / `compute` / `handoff` are the SDK's phase definitions
(fidelity_core docstring; `compute` includes generating the first output, so
use `durations.work`), `inputs_ready` / `delay` as before.

**Per edge** `u->v` (one named object per edge): the instants along the data path
(`times`), the observed `kind` (`inter`, `same-core`, `intra`, or `same-machine`
when cores are unknown, as on Wayline), and a decomposition of
`data_movement` = consumer read end − producer send start:

| part | meaning |
|---|---|
| `producer_write` | the producer's write/send of this object (Wayline: sha256 + local install + push request; Slurm: file write + rename) |
| `network` | the observed transfer interval: Wayline push (sender's agent), Slurm direct pull (data server on the producer machine). `0.0` for a same-machine edge on Wayline / direct (local file). `"unobservable"` on shared_fs (the network is inside the write and/or the read) or when no record exists |
| `consumer_read` | the consumer's read of this input, minus `network` when the transfer happens inside the read (direct pull) |
| `wait_launch` | the rest: `data_movement - producer_write - network - consumer_read` (producer close, driver/controller reaction, step/pod launch, interpreter start, earlier inputs, push queue). `wait_launch_detail` splits it where both ends are known |

`work_gap` = consumer work start − producer work end: the interval SAGA's comm time
has to fit into. `decomposition.clocks` names the machine clock(s) each part
uses; `cross_clock: true` means `data_movement` and `wait_launch` mix two clocks
(see Clocks). Nothing is estimated: a quantity that is not observed is `null`
or `"unobservable"`.

What each backend / transport observes per edge:

| | Wayline | Slurm `direct` | Slurm `shared_fs` |
|---|---|---|---|
| producer work end, output generated, written | task timeline (pod log `FID_TIMELINE`) | task timeline (`FID_RESULT`) | task timeline |
| transfer start / end | cross-node: push wire start/end, sender's agent (`actualNetworkFlows`; absolute `startUnix` needs the saga-executor controller, otherwise reconstructed from the whole-second pod origin and flagged) + `push_request`, `push_hashed`, receiver's `receive_start`; same node: none (local file) | cross-machine: server-side pull (`received`, `start`, `body_start`, `end`, bytes), producer machine's clock; same machine: none | **unobservable** (NFS); write side `written`/`renamed`, read side `opened`, `attempts`, `latencyWaited` |
| consumer input available | install record on the consumer's node, per object (`status.tasks[].inputs`, saga-executor controller); else `inputsReadyTime` for single-input tasks | producer step exit noticed by the driver (driver's clock) | same as direct |
| consumer read start / end | task timeline | task timeline (includes the pull) | task timeline (includes NFS read and latency wait) |

compare.json (`saga_exec.compare.side_by_side`, written next to actual.json by
both backends): per task SAGA's machine/core/start/end/duration next to the actual
task start/end, work start/end and work duration, with `actual_minus_saga` for
both pairs; per edge SAGA's link kind and speed, comm time (bytes / link speed, as
the bridge builds the link from the request) and predicted gap, next to the
actual `data_movement`, `work_gap` and decomposition; the makespans; the
per-machine order (SAGA, actual by work start, actual by task start). It states
differences, not verdicts.

**Clocks.** Every absolute time is the clock of the machine that took it, so a
cross-machine interval contains the offset between the two clocks. Nothing is
corrected; `clocks` records what is known, best-effort:
- Slurm: `driver.py` runs a one-line probe step on every machine
  (`chronyc -c tracking`, else `timedatectl timesync-status`, plus `date`) at job
  start, in the background and never waited for, and again after the last task
  (`clock-start.log`, `clock-end.log`; run file `"clock_probe": false` turns it
  off). With `direct`, the status call to each data server at the end is timed:
  `server_vs_driver.<m>.offset` = server time − midpoint, ± `rtt` / 2.
- Wayline: `run.py` records `chronyc -c tracking` (or timedatectl) on the machine
  it runs on (cse-lambda1) before and after each run (`runs/<odag>/clock.json`).
  The other machine is not read from there (no ssh from run.py); to capture it,
  run `chronyc -c tracking` on cse-isha before and after a batch (e.g.
  `ssh cse-isha chronyc -c tracking >> isha-clock.csv`) and keep it with the
  results. Within one Wayline edge the push times are on the sender's clock and
  `available` / `receive_start` on the receiver's.

**Measurement only, and its limits.** The instrumentation adds timestamps and
records around code that already runs; it does not reorder, wait, or change
what the drivers launch or how data moves (`tests/test_timeline.py` checks that
the task program's operations and their order are identical to before). The
two things that run in addition are the Slurm clock probe step at job start (in
the background, through slurmctld like any step; `"clock_probe": false` removes
it) and one local `chronyc` call in Wayline's `run.py` before a run is created.
Not measured, because it would need a change to execution:
- the network part of a shared_fs edge (NFS client write-back / read; it would
  need a separate sync or per-file NFS statistics);
- client-side first byte of a direct pull (the server's `body_start` is the
  proxy; timing inside `request()` would add work to the read);
- the moment a Wayline consumer *could* have been created vs when the controller
  noticed (its 500 ms poll / events are inside `wait_launch`), and per-core
  placement on Wayline (no pinning);
- on Slurm, when a producer's exit became visible to the driver is observed
  (`available`, 5 ms poll), not when its `srun` step was torn down;
- clock offsets are snapshots at start/end, not corrections; cross-clock
  intervals are reported as they are.

- Per-run folder: Wayline `results/wayline/<instance>/<config name>/` with `template.yml`,
  `network-profile.yml`, `request.json`, `prediction.json`, `instance.json`, `config.json`,
  `runs/<odag>/{status.json,logs.txt,clock.json,prediction.json,actual.json,compare.json,gantt.png}`;
  Slurm `~/slurm-logs/saga/<run_id>/` with `prediction.json`, `actual.json`, `compare.json`,
  `summary.json` (incl. `transport`, `transfers`), `plan.json`, `job.sbatch`, `task.pyz`,
  `results.json` (incl. `clocks`), `driver.log`, `server-<machine>.log` (direct),
  `clock-start.log`, `clock-end.log`, `<task>.out`, `gantt.png`.
- The Wayline per-edge fields need the task image rebuilt from `task/` (for the
  `FID_TIMELINE` line) and, for per-object availability and absolute push times,
  the controller, data agent and CRD (`api/v1/odag-crd.yml`, so the API server keeps
  the new status fields) from Wayline's `saga-executor` branch. Older runs still
  analyze; the missing fields are `null`.

## 4. Enactment: `enact_order` (both backends)

| Value | Wayline | Slurm |
|---|---|---|
| `none` | `schedulerConfig.enactOrder: none`: placement only, dispatch on data readiness | tasks start when their data has arrived |
| `order` (default) | `enactOrder: order`: a task's pod is created only after every earlier task in its machine's order has a pod | `start_after`: a step starts only after its machine-order predecessor has been started |

Placement is always enacted (Wayline: the controller places; Slurm: `srun --nodelist`).
On Slurm a task also needs its producers' steps COMPLETED; on Wayline its input
data on its node (see 4b).
Slurm's old `strict_order` (wait for the previous task on the same SAGA **core** to
finish) is accepted with a deprecation warning and mapped to `order` / `none`.
Fallbacks: the controller falls back to random placement when the sidecar fails and
records it (`status.scheduling.used != requested`, `fallbackError`); `analyze.py`
rejects such runs (`run-log.csv` phase `Rejected`).

## 4b. What tasks do and how data moves

Both backends run the same task program (`task/`): read every input, then `sleep`
(runtime / speed) or `cpu` (runtime × `hash_rate` chained sha256, so the real
machine sets the time), then write random-byte outputs, one per edge. Data is
never simulated. How it moves is native to each system, and **the transport is
an experimental variable**, not something the backends try to make equal
([data-movement.md](data-movement.md)):

| Backend / `transport` | Data path | Consumer starts when | Network transfer is in |
|---|---|---|---|
| Wayline | data agent: local install, background push to the consumer's node | its inputs are on its node | no task phase (gap before start) |
| Slurm `direct` (default) | node-local scratch; per-machine data server; consumer pulls | producers' steps completed | consumer `input_read` |
| Slurm `shared_fs` | files in a shared work dir (NFS) | producers' steps completed | producer `handoff` and/or consumer `input_read` |

Comparing the two Slurm transports with each other and with Wayline is part of
the study, for example whether SAGA's link model fits one data path better than
another.

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
| 13 | What a task does | SETTLED: the same task program (`task/`, `sleep` or `cpu`) on both; container (python:3.11) vs. `srun` step (uv Python 3.12) | |
| 14 | Data transfers | SETTLED (no simulation): real bytes on both, each backend's own data path (4b); the transport is an experimental variable | |
| 16 | Placement mechanism | the controller binds pods to SAGA's node | `srun --nodelist=machine` (+ `--exact`, `--cpu-bind=none`) |
| 17 | Measured start / end | SETTLED: the task core's `taskStartUnix` / `closeUnix` on both (end includes output handoff); exact work start / end from the task timeline (section 3) | |
| 18 | Clocks | per-node clocks in SDK timestamps; recorded, not corrected: run.py records lambda1's NTP state, isha by hand (section 3, Clocks) | per-node clocks (`inputs_ready` is the producer's clock); probe step per machine at job start / end + server offsets (section 3, Clocks) |
| 19 | Measured makespan | last close − first start (also `makespan_from_odag_start`) | max end − min start |
| 20 | Actual `node` | machine only | host + physical core |
| 21 | Phase breakdown | SETTLED: `input_read`, `compute`, `handoff`, `inputs_ready` on both (Slurm also `delay`); actual.json v2 adds the exact timeline per task and per edge (section 3) | |
| 23 | Output location | `results/wayline/` in the repo | `~/slurm-logs/saga/<run_id>/` on lambda |
| 24 | Run identity | config hash in name + ODAG name | timestamp `run_id` |
| 25 | Repetitions | `run.py --reps N --shuffle SEED`, `run-log.csv` | one run per invocation |
| 27 | Speed calibration | `hash_rate` by hand (k8s calibration outlined in `calibrate.cpu`) | `saga-calibrate-cpu`: the cpu-mode loop per core, single and all cores busy, into machines.yaml `calibration.cpu.slurm` + `speed` (not run yet) |
| 28 | Gantt rows | predicted: SAGA core; actual: first-free slot per machine | predicted: SAGA core; actual: physical core |
| 29 | Python on the task side | `python:3.11-slim` image | uv-managed 3.12 on each machine (testbed/slurm/task-environment.md) |
| 30 | Shared-FS link asymmetry | n/a | with `shared_fs` over NFS from lambda, a "same-machine" transfer on isha crosses the network twice (write to lambda, read back) while lambda→isha crosses once; SAGA's per-machine self/intra and per-pair inter speeds cannot express this. Fit separately per transport (`calibrate.links`)? |

Further questions:

- Should the per-task CPU unit be the same hardware on both (thread vs core)?
  Recommended k8s change (static CPU manager, whole cores): testbed/k8s/README.md.
- Which data paths fit SAGA's model best (Wayline push, Slurm direct pull, Slurm
  shared FS)? Link calibration per backend and transport: `saga_exec.calibrate.links`.
- How should the k8s and Slurm partitions be sized so both backends give SAGA the same capacity?
- Directed links: the bridge's min-of-directions is a conservative undirected model (bridge.py LIMITATION).
- Model variants to evaluate: `links` free vs measured (needs `calibrate.links` run
  first) and `transfer_latency` (needs the SAGA extension above). Is the latency term
  worth its complexity? Compare prediction error with and without it.
- Where do calibration results go (machines.yaml vs per-run config)? For now
  machines.yaml `calibration` (per backend), with speeds copied to `machines.<m>.speed`.
- Wayline's own `status.predictedTasks` re-predicts SAGA's placement with Wayline's
  cost model; worth comparing with SAGA's prediction as a third data point.
