# How data moves on each backend

Both backends run the same task program (`task/`: `task.py` + `fidelity_core.py`).
Each task reads its inputs, sleeps or hashes for its runtime, and writes
random-byte outputs. It reports the same phase fields: `input_read`, `compute`,
`handoff`, plus `start` / `end`. What differs is how data moves. Each backend
moves data the way that system would normally do it. **The differences are
findings, not things to engineer away.** The tables below say which phase each
cost lands in, so results can be read side by side.

## Wayline (Kubernetes)

Read from Wayline branch `saga-executor` (`sdk/python/wl/transport/file.py`,
`cmd/data-agent/main.go`, `cmd/odag-controller/{main,durability}.go`). For these
parts it is identical to `4168029`, the commit deployed on the cluster. Each node
runs one data agent (DaemonSet, Go, hostPort 8082) that stores data in
`/data/wl-outputs` on the node's disk.

1. **Producer handoff** (`send_raw(object, data)`): the SDK computes sha256 of the
   payload and sends it with HTTP PUT to the agent on its own node. The agent
   streams it into a temp file, hashes it, fsyncs only *durable* objects, checks
   the digest, renames the file into place and marks it ready. Under the
   default durability policy (`auto`), only objects that nothing consumes are
   durable; for us that is a leaf's `b"done"`.
2. **Push request**: the SDK POSTs `/push` with the consumers on *other* nodes.
   The agent queues them, answers 202, and the producer closes and exits. **The
   producer never waits for the network.**
3. **Push, in the background**: the agent sends an HTTP PUT to the consumer
   node's agent, streamed from disk. At most 4 pushes run at a time per node,
   with 5 retries. The receiving agent installs the object atomically and marks
   it ready. This is push, not pull, and there is no compression:
   `--push-compress` defaults to `none` and the DaemonSet passes no flags. (The
   old task.py comment that "the agent gzips" was out of date. Random payloads
   make the flag irrelevant anyway.)
4. **Consumer start**: the controller creates the consumer's pod once every
   object it consumes is ready on *its* node (it polls the agents on events
   plus every 500 ms; `inputsReadyTime`). It does not wait for the producer's
   pod to complete. With `enactOrder: order`, the consumer also waits until its
   machine predecessor has a pod.
5. **Consumer read** (`recv_raw`): a plain local file read.

| Cost | Wayline phase |
|---|---|
| sha256 + local install (+ generating outputs after the first) | producer `handoff` (exact per output: `outputs[].send`, `generate`) |
| cross-node network transfer | **none**: between producer `end` and consumer `inputs_ready` |
| controller reaction + pod start | `delay` (`inputs_ready` → consumer `start`) |
| local file read | consumer `input_read` |

## Slurm: two transports, chosen per run (`"transport"`)

The design follows workflow systems on Slurm (Snakemake, Nextflow, Pegasus). A
task becomes an `srun` step once its producers' **steps have completed** (and,
with `enact_order: order`, once its machine predecessor has started). Data is
files. Which transport a run uses is an **experimental variable**, recorded in
`summary.json` and `results.json`.

### `direct` (default; works on today's cluster)

- **Producer**: writes each output to node-local scratch
  (`/tmp/saga-exec-<user>/<run>/<task>.<object>`, temp file + rename). This lands
  in `handoff` and is a local disk write wherever the consumer runs.
- **Same-machine consumer**: reads the local file (`input_read`).
- **Cross-machine consumer**: *pulls* the file over HTTP from the producer
  machine's **data server** into memory (`input_read`, so the network transfer is
  in the consumer's phase).
- **Data server** (`task.pyz serve`, `task/transport_direct.py`, stdlib): one per
  machine, its own Slurm step for the whole job (`srun --overlap`, floating over
  the job's CPUs there). It serves files with `sendfile`, handles many requests
  at once, binds to the interface that reaches the other machine (or
  `server_bind`), uses a free port that it reports to the driver, and requires
  a per-run token. At the end of the job (or on SIGTERM) it deletes the run's
  scratch and `task.pyz`. It needs no ssh, scp or shared FS.
- **Program delivery**: `sbcast` of `task.pyz` to node-local `/tmp`.

### `shared_fs` (needs a shared work dir, e.g. NFS; [testbed/slurm/nfs.md](../testbed/slurm/nfs.md))

- **Producer**: writes `<workdir>/<user>/<run>/data/<task>.<object>` (temp file +
  rename) and lands in `handoff`. On an NFS client, `close()` flushes the bytes
  to the server, so the network is in the *producer's* handoff.
- **Consumer**: reads the file and lands in `input_read`. On an NFS client this
  is a network read unless the client's cache still holds the data. A file that
  another client just created may not be visible yet, so the read retries for up
  to `latency_wait` s (like Snakemake's `--latency-wait`). The wait counts as
  `input_read`.
- **Program delivery**: `task.pyz` lives in the run's work-dir folder. The driver
  first checks, with one step per machine, that the folder is visible
  everywhere.

### Where each cost lands, side by side

| Edge | Wayline | Slurm `direct` | Slurm `shared_fs` (NFS server on lambda) |
|---|---|---|---|
| same machine | local install (`handoff`) + local read (`input_read`) | local write (`handoff`) + local read (`input_read`) | lambda: local disk*; **isha: write to lambda over the network (`handoff`) + read back (`input_read`, maybe cached)** |
| across machines | local install (`handoff`) + push in **no phase** (gap before the consumer starts) | local write (`handoff`) + pull in consumer `input_read` | writer's `handoff` and/or reader's `input_read`, depending on which side is the NFS server |
| consumer start | when data is on its node | when producers completed | when producers completed |

\* if lambda uses the exported directory directly; through a loopback NFS mount
it is also NFS.

So on `shared_fs` a "same-machine" transfer on isha can cross the network
twice, while lambda→isha crosses it once. SAGA's link model (self / intra /
inter speeds per machine) cannot express this asymmetry. This is an open
question (docs/formats.md).

### Exact per-edge times (actual.json v2)

Besides the phase fields above, every task records an exact timeline (each read,
the work itself, each output's generation and write/send) and each edge gets the
instants along its data path and a decomposition into producer write, wait /
launch, network and consumer read, with "unobservable" where the data path
hides a part (shared_fs network). What is observable on which backend and
transport, and which clock each part uses: [formats.md](formats.md), section 3.
Server-side pull times (direct) and push times (Wayline: sender's agent, plus
the install time on the consumer's node) are recorded by the data path's own
components, which already handle every transfer.

### Practical notes

- Python: the image uses `python:3.11-slim`, Slurm uses a uv-managed 3.12, so
  sha256 speed can differ. Hash rates are calibrated per backend
  (`calibration.cpu.<backend>`).
- Memory: Wayline sizes each pod (512 MiB + 2× bytes held). Slurm uses the run
  file's `mem` and warns when payloads may not fit.
- Network path: Wayline pushes go node IP to node IP. Slurm pulls go to the
  address the machine name resolves to. Check that both use the same NIC
  (iperf3 measured `eno1np0`).
- CPU unit: one hardware thread on k8s vs. one core on Slurm
  ([testbed/k8s/README.md](../testbed/k8s/README.md)).

## Env contract (superset of the original FID_*)

| Var | Used by | Meaning |
|---|---|---|
| `FID_MODE` | all | `sleep` or `cpu` |
| `FID_SECONDS` | all | runtime at speed 1.0 (s) |
| `FID_SPEED` | all | `machine=speed,...`; `sleep` sleeps `FID_SECONDS / speed` |
| `FID_HASH_RATE` | all | `cpu`: sha256/s of a speed-1.0 slot; the task does `FID_SECONDS * FID_HASH_RATE` hashes |
| `FID_INPUTS` | all | `producer.object,...` |
| `FID_OUTPUTS` | all | `object:bytes,...` |
| `FID_TRANSPORT` | all | `wayline` (default, so existing templates are unchanged), `direct`, `shared_fs` |
| `FID_TASK`, `FID_NODE` | Slurm | task name, machine |
| `FID_RUN`, `FID_SCRATCH`, `FID_SERVERS`, `FID_INPUT_NODES`, `FID_TOKEN` | direct | run id, node-local scratch root, `machine=host:port,...`, `producer.object=machine,...`, per-run secret |
| `FID_WORKDIR`, `FID_LATENCY_WAIT` | shared_fs | the run's data dir in the work dir; seconds to wait for a not-yet-visible input |

Wayline tasks also get the controller's `WL_*` variables, which the SDK reads.
