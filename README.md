# saga-executors

Execute [SAGA](https://github.com/ANRGUSC/saga) schedules on real systems
(Kubernetes/Wayline and Slurm, on `cse-lambda1` and `cse-isha`) to measure how
well SAGA's *related machines* model (task cost / machine speed, link bandwidth)
predicts real executions. Only SAGA's task -> machine mapping (and optionally each
machine's start order) is enacted; actual start times are emergent and compared
with SAGA's predicted times afterwards.

## Architecture: one SAGA path, backends only enact and measure

```
graphs/<x>.json ─┐
testbed/machines.yaml ─┼─> saga_exec.scheduling.build_request()  ==  Wayline controller's saga.go request
config ──────────┘                 │
                                   ▼
                     Wayline's bridge (saga-sidecar/bridge.py, PYTHONHASHSEED=0)
                                   │
                 ┌─────────────────┴──────────────────┐
     Wayline: in the cluster                   Slurm: offline
     (spec.scheduler saga/<alg>; controller    (saga_slurm.prepare -> allocation.py
      enacts it, records SAGA's prediction       one sbatch, driver.py srun steps,
      in status.scheduling) + the same           enact_order "order" = start order)
      request offline for comparison
                 └─────────────────┬──────────────────┘
                     prediction.json (SAGA)  vs  actual.json (measured)
```

- SAGA sees each machine as C cores `<machine>/c<i>` that run one task at a time
  (the bridge's opt-in `nodeModel: cores`; C from `machines.yaml`), runtimes =
  cost / speed (so the bridge's cost-model fit is exact), and three link kinds:
  self (same core), intra-machine, inter-machine (measured, min of both
  directions). The run config's `model` picks the variant (`links: free|measured`,
  `transfer_latency` reserved). Details and the open questions: `docs/formats.md`.
- The request is identical offline and in the cluster: `tests/data/golden/` was
  produced by the controller's own Go code and `tests/test_scheduling.py` checks it.
- The students' original SAGA construction (core-nodes per machine) is in
  `saga_exec.legacy`, compared with the bridge in `tests/test_legacy_vs_bridge.py`.

> **Cluster status:** the `cse-lambda1:5000` registry has no `wl-saga-sidecar` image,
> so in-cluster `saga/<alg>` runs would fall back to random placement (and be rejected
> by `analyze.py`) until the sidecar (same SAGA as `uv.lock`, 2.1.0) and the
> `saga-executor` controller are built and deployed. Offline scheduling works.

## Dependencies

`wl-saga-sidecar` (the bridge) comes from a local clone of Wayline's branch
`saga-executor` next to this repo (`../wayline/saga-sidecar`, editable; see
`[tool.uv.sources]` in `pyproject.toml`). **Once that branch is pushed**, switch to
the commented git source there (`git = "https://github.com/ANRGUSC/wayline",
branch = "saga-executor", subdirectory = "saga-sidecar"`) and run `uv lock`.
`anrg-saga` is PyPI 2.1.0: enough for the cores model (the bridge's default capacity
model would need SAGA `feature/capacity`, unpublished). The cluster must
run the controller from the same Wayline branch (it records SAGA's prediction in
`status.scheduling`) with a sidecar image built from the same SAGA.

## Quickstart

```bash
git clone https://github.com/ANRGUSC/wayline.git ../wayline && git -C ../wayline checkout saga-executor
uv sync --all-extras          # Python 3.12 venv in .venv, from uv.lock
uv run pytest                 # offline tests; never touch a cluster

# Wayline: templates + offline prediction (no cluster) -> results/wayline/<graph>/<config>/
uv run python experiments/wayline/generate.py
# ... then on cse-lambda1 (kubectl + Wayline saga-executor deployed; see testbed/k8s/README.md):
uv run saga-wayline-run results/wayline/smoke-diamond/heft-order-sleep-119bea --reps 20 --shuffle 1
uv run saga-wayline-analyze results/wayline/<graph>/<config>/runs/<odag>/status.json

# Slurm, on cse-lambda1 (submits a job!). Add "dry_run": true to a run file to only print it.
uv run saga-slurm experiments/slurm/runs/diamond.json
uv run saga-slurm-profile      # rough per-machine speed test (submits a job)
```

No `PYTHONHASHSEED` needed: the bridge always runs with `PYTHONHASHSEED=0`, as in
the sidecar deployment. The task container for Wayline is built from `task/`
(see `task/Dockerfile`).

## Layout

```
testbed/            machines.yaml (single source of truth), k8s/, slurm/ cluster configs + notes
src/saga_exec/
  instance.py       graphs/*.json
  testbed.py        machines.yaml
  scheduling.py     THE SAGA path: bridge request, run_bridge, prediction.json
  schedule.py       prediction.json / actual.json records
  compare.py        predicted vs actual metrics
  calibrate/        speed / bandwidth calibration (links.py: outline for self/intra links)
  backends/
    base.py         Backend protocol: prepare / submit / collect (draft)
    wayline/        convert (template + offline prediction), run (kubectl), analyze (status -> prediction/actual)
    slurm/          saga_slurm (run file -> prediction -> plan), allocation, driver, ...
  legacy/           students' original SAGA construction (wayline_convert, slurm_saga, core_network)
task/               Wayline task program + Dockerfile
graphs/             shared instances (smoke.json, slurm-*.json)
experiments/        what to run: wayline/generate.py, slurm/runs/*.json
results/            run outputs (gitignored) + examples/ from the original repos (legacy model)
analysis/           cross-backend analysis (empty)
docs/               formats.md (architecture, formats, open questions)
tests/              pytest, offline only (tests/data/golden: requests from the controller's Go code)
```

## Who owns what

| Area | Owner |
|---|---|
| `src/saga_exec/backends/wayline/`, `task/`, `experiments/wayline/`, `testbed/k8s/` | Nick Laus |
| `src/saga_exec/backends/slurm/`, `experiments/slurm/`, `testbed/slurm/` | Sebastian Lange |
| `src/saga_exec/scheduling.py`, `testbed.py`, shared modules, `docs/`, `testbed/machines.yaml` | everyone (Jared reviews) |
| `src/saga_exec/legacy/` | frozen (Nick's and Sebastian's original code) |
| Wayline bridge / controller (`ANRGUSC/wayline`, branch `saga-executor`) | Wayline maintainers |

## Provenance

Imported on 2026-11-17 from read-only copies on cse-lambda1. Changes on import were
limited to imports, paths, usage strings, `main()` wrappers and TODO comments. Since
then SAGA scheduling moved to Wayline's bridge: the students' SAGA construction
lives unchanged in `src/saga_exec/legacy/`, and the backends were adapted to enact
the bridge's prediction (`git log` has the details).

| Piece here | Source | Commit |
|---|---|---|
| `src/saga_exec/legacy/wayline_convert.py` (was `backends/wayline/convert.py`), `backends/wayline/{run,analyze}.py`, `README.md` | `github.com/nick23447/wayline` (fork of `ANRGUSC/wayline`), branch `lab`, `eval/experiments/fidelity/` | `aff6fa8a698c1ca2cda6ecd6bb10948ef54df5a8` |
| `experiments/wayline/generate.py` | same, `generate.py` | `aff6fa8` |
| `task/task.py`, `task/Dockerfile` (build context changed) | same, `task/` | `aff6fa8` |
| `graphs/smoke.json` | same, `graphs/smoke.json` | `aff6fa8` |
| `results/examples/wayline/` | same, `out/` (smoke runs, 2026-11-05) | `aff6fa8` |
| `testbed/k8s/wayline-deployments/`, `testbed/k8s/wayline-lab.diff` | same repo, `deployments/` (lab vs `origin/main` `4168029feeaac5629e1456b72d29ec110c3a531d`) | `aff6fa8` |
| `testbed/k8s/{kubeadm,calico}/` | Nick's `~/k8s-cluster/` (not under version control; files dated 2026-09-15) | n/a |
| `src/saga_exec/backends/slurm/*.py`, `src/saga_exec/legacy/slurm_saga.py` (was `saga_slurm.py`) | `github.com/MasterSebas04/saga-slurm`, `executor/` | `e4f6de2b5546fe54060c4f19bfc3d417fcdd1312` |
| `tests/test_legacy_slurm_network.py` | same, `executor/test_network.py` | `e4f6de2` |
| `experiments/slurm/runs/` | same, `runs/` | `e4f6de2` |
| `testbed/slurm/{README.md,slurm.conf,cgroup.conf}` | same, `README.md` (configs extracted from it) | `e4f6de2` |
| `results/examples/slurm/*.csv` | same, `results/` (produced at `3caa937`) | `e4f6de2` |
| `graphs/slurm-*.json` | generated from Sebastian's run files (see `graphs/README.md`) | `e4f6de2` |

SAGA: both students used `anrg-saga` 2.1.0 (Nick from PyPI; Sebastian an editable
checkout of `ANRGUSC/saga` at `5b20e8d`). This repo locks `anrg-saga` 2.1.0 from
PyPI (enough for the legacy code and the bridge's cores model)
and `wl-saga-sidecar` from the local Wayline clone. No submodules.
