# saga-executors

Adapters that take [SAGA](https://github.com/ANRGUSC/saga) schedules
(PyPI `anrg-saga`, imported as `saga`) and execute them on real systems, to
measure how well SAGA's *related machines* model (node speed + link bandwidth)
predicts real makespans.

Pipeline per backend:

1. testbed info -> `saga.Network` (each machine = C cores = C SAGA nodes; free
   links inside a machine, measured bandwidth across machines),
2. schedule an instance with a SAGA scheduler,
3. convert the schedule into something the system can execute,
4. run it and compare the predicted vs. actual schedule / makespan.

Backends: **Wayline** (Kubernetes) and **Slurm**, on the same two machines,
`cse-lambda1` and `cse-isha`.

> Status: the two backends were imported mostly as-is from the students' repos
> and do not share code or formats yet. `docs/formats.md` is the draft proposal
> and lists every divergence.

## Quickstart

```bash
uv sync --all-extras          # Python 3.12 venv in .venv, anrg-saga etc. from uv.lock
uv run pytest                 # offline tests; never touch a cluster

# Wayline: schedule + generate templates (offline) -> results/wayline/
PYTHONHASHSEED=0 uv run python experiments/wayline/generate.py
# ... then on cse-lambda1 (kubectl + Wayline deployed; see testbed/k8s/README.md):
uv run saga-wayline-run results/wayline/smoke-diamond/heft-c8-sleep-a5faff --reps 20 --shuffle 1
uv run saga-wayline-analyze results/wayline/<graph>/<run>/runs/<odag>/status.json

# Slurm, on cse-lambda1 (submits a job!). Add "dry_run": true to a run file to only print it.
uv run saga-slurm experiments/slurm/runs/diamond.json
uv run saga-slurm-profile      # rough per-machine speed test (submits a job)
```

The task container for Wayline is built from `task/` (see `task/Dockerfile`).

## Layout

```
testbed/            machines.yaml (DRAFT single source of truth), k8s/, slurm/ cluster configs + notes
src/saga_exec/
  instance.py       shared instance format (draft)
  network.py        machines.yaml -> per-core saga.Network (draft)
  schedule.py       prediction.json / actual.json records (draft)
  compare.py        predicted vs actual metrics (draft)
  calibrate/        speed / bandwidth calibration (placeholder)
  backends/
    base.py         Backend protocol: prepare / submit / collect (draft)
    wayline/        Kubernetes / Wayline backend (convert, run, analyze)
    slurm/          Slurm backend (saga_slurm, allocation, driver, ...)
task/               Wayline task program + Dockerfile
graphs/             shared instances (smoke.json, slurm-*.json)
experiments/        what to run: wayline/generate.py, slurm/runs/*.json
results/            run outputs (gitignored) + examples/ from the original repos
analysis/           cross-backend analysis (empty)
docs/               formats.md (draft proposal + open questions)
tests/              pytest, offline only
```

## Who owns what

| Area | Owner |
|---|---|
| `src/saga_exec/backends/wayline/`, `task/`, `experiments/wayline/`, `testbed/k8s/` | Nick Laus |
| `src/saga_exec/backends/slurm/`, `experiments/slurm/`, `testbed/slurm/` | Sebastian Lange |
| shared modules, `docs/`, `testbed/machines.yaml` | everyone (draft) |

## Provenance

Imported on 2026-11-17 from read-only copies on cse-lambda1. Changes on import were
limited to imports, paths, usage strings, `main()` wrappers and TODO comments.

| Piece here | Source | Commit |
|---|---|---|
| `src/saga_exec/backends/wayline/{convert,run,analyze}.py`, `README.md` | `github.com/nick23447/wayline` (fork of `ANRGUSC/wayline`), branch `lab`, `eval/experiments/fidelity/` | `aff6fa8a698c1ca2cda6ecd6bb10948ef54df5a8` |
| `experiments/wayline/generate.py` | same, `generate.py` | `aff6fa8` |
| `task/task.py`, `task/Dockerfile` (build context changed) | same, `task/` | `aff6fa8` |
| `graphs/smoke.json` | same, `graphs/smoke.json` | `aff6fa8` |
| `results/examples/wayline/` | same, `out/` (smoke runs, 2026-11-05) | `aff6fa8` |
| `testbed/k8s/wayline-deployments/`, `testbed/k8s/wayline-lab.diff` | same repo, `deployments/` (lab vs `origin/main` `4168029feeaac5629e1456b72d29ec110c3a531d`) | `aff6fa8` |
| `testbed/k8s/{kubeadm,calico}/` | Nick's `~/k8s-cluster/` (not under version control; files dated 2026-09-15) | n/a |
| `src/saga_exec/backends/slurm/*.py` | `github.com/MasterSebas04/saga-slurm`, `executor/` | `e4f6de2b5546fe54060c4f19bfc3d417fcdd1312` |
| `tests/test_slurm_network.py` | same, `executor/test_network.py` | `e4f6de2` |
| `experiments/slurm/runs/` | same, `runs/` | `e4f6de2` |
| `testbed/slurm/{README.md,slurm.conf,cgroup.conf}` | same, `README.md` (configs extracted from it) | `e4f6de2` |
| `results/examples/slurm/*.csv` | same, `results/` (produced at `3caa937`) | `e4f6de2` |
| `graphs/slurm-*.json` | generated from Sebastian's run files (see `graphs/README.md`) | `e4f6de2` |

SAGA: both students used `anrg-saga` 2.1.0 (Nick from PyPI; Sebastian an editable
checkout of `ANRGUSC/saga` at `5b20e8d`). This repo uses `anrg-saga` from PyPI,
locked in `uv.lock` (2.1.0). No submodules.
