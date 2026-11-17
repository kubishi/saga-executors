# docs/

- `data-movement.md` — how data moves on each backend (Wayline's data agent;
  Slurm's `direct` and `shared_fs` transports), which timing phase each cost lands
  in, and the task env contract.
- `formats.md` — architecture (one SAGA path through Wayline's bridge), shared
  formats (instance, machines.yaml, prediction/actual, enact_order), legacy vs
  bridge, the SAGA-version blocker, and the remaining open questions.
- Backend docs: `src/saga_exec/backends/wayline/README.md` (Nick's original fidelity README),
  `testbed/k8s/README.md`, `testbed/slurm/README.md` (Sebastian's original README),
  `testbed/slurm/task-environment.md` (Python on both machines, first runs),
  `testbed/slurm/nfs.md` (shared work dir for `shared_fs`).

## Missing: the fidelity plan

Nick's README (`eval/experiments/fidelity/README.md` in his Wayline fork) says
"Plan: `../../../../fidelity-plan/main.tex` (outside the repo)" and refers to it
for the execution model ("see plan"). That resolves to
`~/src/fidelity-plan/main.tex in Nick's home directory`, which **does not exist on
this machine** (cse-lambda1, checked 2026-11-17), and no `fidelity-plan` directory
was found elsewhere. Ask Nick for it; it should probably live here as
`docs/fidelity-plan/`.

## Provenance

Imported on 2026-11-17 from read-only copies on cse-lambda1. On import, changes
were limited to imports, paths, usage strings, `main()` wrappers and TODO
comments. SAGA scheduling later moved to Wayline's bridge; the students' own SAGA
code is kept unchanged in `src/saga_exec/legacy/` (`git log` has the details).

| Piece here | Source | Commit |
|---|---|---|
| `src/saga_exec/legacy/wayline_convert.py` (was `backends/wayline/convert.py`), `backends/wayline/{run,analyze}.py`, `backends/wayline/README.md` | `github.com/nick23447/wayline` (fork of `ANRGUSC/wayline`), branch `lab`, `eval/experiments/fidelity/` | `aff6fa8a698c1ca2cda6ecd6bb10948ef54df5a8` |
| `experiments/wayline/generate.py`, `task/`, `graphs/smoke.json`, `results/examples/wayline/` | same | `aff6fa8` |
| `testbed/k8s/wayline-deployments/`, `testbed/k8s/wayline-lab.diff` | same repo, `deployments/` (lab vs `origin/main` `4168029`) | `aff6fa8` |
| `testbed/k8s/{kubeadm,calico}/` | Nick's `~/k8s-cluster/` (not under version control) | n/a |
| `src/saga_exec/backends/slurm/*.py`, `src/saga_exec/legacy/slurm_saga.py` (was `saga_slurm.py`) | `github.com/MasterSebas04/saga-slurm`, `executor/` | `e4f6de2b5546fe54060c4f19bfc3d417fcdd1312` |
| `tests/test_legacy_slurm_network.py`, `experiments/slurm/runs/`, `testbed/slurm/`, `results/examples/slurm/` (produced at `3caa937`) | same | `e4f6de2` |
| `graphs/slurm-*.json` | generated from Sebastian's run files (see `graphs/README.md`) | `e4f6de2` |

Both students used `anrg-saga` 2.1.0. SAGA and Wayline are now submodules in
`external/` on branch `saga-executor` (SAGA branched from `main` at `5b20e8d`,
Wayline from `811c951`).
