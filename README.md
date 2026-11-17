# saga-executors

How well does [SAGA](https://github.com/ANRGUSC/saga)'s simple model of a
distributed system predict what really happens? This project runs SAGA schedules
on two real systems, [Wayline](https://github.com/ANRGUSC/wayline) (Kubernetes)
and Slurm, on the same two machines (`cse-lambda1` and `cse-isha`), and compares
SAGA's predicted schedule with the measured one.

## How it works

Every experiment starts from a task graph (`graphs/`), a description of the
machines (`testbed/machines.yaml`) and a run config. SAGA schedules them through
one shared path: Wayline's SAGA bridge, the same code the Wayline controller uses
in the cluster. So both systems get identical schedules.

Each backend then enacts only the schedule's decisions: which machine runs each
task, and optionally the order of tasks on each machine. When tasks actually start
is up to the real system, and the gap between SAGA's predicted times
(`prediction.json`) and the measured ones (`actual.json`) is what we study.
On both systems every task runs the same program (`task/`), which really
computes and really moves its data. Each system moves data in its own way, and
on Slurm the data path is itself a setting of the experiment
([docs/data-movement.md](docs/data-movement.md)).

In SAGA's model each machine is a set of cores that each run one task at a time.
Task runtime is cost divided by machine speed, and a transfer takes data size
divided by link speed, with separate speeds for the same core, between cores, and
between machines. The run config chooses which parts of that model are used, so we
can test whether adding detail improves predictions. See
[docs/formats.md](docs/formats.md) for the formats, model options and open
questions.

## Setup

```bash
git clone --recurse-submodules https://github.com/kubishi/saga-executors.git
cd saga-executors
uv sync --all-extras
uv run pytest            # offline; never touches a cluster
```

SAGA and Wayline are submodules in `external/`, both on branch `saga-executor`,
and uv installs them in editable mode, so changes to either take effect
immediately. Before editing one, run `git switch saga-executor` inside it (a fresh
submodule checkout is not on any branch). Push submodule commits before the commit
here that uses them; `git config push.recurseSubmodules on-demand` does this for
you.

## Running experiments

Wayline: `experiments/wayline/generate.py` writes templates and SAGA's offline
prediction to `results/wayline/`. On `cse-lambda1`, `uv run saga-wayline-run
<dir>` runs them and `uv run saga-wayline-analyze` compares the results. The
cluster must run the controller and SAGA sidecar built from the `saga-executor`
branches (see [testbed/k8s/README.md](testbed/k8s/README.md)); currently no
sidecar image is deployed.

Slurm: `uv run saga-slurm experiments/slurm/runs/<run>.json` on `cse-lambda1`
submits a job. Add `"dry_run": true` to a run file to only print it. Both
machines need the same uv-managed Python first
([testbed/slurm/task-environment.md](testbed/slurm/task-environment.md)).

## Layout and ownership

The shared code (`src/saga_exec/`, `testbed/machines.yaml`, `docs/`) belongs to
everyone. Nick Laus owns the Wayline backend (`backends/wayline/`, `task/`,
`experiments/wayline/`, `testbed/k8s/`), and Sebastian Lange owns the Slurm
backend (`backends/slurm/`, `experiments/slurm/`, `testbed/slurm/`). Their
original scheduling code is kept unchanged in `src/saga_exec/legacy/`, and
[docs/README.md](docs/README.md) records where everything was imported from.
