<!-- Nick Laus's original README for eval/experiments/fidelity/ (Wayline fork, branch
     lab @ aff6fa8). Kept verbatim below; in saga-executors the paths map as:
       convert.py, run.py, analyze.py -> src/saga_exec/backends/wayline/
       generate.py                    -> experiments/wayline/generate.py
       graphs/                        -> graphs/ (repo root)
       task/                          -> task/ (repo root; see its Dockerfile for the new build command)
       out/                           -> results/wayline/ (gitignored; examples in results/examples/wayline/)
     Commands:
       uv run python experiments/wayline/generate.py
       uv run saga-wayline-run results/wayline/<graph>/<run name> --reps 20 --shuffle 1
       uv run saga-wayline-analyze results/wayline/<graph>/<run name>/runs/<odag>/status.json
     The plan referenced below (fidelity-plan/main.tex) was not found on this machine;
     see docs/README.md.
     CHANGED SINCE: SAGA now runs in the cluster through Wayline's saga-sidecar
     (spec.scheduler saga/<alg>, enactOrder from Config.enact_order, nothing pinned;
     schedulerConfig.model asks the bridge for C core-nodes "<machine>/c<i>" per
     machine, as before, plus explicit self/intra-machine link speeds);
     convert.py writes the template, the wl-network-profile ConfigMap, the bridge
     request and an offline bridge prediction; analyze.py reads SAGA's prediction
     from status.scheduling and rejects runs that fell back to random placement.
     Config.C is now Config.model.cores (default: testbed/machines.yaml). The converter described
     below is src/saga_exec/legacy/wayline_convert.py. PYTHONHASHSEED is set by the
     bridge runner. See docs/formats.md.
     task/task.py is now split into a backend-agnostic core (task/fidelity_core.py)
     and transports (task/transport_wayline.py here; the Slurm backend runs the same
     core). Behaviour on Wayline is unchanged; the env contract below is a subset of
     docs/data-movement.md (FID_TRANSPORT defaults to wayline). The image now COPYs
     task/*.py. -->

# Fidelity: how well does SAGA's model predict Wayline?

Plan: `../../../../fidelity-plan/main.tex` (outside the repo).

## Layout

```
fidelity/
├── task/              dummy task image (one image for every task)
│   ├── Dockerfile
│   └── task.py
├── convert.py         one instance + one Config -> SAGA schedule + ODAGTemplate
├── generate.py        edit this: lists the convert() calls you want
├── run.py             runs templates on Wayline and collects each run (lambda1)
├── analyze.py         one run -> actual.json + gantt.png (run.py calls it)
├── graphs/
│   └── smoke.json     4-task diamond for end-to-end testing
└── out/               generated; one folder per graph and Config:
    ├── run-log.csv    one line per run (run.py)
    └── <graph name>/<run name>/
        ├── template.yml     apply this
        ├── prediction.json  SAGA's makespan + per-task node/start/end
        ├── instance.json    the exact input
        ├── config.json      the exact settings
        └── runs/<odag name>/
            ├── status.json  the run as Wayline recorded it
            ├── logs.txt     every task's log
            ├── actual.json  same shape as prediction.json, measured
            └── gantt.png    SAGA's schedule above Wayline's
```

## Instance format (`graphs/*.json`)

```json
{
  "name": "smoke-diamond",
  "source": "hand-written",
  "tasks": {"a": 2.0, "b": 4.0},
  "edges": [["a", "b", 1.0]]
}
```

`tasks`: task -> cost (cost units). `edges`: `[producer, consumer, size]` (size units).

`convert.py` turns units into seconds and bytes with `Config.sec_per_cost` and
`Config.bytes_per_size`.

Task names must be valid Kubernetes names (lowercase letters, digits, hyphens),
since pods and containers are named after them. Graph names must be unique
across datasets, since template names include them but not the output folder.

## Execution model (see plan)

- Each machine = C identical cores; every task uses exactly 1 core.
- SAGA side: each machine becomes C SAGA nodes (`<machine>-c0..c<C-1>`),
  free links between cores of one machine, measured bandwidth across machines.
- Wayline side: `cpu: "1"` and `constraints.nodeNames: [<machine>]` per task,
  `schedulerConfig.nodeOrder` (per machine, sorted by SAGA's predicted start),
  `enactOrder: order`.

## Task env contract (`task/task.py`)

| Var | Example | Meaning |
|---|---|---|
| `FID_MODE` | `sleep` | `sleep` or `cpu` |
| `FID_SECONDS` | `4.0` | runtime at speed 1.0 (already scaled) |
| `FID_SPEED` | `cse-lambda1=1.0,cse-isha=0.5` | speed per node. Sleep mode applies it; cpu mode only logs the planned time from it |
| `FID_HASH_RATE` | `2500000` | cpu mode: hashes/s of the reference machine (from calibration) |
| `FID_INPUTS` | `a.to-b` | `producer.object`, comma-separated |
| `FID_OUTPUTS` | `to-c:1000000` | `object:bytes`, comma-separated |

## Workflow

```bash
# laptop: edit the convert() calls in generate.py, then schedule + generate
PYTHONHASHSEED=0 python generate.py

# lambda1: build image (repo root) when task.py changed
sudo docker build -f eval/experiments/fidelity/task/Dockerfile \
  -t localhost:5000/wl-fidelity-task:latest .
sudo docker push localhost:5000/wl-fidelity-task:latest

# lambda1, from fidelity/ with the venv active: run and collect, one at a time
python run.py out/<graph name>/<run name> [more folders] --reps 20 --shuffle 1
```

For each run, `run.py` applies the template, starts the run, waits for it,
saves `status.json` and `logs.txt`, deletes the run (Wayline does not clean up
when profiling is off), then calls `analyze.py`. It stops if another run is
still active in the namespace. A run that fails or times out is saved and
logged, not analyzed. To analyze a saved run again:
`python analyze.py out/<graph>/<run name>/runs/<odag>/status.json`.

Measured makespan = first task start -> last task close (SDK times), not
Wayline's `status.makespan` (whole seconds, from the first pod).
