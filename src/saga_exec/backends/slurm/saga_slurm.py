#!/usr/bin/env python3
"""Run a SAGA schedule on the Slurm cluster.

Everything about a run comes from its JSON run file (see RUN_DEFAULTS below and
the examples in experiments/slurm/runs/):

1. Load the instance (graphs/*.json) and the testbed (testbed/machines.yaml).
2. Schedule with SAGA through Wayline's bridge (saga_exec.scheduling): the same
   request, model and SAGA code as Wayline's controller uses in the cluster.
   SAGA sees each machine as C one-task-at-a-time cores "<machine>/c<i>"
   (C = machines.yaml partitions.slurm.saga_nodes_per_machine, or model.cores).
3. Enact SAGA's task -> machine mapping (and, with enact_order "order", each
   machine's start order) as one sbatch job (allocation.py), inside which
   driver.py starts each task as an srun step. Each task is a placeholder
   `sleep` lasting its runtime on that machine (cost * sec_per_cost / speed).
4. Read the real start/end times and compare them with SAGA's prediction.

prediction.json, actual.json, summary.json and gantt.png go to
~/slurm-logs/saga/<run_id>/ on the submitting node (lambda).

Usage:
    uv run saga-slurm experiments/slurm/runs/diamond.json
"""

import json
import sys
import warnings
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List

from saga import ScheduledTask

from saga_exec.backends.slurm.allocation import allocation_files, run_allocation
from saga_exec.backends.slurm.slurm_common import RUNS_DIR
from saga_exec.instance import Instance, load_instance
from saga_exec.scheduling import (
    ENACT_ORDERS,
    FREE_LINK,
    BridgeError,
    ModelConfig,
    SchedulingConfig,
    build_request,
    machine_of,
    prediction_from_response,
    run_bridge,
    runtimes,
    task_inputs,
)
from saga_exec.testbed import Testbed, load_testbed

ROOT = Path(__file__).resolve().parents[4]      # repo root

# Every setting a run file can have, with its default. Keys starting with "_" are comments.
RUN_DEFAULTS = {
    "instance": None,           # graphs/*.json, relative to the repo root (or absolute)
    "scheduler": "heft",        # SAGA algorithm in Wayline's bridge (heft, cpop, peft, ... or a dotted path)
    "enact_order": "order",     # "none": placement only; "order": also each machine's start order
    "sec_per_cost": 1.0,        # instance cost units -> seconds at speed 1.0 (was time_scale)
    "bytes_per_size": 1.0,      # instance size units -> bytes
    "speed": {},                # relative machine speeds, overriding machines.yaml (was "nodes")
    "cpus": 1,                  # whole cores per task = SAGA demand (each core = 2 Slurm CPUs)
    "mem": "1G",                # memory per task (always set: Slurm otherwise gives a job the whole node)
    "options": {},              # SAGA scheduler constructor options
    "model": {},                # SAGA model variant: nodes, cores, links, transfer_latency (ModelConfig)
    "poll": 1.0,                # seconds between status checks
    "dry_run": False,           # print the job without submitting it
}
DEPRECATED = {"strict_order": "enact_order"}
# Old run-file keys that the shared scheduling path replaced (docs/formats.md).
REMOVED = {
    "tasks": 'put the graph in graphs/*.json and set "instance"',
    "dependencies": 'put the graph in graphs/*.json and set "instance"',
    "graph": 'materialize it into graphs/*.json (see graphs/README.md) and set "instance"',
    "time_scale": 'use "sec_per_cost" (it scales costs before scheduling; transfers use real bytes / measured bandwidth)',
    "nodes": 'renamed "speed"',
    "bandwidth": "comes from testbed/machines.yaml (network.links); scale sizes with bytes_per_size",
    "cores": 'use "model": {"cores": C} (default: machines.yaml partitions.slurm.saga_nodes_per_machine)',
}


def load_run_file(path: str) -> SimpleNamespace:
    """Read a run file and fill in the defaults. A typo stops the run instead of being ignored."""
    spec = json.loads(Path(path).read_text())
    spec = {k: v for k, v in spec.items() if not k.startswith("_")}
    for old, new in DEPRECATED.items():
        if old in spec:
            if new in spec:
                sys.exit(f"{path}: give {new!r}, not both {new!r} and {old!r}")
            value = spec.pop(old)
            warnings.warn(f'{path}: "{old}" is deprecated, use "{new}": "order" / "none"',
                          DeprecationWarning, stacklevel=2)
            spec[new] = "order" if value else "none"
    removed = sorted(set(spec) & set(REMOVED))
    if removed:
        sys.exit(f"{path}: settings no longer supported: "
                 + "; ".join(f"{k}: {REMOVED[k]}" for k in removed))
    unknown = sorted(set(spec) - set(RUN_DEFAULTS))
    if unknown:
        sys.exit(f"{path}: unknown settings {unknown}; expected {sorted(RUN_DEFAULTS)}")
    if not spec.get("instance"):
        sys.exit(f'{path}: "instance" (a graphs/*.json path) is required')
    config = SimpleNamespace(**{**RUN_DEFAULTS, **spec}, run_file=str(path), spec=spec)
    if config.enact_order not in ENACT_ORDERS:
        sys.exit(f"{path}: enact_order must be one of {ENACT_ORDERS}, got {config.enact_order!r}")
    try:
        config.model = ModelConfig(**config.model)
    except (TypeError, ValueError, NotImplementedError) as e:
        sys.exit(f"{path}: model: {e}")
    return config


def scheduling_config(config: SimpleNamespace) -> SchedulingConfig:
    return SchedulingConfig(scheduler=config.scheduler, partition="slurm",
                            sec_per_cost=float(config.sec_per_cost), bytes_per_size=float(config.bytes_per_size),
                            speed=dict(config.speed) or None, task_cpu=str(config.cpus),
                            options=dict(config.options), model=config.model)


def instance_path(config: SimpleNamespace) -> Path:
    p = Path(config.instance)
    return p if p.is_absolute() else ROOT / p


# ---------------------------------------------------------------------------
# Prediction -> what allocation.py / driver.py enact
# ---------------------------------------------------------------------------

def link_speed(tb: Testbed, model: dict | None, src: str, dst: str) -> float:
    """What SAGA (the bridge) assumes between two SAGA nodes (cores "<m>/c<i>"):
    self (same core) and intra-machine links from the request's model (1e12 if
    unset), across machines the min of both measured directions."""
    a, b = machine_of(src), machine_of(dst)
    if a != b:
        return min(tb.bandwidth_between(a, b), tb.bandwidth_between(b, a))
    key = "selfBandwidth" if src == dst else "intraBandwidth"
    value = (model or {}).get(key)
    return float(value.get(a, FREE_LINK) if isinstance(value, dict) else (value or FREE_LINK))


def build_plan(inst: Instance, tb: Testbed, sched: SchedulingConfig, prediction: dict,
               enact_order: str) -> Dict[str, dict]:
    """Per task: machine, SAGA's core, start/end, duration on that machine, data
    parents, start_after (previous task in the machine's order, if enacted) and
    simulated transfer seconds from each parent (bytes / SAGA's link speed)."""
    inputs = task_inputs(inst, sched.bytes_per_size)
    rts = runtimes(inst, tb, sched)
    pred = prediction["tasks"]
    prev = {}
    if enact_order == "order":
        for order in prediction["order"].values():
            prev.update({b: [a] for a, b in zip(order, order[1:])})
    model = prediction.get("request", {}).get("model")
    plan = {}
    for name in sorted(pred, key=lambda n: (pred[n]["start"], n)):
        machine = pred[name]["machine"]
        plan[name] = {
            "machine": machine,
            "core": pred[name]["node"],
            "start": pred[name]["start"],
            "end": pred[name]["end"],
            "duration": rts[name][1][machine],
            "waits_for": [p for p, _, _ in inputs[name]],
            "start_after": prev.get(name, []),
            "transfer_from": {p: b / link_speed(tb, model, pred[p]["node"], pred[name]["node"])
                              for p, _, b in inputs[name]},
        }
    return plan


def prepare(run_file: str, *, testbed: Testbed | None = None, **bridge_kw) -> SimpleNamespace:
    """Everything up to submission; never touches the cluster."""
    config = load_run_file(run_file)
    tb = testbed or load_testbed()
    inst = load_instance(instance_path(config))
    sched = scheduling_config(config)
    request = build_request(inst, tb, sched)
    prediction = prediction_from_response(request, run_bridge(request, **bridge_kw), {
        "backend": "slurm", "instance": inst.name, "config": Path(run_file).stem,
        "enact_order": config.enact_order})
    plan = build_plan(inst, tb, sched, prediction, config.enact_order)
    return SimpleNamespace(config=config, testbed=tb, instance=inst, prediction=prediction, plan=plan)


def print_plan(plan: Dict[str, dict]) -> None:
    print(f"{'task':<10}{'SAGA core':<18}{'pred start':>11}{'pred end':>10}  waits for / starts after")
    for name, t in plan.items():
        after = f" | after {', '.join(t['start_after'])}" if t["start_after"] else ""
        print(f"{name:<10}{t['core']:<18}{t['start']:>11.2f}{t['end']:>10.2f}  "
              f"{', '.join(t['waits_for']) or '-'}{after}")
    for task, parents in plan.items():
        for parent, seconds in parents["transfer_from"].items():
            if seconds >= 0.001:
                print(f"  simulated transfer: {parent} to {task} takes {seconds:.2f}s")


# ---------------------------------------------------------------------------
# Gantt chart
# ---------------------------------------------------------------------------

def save_gantt(
    predicted: Dict[str, List[ScheduledTask]],
    actual: Dict[str, List[ScheduledTask]],
    title: str,
    path: Path,
) -> None:
    """Predicted (top) and actual (bottom) schedules on the same time axis."""
    import matplotlib
    matplotlib.use("Agg")                    # no display on the server
    import matplotlib.pyplot as plt
    from saga.utils.draw import draw_gantt

    # draw_gantt reads each node's last task as its finish time, so keep lists sorted.
    for mapping in (predicted, actual):
        for tasks in mapping.values():
            tasks.sort(key=lambda t: t.start)
    xmax = max((t.end for m in (predicted, actual) for ts in m.values() for t in ts), default=1.0)

    fig, (ax_pred, ax_act) = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    draw_gantt(predicted, axis=ax_pred, xmax=xmax, font_size=12, tick_font_size=11)
    draw_gantt(actual, axis=ax_act, xmax=xmax, font_size=12, tick_font_size=11)
    ax_pred.set_title("Predicted (saga)", fontsize=12)
    ax_act.set_title("Actual (Slurm)", fontsize=12)
    ax_pred.set_xlabel("")
    ax_act.set_xlabel("Time (s)", fontsize=12)
    fig.suptitle(title, fontsize=13)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("Usage: uv run saga-slurm experiments/slurm/runs/<name>.json")
    try:
        p = prepare(sys.argv[1])
    except BridgeError as e:
        sys.exit(f"SAGA scheduling failed: {e}")
    config, plan, prediction = p.config, p.plan, p.prediction
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    print(f"Scheduler: {config.scheduler} (SAGA {prediction['meta']['saga_version']}, via Wayline's bridge)   "
          f"tasks: {len(plan)}   predicted makespan: {prediction['makespan']:.2f}s   "
          f"enact_order: {config.enact_order}")
    print_plan(plan)
    print()

    run_dir = RUNS_DIR / run_id
    if config.dry_run:
        _, script = allocation_files(run_id, run_dir, plan, config, p.testbed)
        print(f"# job.sbatch (dry run, nothing submitted; would be written to {run_dir}/):")
        print(script)
        return
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "prediction.json").write_text(json.dumps(prediction, indent=2) + "\n")
    measured = run_allocation(run_id, plan, config, p.testbed)

    # Actual times, relative to the first task's start.
    t0 = min(m["start"] for m in measured.values() if m["start"] is not None)
    rel = lambda v: v - t0 if v is not None else None
    fmt = lambda v: f"{v:.2f}" if v is not None else "-"

    print()
    print(f"{'task':<10}{'node':<14}{'state':<11}{'pred start':>11}{'act start':>10}"
          f"{'pred end':>10}{'act end':>9}{'delay':>8}")
    rows = []
    for name, t in plan.items():
        m = measured[name]
        a_start, a_end = rel(m["start"]), rel(m["end"])
        # delay: how long the task waited after everything it depends on had finished
        # and its data had arrived, so a simulated transfer doesn't count as delay
        parent_ends = [measured[d]["end"] + t["transfer_from"].get(d, 0.0)
                       for d in t["waits_for"] if measured[d]["end"] is not None]
        delay = m["start"] - max(parent_ends) if parent_ends and m["start"] is not None else None
        print(f"{name:<10}{m['node']:<14}{m['state']:<11}{t['start']:>11.2f}{fmt(a_start):>10}"
              f"{t['end']:>10.2f}{fmt(a_end):>9}{fmt(delay):>8}")
        rows.append({
            "task": name, "job_id": m["job_id"], "state": m["state"],
            "node": m["node"], "core": m.get("core"), "predicted_node": t["core"],
            "predicted_start": t["start"], "predicted_end": t["end"],
            "start": a_start, "end": a_end, "delay": delay,
            "waits_for": t["waits_for"], "start_after": t["start_after"],
            "transfer_from": t["transfer_from"],                               # simulated data transfer
        })

    predicted = prediction["makespan"]
    actual = max((r["end"] for r in rows if r["end"] is not None), default=None)
    delays = [r["delay"] for r in rows if r["delay"] is not None]
    print()
    print(f"Predicted makespan: {predicted:.2f}s")
    if actual is not None:
        print(f"Actual makespan:    {actual:.2f}s")
        print(f"Difference:         {actual - predicted:+.2f}s")
    if delays:
        print(f"Delay after dependencies: mean {sum(delays) / len(delays):.2f}s, max {max(delays):.2f}s")

    # actual.json in the shared shape (docs/formats.md); node = most specific location seen.
    actual_record = {
        "run": run_id, "makespan": actual, "t0": t0,
        "tasks": {r["task"]: {"node": f"{r['node']} core{r['core']}" if r["core"] is not None else r["node"],
                              "machine": r["node"], "start": r["start"], "end": r["end"],
                              "state": r["state"], "delay": r["delay"]}
                  for r in rows},
    }
    (run_dir / "actual.json").write_text(json.dumps(actual_record, indent=2) + "\n")
    out_path = run_dir / "summary.json"
    out_path.write_text(json.dumps({
        "run_id": run_id,
        "scheduler": config.scheduler,
        "run_file": config.run_file,
        "run_settings": config.spec,         # the file's contents, in case it's edited later
        "instance": p.instance.to_dict(),
        "enact_order": config.enact_order,
        "predicted_makespan": predicted,
        "makespan": actual,
        "mean_delay": sum(delays) / len(delays) if delays else None,
        "tasks": rows,
    }, indent=2))
    print(f"Saved {out_path}, prediction.json and actual.json")

    # Gantt chart, same time axis for both. Predicted rows are SAGA's cores (cse-isha/c3);
    # actual rows are the real cores Slurm picked (cse-isha core5), so they needn't match.
    predicted_map: Dict[str, List[ScheduledTask]] = {}
    for n, t in plan.items():
        predicted_map.setdefault(t["core"], []).append(
            ScheduledTask(node=t["core"], name=n, start=t["start"], end=t["end"]))
    actual_map: Dict[str, List[ScheduledTask]] = {}
    for r in rows:
        if r["start"] is not None and r["end"] is not None:
            row = f"{r['node']} core{r['core']}" if r["core"] is not None else r["node"]
            actual_map.setdefault(row, []).append(ScheduledTask(
                node=row, name=r["task"], start=r["start"], end=r["end"]))
    gantt_path = run_dir / "gantt.png"
    title = (f"{config.scheduler} on {Path(config.run_file).stem}: predicted {predicted:.1f}s, "
             f"actual {actual:.1f}s" if actual is not None else config.scheduler)
    try:
        save_gantt(predicted_map, actual_map, title, gantt_path)
        print(f"Saved {gantt_path}")
    except Exception as e:                   # a plotting problem shouldn't lose the run
        print(f"Could not draw Gantt chart: {e}")


if __name__ == "__main__":
    main()
