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
   driver.py starts each task as an srun step. Each step runs the same task
   program as the Wayline backend (task/task.py: read inputs, sleep or hash
   for the task's runtime, write random-byte outputs). A task starts when its
   producers' steps have completed, and its data really moves, by the run's
   "transport": "direct" (default: node-local scratch, consumers pull
   cross-machine inputs from a per-machine data server) or "shared_fs" (files
   staged in a shared work dir such as NFS). See docs/data-movement.md.
4. Read the real start/end times and phases and compare them with SAGA's prediction.

prediction.json, actual.json, summary.json and gantt.png go to
~/slurm-logs/saga/<run_id>/ on the submitting node (lambda).

Usage:
    uv run saga-slurm experiments/slurm/runs/diamond.json
"""

import json
import re
import shlex
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
    machine_speeds,
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
    "mode": "sleep",            # task work: "sleep" (runtime / speed) or "cpu" (runtime * hash_rate sha256s)
    "hash_rate": None,          # cpu mode: sha256/s of a speed-1.0 slot; None = machines.yaml calibration.cpu.slurm
    "task_python": "$HOME/.local/bin/python3.12",   # interpreter for tasks on every machine (uv-managed)
    "transport": "direct",      # "direct": node-local scratch + pull from per-machine data servers;
                                # "shared_fs": files in a shared work dir (NFS). An experimental variable.
    "workdir": None,            # shared_fs: shared work dir, same path on every machine; None = machines.yaml
                                # partitions.slurm.shared_workdir. Run data go to <workdir>/<user>/<run_id>/
    "latency_wait": 5.0,        # shared_fs: seconds a task retries opening an input not yet visible (NFS caching)
    "keep_data": False,         # shared_fs: keep <workdir>/<user>/<run_id>/ after the run
    "scratch": None,            # direct: node-local scratch root; None = /tmp/saga-exec-<user>
    "server_port": 0,           # direct: data servers' TCP port; 0 = any free port (each reports its own)
    "server_bind": {},          # direct: {machine: address} to bind to; default: the interface towards the others
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
    if config.mode not in ("sleep", "cpu"):
        sys.exit(f'{path}: mode must be "sleep" or "cpu", got {config.mode!r}')
    if config.transport not in ("direct", "shared_fs"):
        sys.exit(f'{path}: transport must be "direct" or "shared_fs", got {config.transport!r}')
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


# Task and object names become file names and data-server URL segments (task/transport_direct.py).
NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_+:-]{0,100}$")


def build_plan(inst: Instance, tb: Testbed, sched: SchedulingConfig, prediction: dict,
               enact_order: str) -> Dict[str, dict]:
    """Per task: machine, SAGA's core, predicted start/end, runtime at speed 1.0
    (seconds, = FID_SECONDS) and on its machine (duration), inputs and outputs
    (one object per edge, with bytes and the consumer's machine), data parents,
    start_after (previous task in the machine's order, if enacted) and SAGA's
    predicted transfer seconds from each parent (bytes / SAGA's link speed;
    reporting only, the transfers are real)."""
    inputs = task_inputs(inst, sched.bytes_per_size)
    rts = runtimes(inst, tb, sched)
    pred = prediction["tasks"]
    bad = sorted(n for n in pred if not NAME_RE.match(n))
    if bad:
        raise ValueError(f"task names {bad} must match {NAME_RE.pattern} (they become paths and URLs)")
    prev = {}
    if enact_order == "order":
        for order in prediction["order"].values():
            prev.update({b: [a] for a, b in zip(order, order[1:])})
    model = prediction.get("request", {}).get("model")
    outputs: Dict[str, list] = {n: [] for n in pred}
    for consumer, ins in inputs.items():
        for p, obj, b in ins:
            outputs[p].append({"object": obj, "bytes": b, "consumer": consumer,
                               "consumer_node": pred[consumer]["machine"]})
    plan = {}
    for name in sorted(pred, key=lambda n: (pred[n]["start"], n)):
        machine = pred[name]["machine"]
        plan[name] = {
            "machine": machine,
            "core": pred[name]["node"],
            "start": pred[name]["start"],
            "end": pred[name]["end"],
            "seconds": rts[name][0],
            "duration": rts[name][1][machine],
            "inputs": [{"producer": p, "object": o, "bytes": b} for p, o, b in inputs[name]],
            "outputs": outputs[name],
            "waits_for": [p for p, _, _ in inputs[name]],
            "start_after": prev.get(name, []),
            "predicted_transfer": {p: b / link_speed(tb, model, pred[p]["node"], pred[name]["node"])
                                   for p, _, b in inputs[name]},
        }
    return plan


def manual_plan(tasks: Dict[str, tuple], edges: List[tuple], enact_order: Dict[str, List[str]] | None = None,
                speed: Dict[str, float] | None = None) -> Dict[str, dict]:
    """A plan in build_plan's format without SAGA: tasks {name: (machine, seconds at
    speed 1.0)}, edges [(producer, consumer, bytes)], optional per-machine start
    order. For calibration (saga_exec.calibrate.links) and tests."""
    speed = speed or {}
    prev = {b: [a] for order in (enact_order or {}).values() for a, b in zip(order, order[1:])}
    plan = {}
    for name, (machine, seconds) in tasks.items():
        ins = [(p, b) for p, c, b in edges if c == name]
        plan[name] = {
            "machine": machine, "core": f"{machine}/c0", "start": 0.0, "end": seconds,
            "seconds": seconds, "duration": seconds / speed.get(machine, 1.0),
            "inputs": [{"producer": p, "object": f"to-{name}", "bytes": b} for p, b in ins],
            "outputs": [{"object": f"to-{c}", "bytes": b, "consumer": c, "consumer_node": tasks[c][0]}
                        for p, c, b in edges if p == name],
            "waits_for": [p for p, _ in ins], "start_after": prev.get(name, []),
            "predicted_transfer": {p: 0.0 for p, _ in ins},
        }
    return plan


def check_memory(plan: Dict[str, dict], mem: str) -> List[str]:
    """Tasks whose payloads may not fit their --mem: the task holds an input or
    output in memory (plus a copy while sending), as on Wayline (convert.py
    sizes pods 512 MiB + 2x bytes held). Returns warnings."""
    from saga_exec.backends.slurm.allocation import mem_mb
    limit = mem_mb(mem) * 2**20
    out = []
    for name, t in plan.items():
        held = max([i["bytes"] for i in t["inputs"]] + [o["bytes"] for o in t["outputs"]] + [0])
        need = 512 * 2**20 + 2 * held
        if need > limit:
            out.append(f"task {name} moves {held / 2**20:.0f} MiB at once; mem {mem} may be too small "
                       f"(about {need / 2**30:.1f}G suggested)")
    return out


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
    config.machine_speed = machine_speeds(tb, sched)
    if config.transport == "shared_fs":
        config.workdir = config.workdir or tb.partition("slurm").raw.get("shared_workdir")
        if not config.workdir:
            sys.exit(f"{run_file}: transport shared_fs needs a shared work dir: \"workdir\" in the run file or "
                     "machines.yaml partitions.slurm.shared_workdir (testbed/slurm/nfs.md)")
    if config.hash_rate is None:
        config.hash_rate = tb.hash_rate("slurm")
    if config.mode == "cpu" and not config.hash_rate:
        sys.exit(f"{run_file}: mode cpu needs hash_rate (run file, or machines.yaml calibration.cpu.slurm "
                 "from saga-calibrate-cpu)")
    for w in check_memory(plan, config.mem):
        warnings.warn(w, stacklevel=2)
    return SimpleNamespace(config=config, testbed=tb, instance=inst, prediction=prediction, plan=plan)


def print_plan(plan: Dict[str, dict]) -> None:
    print(f"{'task':<10}{'SAGA core':<18}{'pred start':>11}{'pred end':>10}  waits for / starts after")
    for name, t in plan.items():
        after = f" | after {', '.join(t['start_after'])}" if t["start_after"] else ""
        print(f"{name:<10}{t['core']:<18}{t['start']:>11.2f}{t['end']:>10.2f}  "
              f"{', '.join(t['waits_for']) or '-'}{after}")
    for task, t in plan.items():
        for i in t["inputs"]:
            seconds = t["predicted_transfer"][i["producer"]]
            if seconds >= 0.001:
                where = "across machines" if plan[i["producer"]]["machine"] != t["machine"] else "same machine"
                print(f"  data: {i['producer']} -> {task} {i['bytes']} B ({where}); SAGA assumes {seconds:.2f}s")


def summarize(run_id: str, plan: Dict[str, dict], measured: Dict[str, dict]) -> tuple:
    """Measured times (epoch, per node clock) -> summary rows and actual.json
    (docs/formats.md), relative to the first task's start. start/end are the
    task's own taskStartUnix/closeUnix (as Wayline's taskStartTime/taskCloseTime);
    delay = start - inputs_ready, where inputs_ready is when its last input became
    available (the last producer's close, on the producer's clock): driver
    reaction + srun step launch + interpreter start-up."""
    starts = [m["start"] for m in measured.values() if m.get("start") is not None]
    t0 = min(starts) if starts else None
    rel = lambda v: v - t0 if v is not None and t0 is not None else None
    rows = []
    for name, t in plan.items():
        m = measured[name]
        ready = m.get("inputs_ready")
        delay = m["start"] - ready if ready is not None and m.get("start") is not None else None
        rows.append({
            "task": name, "job_id": m.get("job_id"), "state": m["state"],
            "node": m["node"], "core": m.get("core"), "predicted_node": t["core"],
            "predicted_start": t["start"], "predicted_end": t["end"],
            "start": rel(m.get("start")), "end": rel(m.get("end")),
            "inputs_ready": rel(ready), "delay": delay,
            "input_read": m.get("input_read"), "compute": m.get("compute"), "handoff": m.get("handoff"),
            "waits_for": t["waits_for"], "start_after": t["start_after"],
            "predicted_transfer": t["predicted_transfer"],                    # what SAGA assumed
        })
    ends = [r["end"] for r in rows if r["end"] is not None]
    actual = {
        "run": run_id, "makespan": max(ends) if ends else None, "t0": t0,
        "tasks": {r["task"]: {"node": f"{r['node']} core{r['core']}" if r["core"] is not None else r["node"],
                              "machine": r["node"], "start": r["start"], "end": r["end"],
                              "input_read": r["input_read"], "compute": r["compute"], "handoff": r["handoff"],
                              "inputs_ready": r["inputs_ready"], "state": r["state"], "delay": r["delay"]}
                  for r in rows},
    }
    return rows, actual


def print_dry_run(plan_json: dict, script: str, run_dir: Path) -> None:
    """What the job would do: job.sbatch, then the driver's setup and task steps."""
    from saga_exec.backends.slurm import driver
    print(f"# job.sbatch (dry run, nothing submitted; would be written to {run_dir}/):")
    print(script)
    print(f"# inside the job, driver.py would run (transport {plan_json['transport']}):")
    servers = {}
    if driver.is_direct(plan_json):
        print(" ".join(driver.sbcast_command(plan_json)))
        for m in plan_json["machines"]:
            print(" ".join(shlex.quote(c) for c in driver.server_command(plan_json, m)) + " &")
        servers = {m: {"host": f"<{m} server>", "port": 0} for m in plan_json["machines"]}
    else:
        print(f"mkdir -p {shlex.quote(driver.shared_data_dir(plan_json))} && "
              f"cp {shlex.quote(plan_json['pyz'])} {shlex.quote(driver.remote_pyz(plan_json))}")
        print(" ".join(shlex.quote(c) for c in driver.visibility_check_command(plan_json)))
    tasks = {t["name"]: t for t in plan_json["tasks"]}
    for t in plan_json["tasks"]:
        env = driver.task_env(t, plan_json, tasks, servers, "<token>")
        producers = sorted({i["producer"] for i in t["inputs"]})
        gate = f"after {', '.join(producers)} completed" if producers else "no inputs"
        after = f"; after {', '.join(t['start_after'])} started" if t["start_after"] else ""
        print(f"# {t['name']} on {t['node']}: {gate}{after}")
        print(" ".join(f"{k}={shlex.quote(v)}" for k, v in env.items() if k != "FID_TOKEN") + " \\\n  "
              + " ".join(shlex.quote(c) for c in driver.step_command(t, plan_json)))


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
        plan_json, script = allocation_files(run_id, run_dir, plan, config, p.testbed)
        print_dry_run(plan_json, script, run_dir)
        return
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "prediction.json").write_text(json.dumps(prediction, indent=2) + "\n")
    measured = run_allocation(run_id, plan, config, p.testbed)

    rows, actual_record = summarize(run_id, plan, measured)
    results_path = run_dir / "results.json"
    transfers = json.loads(results_path.read_text()).get("transfers", []) if results_path.exists() else []
    fmt = lambda v: f"{v:.2f}" if v is not None else "-"

    print()
    print(f"{'task':<10}{'node':<14}{'state':<11}{'pred start':>11}{'act start':>10}"
          f"{'pred end':>10}{'act end':>9}{'read':>7}{'compute':>8}{'handoff':>8}{'delay':>7}")
    for r in rows:
        print(f"{r['task']:<10}{r['node']:<14}{r['state']:<11}{r['predicted_start']:>11.2f}{fmt(r['start']):>10}"
              f"{r['predicted_end']:>10.2f}{fmt(r['end']):>9}{fmt(r['input_read']):>7}{fmt(r['compute']):>8}"
              f"{fmt(r['handoff']):>8}{fmt(r['delay']):>7}")
    for f in transfers:
        print(f"  served {f['object']} from {f['src_node']} to {f['to']}: {f['bytes']} B in "
              f"{f['end'] - f['start']:.2f}s")

    predicted = prediction["makespan"]
    actual = actual_record["makespan"]
    delays = [r["delay"] for r in rows if r["delay"] is not None]
    print()
    print(f"Predicted makespan: {predicted:.2f}s")
    if actual is not None:
        print(f"Actual makespan:    {actual:.2f}s")
        print(f"Difference:         {actual - predicted:+.2f}s")
    if delays:
        print(f"Delay after inputs ready: mean {sum(delays) / len(delays):.2f}s, max {max(delays):.2f}s")

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
        "mode": config.mode,
        "hash_rate": config.hash_rate,
        "tasks": rows,
        "transport": config.transport,
        "transfers": transfers,              # direct: cross-machine pulls, as the data servers saw them
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
