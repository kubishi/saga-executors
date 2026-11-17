#!/usr/bin/env python3
"""Run a saga schedule on the Slurm cluster.

Everything about a run comes from its JSON run file (see RUN_DEFAULTS below and
the examples in runs/):

1. Build a task graph (from the run file's tasks, or one of saga's random shapes).
2. Build a network of the Slurm nodes (cse-lambda1, cse-isha) with relative speeds.
3. Run a saga scheduler (HEFT, CPOP, ...) to get a schedule.
4. Run the schedule as one sbatch job (allocation.py), inside which driver.py starts
   each task as an srun step. Each task is a placeholder `sleep` lasting as long as
   saga predicted for that task on that node.
5. Read the real start/end times and compare them with saga's prediction.

summary.json and gantt.png (predicted vs actual) go to
~/slurm-logs/saga/<run_id>/ on the submitting node (lambda).

Usage:
    uv run saga-slurm experiments/slurm/runs/diamond.json
    (originally: python executor/saga_slurm.py runs/diamond.json)
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List

import numpy as np

from saga import Network, Schedule, ScheduledTask, TaskGraph
import saga.schedulers
from saga.utils import random_graphs
from saga.utils.random_variable import UniformRandomVariable

from saga_exec.backends.slurm.allocation import run_allocation
from saga_exec.backends.slurm.slurm_common import NODES, RUNS_DIR

SUPER_NODES = {"__super_source__", "__super_sink__"}

GRAPHS = {
    "diamond": random_graphs.get_diamond_dag,
    "chain": random_graphs.get_chain_dag,
    "fork": random_graphs.get_fork_dag,
    "branching": random_graphs.get_branching_dag,
}

# Every setting a run file can have, with its default. A run file needs either
# "tasks" (usually with "dependencies") or "graph".
RUN_DEFAULTS = {
    "scheduler": "heft",
    "strict_order": True,       # each task also waits for the previous task on its node
    "time_scale": 10.0,         # seconds of real sleep per unit of saga time
    "nodes": {},                # relative node speeds; any node left out has speed 1.0
    "tasks": None,              # {"A": 5, "B": 3}: task name -> cost
    "dependencies": [],         # [["A", "B"], ["A", "C", 1.0]]: parent, child, optional data size
    "graph": None,              # random graph instead of "tasks"; see GRAPH_DEFAULTS
    "bandwidth": 1e9,           # link speed between nodes
    "cpus": 1,                  # whole cores per task (each core = 2 hardware threads)
    "cores": 14,                # How many cores SAGA sees per machine
    "mem": "1G",                # memory per task (always set: Slurm otherwise gives a job the whole node)
    "poll": 1.0,                # seconds between status checks
    "dry_run": False,           # print the job without submitting it
}
GRAPH_DEFAULTS = {"shape": "diamond", "seed": 0, "min_cost": 0.2, "max_cost": 1.0}


def scheduler_registry() -> Dict[str, type]:
    """Map short names ("heft", "cpop", ...) to saga scheduler classes."""
    registry = {}
    for class_name in saga.schedulers.__all__:
        short = class_name.lower().removesuffix("scheduler")
        registry[short] = getattr(saga.schedulers, class_name)
    return registry


# ---------------------------------------------------------------------------
# Building the problem
# ---------------------------------------------------------------------------

def load_run_file(path: str) -> SimpleNamespace:
    # TODO(shared): run files mix the instance graph with execution settings; the
    # graph part should become a shared instance file (saga_exec.instance, graphs/).
    """Read a run file and fill in the defaults. A typo stops the run instead of being ignored."""
    spec = json.loads(Path(path).read_text())
    unknown = sorted(set(spec) - set(RUN_DEFAULTS))
    if unknown:
        sys.exit(f"{path}: unknown settings {unknown}; expected {sorted(RUN_DEFAULTS)}")
    if ("tasks" in spec) == ("graph" in spec):
        sys.exit(f'{path}: give either "tasks" or "graph", not both or neither')

    config = SimpleNamespace(**{**RUN_DEFAULTS, **spec}, run_file=path, spec=spec)
    if config.graph is not None:
        unknown = sorted(set(config.graph) - set(GRAPH_DEFAULTS))
        if unknown:
            sys.exit(f"{path}: unknown graph settings {unknown}; expected {sorted(GRAPH_DEFAULTS)}")
        config.graph = {**GRAPH_DEFAULTS, **config.graph}
        if config.graph["shape"] not in GRAPHS:
            sys.exit(f"{path}: unknown graph shape {config.graph['shape']!r}; expected one of {sorted(GRAPHS)}")
    return config


def build_task_graph(config: SimpleNamespace) -> TaskGraph:
    """Task costs are in "seconds on a speed-1.0 node".

       If the run file has "tasks" and "dependencies", use those costs. Otherwise, generate a random graph with costs in [min_cost, max_cost] and the given shape.
    
    """
    if config.tasks is not None:
        tasks = [(name, float(cost)) for name, cost in config.tasks.items()]
        deps = [tuple(d) for d in config.dependencies]
        return TaskGraph.create(tasks=tasks, dependencies=deps)

    np.random.seed(config.graph["seed"])
    # Generates a random cost of [min_cost, max_cost] for each task generated by slurm not the json files, then builds the graph with those costs.
    dist = UniformRandomVariable(config.graph["min_cost"], config.graph["max_cost"])
    return GRAPHS[config.graph["shape"]](weight_distribution=dist)


def build_network(config: SimpleNamespace) -> Network:
    # TODO(shared): replace with saga_exec.network.build_core_network(...) driven by
    # testbed/machines.yaml. Differences to resolve first (docs/formats.md): core names
    # "<m>:c<i>" vs Wayline's "<m>-c<i>", local bandwidth inf vs 1e12, one bandwidth
    # for all cross-machine links vs measured per direction.
    """Build a network of the Slurm nodes with relative speeds and link bandwidths.

       The run file can give a speed for each node, and a bandwidth for all links.
       Any node left out has speed 1.0. Links default to effectively free transfers.

       Output Example: {"cse-lambda1": 1.0, "cse-isha": 0.5} means lambda is twice as fast as isha.
    """
    speeds = {node: 1.0 for node in NODES}

    for node, value in config.nodes.items():
        if node not in speeds:
            sys.exit(f"Unknown node {node!r}; expected one of {NODES}")
        speeds[node] = float(value)

    core_speeds = {}
    core_names = []
    for node, speed in speeds.items():
        for c in range(0, config.cores):
            core_name = f"{node}:c{c}"
            core_speeds[core_name] = speed

            core_names.append(core_name)



    
    nodes = [(name, speed) for name, speed in core_speeds.items()]


    # Links default to effectively free transfers; lower "bandwidth" to simulate slower ones.
    edges = [(a, b, config.bandwidth)  if a.split(":")[0] != b.split(":")[0] else (a, b, float("inf")) for i, a in enumerate(core_names) for b in core_names[i + 1:] ]
    edges += [(n, n, float("inf")) for n in core_names]
    return Network.create(nodes=nodes, edges=edges)


# ---------------------------------------------------------------------------
# Reading the schedule
# ---------------------------------------------------------------------------

def real_tasks(schedule: Schedule) -> Dict[str, ScheduledTask]:
    """All scheduled tasks except saga's zero-cost super source/sink.

       Example layout: {"A": ScheduledTask(...), "C": ScheduledTask(...), "D": ScheduledTask(...), "B": ScheduledTask(...)}
    """
    result = {}
    for tasks in schedule.mapping.values():   # each machine's list
        for t in tasks:                       # each task in that list
            if t.name not in SUPER_NODES:     # skip saga's placeholder tasks
                result[t.name] = t            # add the task name to the result
    return result


def job_dependencies(task_graph: TaskGraph, schedule: Schedule, strict_order: bool) -> Dict[str, List[str]]:
    """For each task, the tasks that must finish before it starts.

       # in deps: task, what it waits for
       {"A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]}  
       In this case, B and C both depend on A, and D depends on both B and C.
    
    """
    scheduled = real_tasks(schedule)
    deps = {name: [] for name in scheduled}
    for edge in task_graph.dependencies:
        if edge.source in scheduled and edge.target in scheduled:
            deps[edge.target].append(edge.source)
    if strict_order:
        # Also keep saga's order on each node: wait for the previous task there.
        for tasks in schedule.mapping.values():
            ordered = sorted((t for t in tasks if t.name not in SUPER_NODES), key=lambda t: t.start)
            for prev, cur in zip(ordered, ordered[1:]):
                if prev.name not in deps[cur.name]:
                    deps[cur.name].append(prev.name)
    return deps


# ---------------------------------------------------------------------------
# Simulated data transfer
#
# No data actually moves. saga predicts each transfer takes data size / link speed,
# so we work out those times here, and driver.py holds each task until its data
# would have arrived. Uses the task graph rather than job_dependencies, because
# strict_order waits aren't data transfers.
# ---------------------------------------------------------------------------

def transfer_times(task_graph: TaskGraph, network: Network, tasks: Dict[str, ScheduledTask],
                   time_scale: float) -> Dict[str, Dict[str, float]]:
    """For each task, the seconds its data takes to arrive from each parent.

       Example: {"A": {}, "B": {"A": 0.0}, "C": {"A": 3.0}, "D": {"B": 0.0, "C": 0.0}}
       A parent on the same machine gives 0, because that link has infinite speed.
    """
    transfers = {name: {} for name in tasks}
    for edge in task_graph.dependencies:
        if edge.source in tasks and edge.target in tasks:
            link = network.get_edge(tasks[edge.source].node, tasks[edge.target].node)
            transfers[edge.target][edge.source] = edge.size / link.speed * time_scale
    return transfers


def print_transfers(transfers: Dict[str, Dict[str, float]]) -> None:
    """List the transfers long enough to notice."""
    for task, parents in transfers.items():
        for parent, seconds in parents.items():
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
    config = load_run_file(sys.argv[1])

    schedulers = scheduler_registry()
    if config.scheduler not in schedulers:
        sys.exit(f"Unknown scheduler {config.scheduler!r}; expected one of {sorted(schedulers)}")

    task_graph = build_task_graph(config)
    network = build_network(config)
    scheduler = schedulers[config.scheduler]()
    schedule = scheduler.schedule(network, task_graph) # holds the list of tasks with predicted start and end
    print("Label: task graph cool stuff",task_graph.dependencies)

    tasks = real_tasks(schedule)
    deps = job_dependencies(task_graph, schedule, config.strict_order)
    transfers = transfer_times(task_graph, network, tasks, config.time_scale)   # simulated data transfer
    order = sorted(tasks, key=lambda name: (tasks[name].start, name))   # the order saga starts them
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    print(f"Scheduler: {scheduler.name}   tasks: {len(tasks)}   "
          f"predicted makespan: {schedule.makespan * config.time_scale:.2f}s")
    print(f"{'task':<10}{'node':<14}{'pred start':>11}{'pred end':>10}  waits for")
    for name in order:
        t = tasks[name]
        print(f"{name:<10}{t.node:<14}{t.start * config.time_scale:>11.2f}"
              f"{t.end * config.time_scale:>10.2f}  {', '.join(deps[name]) or '-'}")
    print_transfers(transfers)                                                  # simulated data transfer
    print()

    measured = run_allocation(run_id, tasks, deps, transfers, config)
    if measured is None:          # dry run
        return

    # Actual times, relative to the first task's start.
    t0 = min(m["start"] for m in measured.values() if m["start"] is not None)
    rel = lambda v: v - t0 if v is not None else None

    print()
    print(f"{'task':<10}{'node':<14}{'state':<11}{'pred start':>11}{'act start':>10}"
          f"{'pred end':>10}{'act end':>9}{'delay':>8}")
    rows = []
    for name in order:
        t, m = tasks[name], measured[name]
        a_start, a_end = rel(m["start"]), rel(m["end"])
        # delay: how long the task waited after everything it depends on had finished
        # and its data had arrived, so a simulated transfer doesn't count as delay
        parent_ends = [measured[d]["end"] + transfers[name].get(d, 0.0)
                       for d in deps[name] if measured[d]["end"] is not None]
        delay = m["start"] - max(parent_ends) if parent_ends and m["start"] is not None else None
        fmt = lambda v: f"{v:.2f}" if v is not None else "-"
        print(f"{name:<10}{m['node']:<14}{m['state']:<11}{t.start * config.time_scale:>11.2f}{fmt(a_start):>10}"
              f"{t.end * config.time_scale:>10.2f}{fmt(a_end):>9}{fmt(delay):>8}")
        rows.append({
            "task": name, "job_id": m["job_id"], "state": m["state"],
            "node": m["node"], "core": m.get("core"), "predicted_node": t.node,
            "predicted_start": t.start * config.time_scale, "predicted_end": t.end * config.time_scale,
            "start": a_start, "end": a_end, "delay": delay,
            "waits_for": deps[name],
            "transfer_from": transfers[name],                                   # simulated data transfer
        })

    predicted = schedule.makespan * config.time_scale
    actual = max((r["end"] for r in rows if r["end"] is not None), default=None)
    delays = [r["delay"] for r in rows if r["delay"] is not None]
    print()
    print(f"Predicted makespan: {predicted:.2f}s")
    if actual is not None:
        print(f"Actual makespan:    {actual:.2f}s")
        print(f"Difference:         {actual - predicted:+.2f}s")
    if delays:
        print(f"Delay after dependencies: mean {sum(delays) / len(delays):.2f}s, max {max(delays):.2f}s")

    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "summary.json"
    out_path.write_text(json.dumps({
        "run_id": run_id,
        "scheduler": scheduler.name,
        "run_file": config.run_file,
        "run_settings": config.spec,         # the file's contents, in case it's edited later
        "speeds": {n.name: n.speed for n in network.nodes},
        "time_scale": config.time_scale,
        "strict_order": config.strict_order,
        "predicted_makespan": predicted,
        "makespan": actual,
        "mean_delay": sum(delays) / len(delays) if delays else None,
        "tasks": rows,
    }, indent=2))
    print(f"Saved {out_path}")

    # Gantt chart, same time axis for both. Predicted rows are saga's cores (cse-lambda1:c3);
    # actual rows are the real cores Slurm picked (cse-lambda1 core5), so they needn't match
    # row for row. draw_gantt names each row by the task's node=, so key and node= must agree.
    predicted_map: Dict[str, List[ScheduledTask]] = {}
    actual_map: Dict[str, List[ScheduledTask]] = {}
    for r in rows:
        predicted_map.setdefault(r["predicted_node"], []).append(ScheduledTask(
            node=r["predicted_node"], name=r["task"],
            start=r["predicted_start"], end=r["predicted_end"]))
        if r["start"] is not None and r["end"] is not None:
            row = f"{r['node']} core{r['core']}" if r["core"] is not None else r["node"]
            actual_map.setdefault(row, []).append(ScheduledTask(
                node=row, name=r["task"], start=r["start"], end=r["end"]))
    gantt_path = run_dir / "gantt.png"
    title = (f"{scheduler.name} on {Path(config.run_file).stem}: predicted {predicted:.1f}s, "
             f"actual {actual:.1f}s" if actual is not None else scheduler.name)
    try:
        save_gantt(predicted_map, actual_map, title, gantt_path)
        print(f"Saved {gantt_path}")
    except Exception as e:                   # a plotting problem shouldn't lose the run
        print(f"Could not draw Gantt chart: {e}")


if __name__ == "__main__":
    main()
