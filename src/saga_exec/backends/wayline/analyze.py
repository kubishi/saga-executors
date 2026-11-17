#!/usr/bin/env python3
"""Wayline run -> SAGA's in-cluster prediction + actual.json + Gantt chart.

Input: a run's saved status (kubectl get odag <run> -n wl-runs -o json), stored
as <out>/<graph>/<run name>/runs/<odag name>/status.json. Writes in that folder:

  prediction.json   SAGA's prediction for THIS run, as the controller recorded it
                    (status.scheduling.externalAssignments; needs Wayline's
                    saga-executor branch), compared with the offline one
  actual.json       mirrors prediction.json, from Wayline's measured times
  gantt.png         SAGA's predicted schedule above Wayline's actual one

A run is rejected (RejectedRun, nothing written) when SAGA did not place it:
the controller falls back to random placement if the sidecar fails and
records that as status.scheduling.used != requested (+ fallbackError).

Usage: uv run saga-wayline-analyze results/wayline/<graph>/<run name>/runs/<odag name>/status.json [...]

Times come from the SDK (taskStartTime / taskCloseTime, sub-millisecond), and
time 0 is the first task's start, to match SAGA's time 0. status.makespan is not
used: it is whole seconds and starts at the first pod.
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")   # write image files; no window
import matplotlib.pyplot as plt
from saga import ScheduledTask
from saga.utils.draw import draw_gantt

from saga_exec.scheduling import prediction_from_assignments, same_schedule


class RejectedRun(ValueError):
    """The run was not placed by the SAGA scheduler it asked for (or we cannot tell)."""


def parse_time(s: str) -> float:
    """RFC 3339 ("2026-11-05T06:38:58.09465146Z") -> Unix seconds."""
    return datetime.fromisoformat(s).timestamp()


def actual_from_status(status: dict) -> dict:
    """Wayline's measured schedule, in the same shape as prediction.json.

    Wayline only knows which machine ran a task, not which core, so "node" is
    the machine here (in prediction.json it is SAGA's core).
    """
    if status["status"].get("phase") != "Succeeded":
        raise ValueError(f"{status['metadata']['name']} did not succeed")
    tasks = status["status"]["tasks"]
    missing = [t["name"] for t in tasks if "taskStartTime" not in t or "taskCloseTime" not in t]
    if missing:
        raise ValueError(f"no SDK timestamps for tasks {missing}")

    starts = {t["name"]: parse_time(t["taskStartTime"]) for t in tasks}
    closes = {t["name"]: parse_time(t["taskCloseTime"]) for t in tasks}
    t0 = min(starts.values())
    first = min(tasks, key=lambda t: starts[t["name"]])

    return {
        "run": status["metadata"]["name"],
        "makespan": max(closes.values()) - t0,
        "makespan_from_odag_start": max(closes.values()) - parse_time(status["status"]["startTime"]),
        "t0": first["taskStartTime"],
        "tasks": {
            t["name"]: {
                "node": t["node"],
                "machine": t["node"],
                "start": starts[t["name"]] - t0,
                "end": closes[t["name"]] - t0,
                "input_read": t.get("inputReadSeconds"),
                "compute": t.get("computeSeconds"),
                "handoff": t.get("handoffSeconds"),
                # last input installed on its node (controller's inputsReadyTime; sources: None)
                "inputs_ready": (parse_time(t["inputsReadyTime"]) - t0) if t.get("inputsReadyTime") else None,
            }
            for t in sorted(tasks, key=lambda t: (starts[t["name"]], t["name"]))
        },
    }


def prediction_from_status(status: dict) -> dict:
    """SAGA's prediction for this run, from status.scheduling (see wayline
    cmd/odag-controller/saga.go, schedulePlan.statusFields). Same shape as the
    offline prediction.json. Raises RejectedRun if SAGA did not place the run."""
    name = status["metadata"]["name"]
    sch = status.get("status", {}).get("scheduling") or {}
    requested, used = sch.get("requested", ""), sch.get("used", "")
    if not requested.startswith("saga/"):
        raise RejectedRun(f"{name}: scheduler {requested!r} is not a SAGA scheduler")
    if used != requested:
        raise RejectedRun(f"{name}: placed by {used!r}, not {requested!r} "
                          f"(fallback: {sch.get('fallbackError', 'no error recorded')})")
    if "externalAssignments" not in sch:
        raise RejectedRun(f"{name}: status.scheduling has no externalAssignments; the controller "
                          "must be built from Wayline's saga-executor branch")
    return prediction_from_assignments(sch["externalAssignments"], sch["externalEstimatedMakespan"], {
        "source": "wayline-status",
        "run": name,
        "scheduler": requested.removeprefix("saga/"),
        "saga_version": sch.get("externalSagaVersion"),
        "costModelFitRMSE": sch.get("externalCostModelFitRMSE"),
        "capacityModel": sch.get("externalCapacityModel"),
        "nodeCPUMillis": sch.get("externalNodeCPUMillis"),
        "constraintOverrides": sch.get("externalConstraintOverrides"),
    })


def compare_with_offline(in_cluster: dict, offline: dict) -> dict:
    """Did the cluster's SAGA see the same input and decide the same as the offline run?"""
    offline_cpu = {n["name"]: n["cpuMillis"] for n in offline.get("request", {}).get("clusterState", {}).get("nodes", [])}
    differences = same_schedule(offline, in_cluster)
    return {
        "same_schedule": not differences,
        "differences": differences,
        # The cores model takes C from the request, so node CPU does not enter SAGA's
        # problem; a difference still means machines.yaml is out of date, and a
        # machine with fewer free CPUs than SAGA cores cannot run them all at once.
        "same_node_cpu": offline_cpu == (in_cluster["meta"].get("nodeCPUMillis") or {}),
        "offline_cpu_millis": offline_cpu,
        "same_saga_version": offline["meta"].get("saga_version") == in_cluster["meta"].get("saga_version"),
    }


def lanes(tasks: dict, by_core: bool) -> dict[str, list[ScheduledTask]]:
    """Group tasks into Gantt rows, each sorted by start.

    by_core: one row per t["node"] (SAGA's core, "<machine>/c<i>").
    Otherwise tasks go on numbered slots of their machine: a task takes the
    first slot that is free, so overlapping tasks get their own rows. A slot
    is not the real core (Linux picks that).
    """
    rows: dict[str, list[ScheduledTask]] = {}
    for name, t in sorted(tasks.items(), key=lambda kv: (kv[1]["start"], kv[0])):
        if by_core:
            row = t["node"]
        else:
            i = 0
            while rows.get(f"{t['machine']} slot {i}") and rows[f"{t['machine']} slot {i}"][-1].end > t["start"]:
                i += 1
            row = f"{t['machine']} slot {i}"
        rows.setdefault(row, []).append(ScheduledTask(node=row, name=name, start=t["start"], end=t["end"]))
    return rows


def draw(prediction: dict, actual: dict, scheduler: str, path: Path) -> None:
    """SAGA's Gantt chart above Wayline's, on one time axis."""
    predicted_rows = lanes(prediction["tasks"], by_core=True)
    actual_rows = lanes(actual["tasks"], by_core=False)
    xmax = max(prediction["makespan"], actual["makespan"]) * 1.05

    heights = [max(len(predicted_rows), 1), max(len(actual_rows), 1)]
    fig, (top, bottom) = plt.subplots(
        2, 1, sharex=True, figsize=(10, 1.6 + 0.6 * sum(heights)),
        gridspec_kw={"height_ratios": heights},
    )
    for axis, rows, title, ylabel, makespan in (
        (top, predicted_rows, f"SAGA prediction ({scheduler})", "SAGA core", prediction["makespan"]),
        (bottom, actual_rows, f"Wayline run {actual['run'].rsplit('-', 1)[-1]}", "Wayline slot", actual["makespan"]),
    ):
        draw_gantt(rows, axis=axis, xmax=xmax, font_size=11, tick_font_size=10)
        axis.axvline(makespan, color="#52514e", linestyle="--", linewidth=1)
        axis.set_title(f"{title}: makespan {makespan:.3f} s", fontsize=12, loc="left")
        axis.set_ylabel(ylabel, fontsize=11)
        axis.set_xlabel("")
    bottom.set_xlabel("Seconds from the first task's start", fontsize=11)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def analyze(status_path: Path) -> dict:
    """Write prediction.json, actual.json and gantt.png next to status.json.
    Returns the comparison with the offline prediction ({} if there is none)."""
    run_dir = status_path.parent
    template_dir = run_dir.parent.parent   # <out>/<graph>/<run name>/
    status = json.loads(status_path.read_text())
    prediction = prediction_from_status(status)          # raises RejectedRun
    scheduler = prediction["meta"]["scheduler"]

    comparison = {}
    offline_path = template_dir / "prediction.json"
    if offline_path.exists():
        comparison = compare_with_offline(prediction, json.loads(offline_path.read_text()))
        prediction["meta"]["offline"] = comparison
        if not comparison["same_node_cpu"]:
            print(f"  note: cluster node CPU {prediction['meta']['nodeCPUMillis']} != machines.yaml "
                  f"{comparison['offline_cpu_millis']} (update partitions.k8s.saga_cpu_millis)")
        cores = (json.loads(offline_path.read_text()).get("request", {}).get("model") or {}).get("cores", {})
        for m, c in cores.items():
            free = (prediction["meta"].get("nodeCPUMillis") or {}).get(m)
            if free is not None and c * 1000 > free:
                print(f"  warning: SAGA plans {c} cores on {m} but only {free / 1000:g} CPUs are free")
        if not comparison["same_schedule"]:
            print(f"  note: in-cluster SAGA schedule differs from offline: {comparison['differences']}")

    actual = actual_from_status(status)
    (run_dir / "prediction.json").write_text(json.dumps(prediction, indent=2) + "\n")
    (run_dir / "actual.json").write_text(json.dumps(actual, indent=2) + "\n")
    draw(prediction, actual, scheduler, run_dir / "gantt.png")
    print(f"wrote {run_dir}/prediction.json, actual.json and gantt.png "
          f"(makespan {actual['makespan']:.3f} s vs. predicted {prediction['makespan']:.3f} s)")
    return comparison


def main() -> None:
    rejected = 0
    for arg in sys.argv[1:]:
        try:
            analyze(Path(arg))
        except RejectedRun as e:
            rejected += 1
            print(f"rejected: {e}")
    sys.exit(1 if rejected else 0)


if __name__ == "__main__":
    main()
