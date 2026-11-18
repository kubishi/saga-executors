"""Predicted vs. actual.

side_by_side(prediction, actual) is the per-task / per-edge / makespan table both
backends write as compare.json (docs/formats.md). compare() below is the older
summary on ScheduleRecord.


Metrics both backends already report in some form:
  - makespan error (actual - predicted), relative error
  - per-task start/end deviation
  - placement check (same machine), order check (same per-machine start order)

Both backends now write prediction.json / actual.json in the shared shape; the
Gantt pairs and tables are still per backend (analyze.py, saga_slurm.py).
"""

from __future__ import annotations

from dataclasses import dataclass

from saga_exec.schedule import ScheduleRecord


@dataclass
class Comparison:
    predicted_makespan: float
    actual_makespan: float
    right_machine: bool
    right_order: bool
    task_start_error: dict[str, float]
    task_end_error: dict[str, float]

    @property
    def makespan_error(self) -> float:
        return self.actual_makespan - self.predicted_makespan

    @property
    def relative_error(self) -> float:
        return self.makespan_error / self.predicted_makespan if self.predicted_makespan else float("nan")


def _order(rec: ScheduleRecord) -> dict[str, list[str]]:
    by = {}
    for name, t in sorted(rec.tasks.items(), key=lambda kv: (kv[1].start, kv[0])):
        by.setdefault(t.machine, []).append(name)
    return by


def compare(predicted: ScheduleRecord, actual: ScheduleRecord) -> Comparison:
    common = predicted.tasks.keys() & actual.tasks.keys()
    return Comparison(
        predicted_makespan=predicted.makespan,
        actual_makespan=actual.makespan,
        right_machine=all(predicted.tasks[n].machine == actual.tasks[n].machine for n in common),
        right_order=_order(predicted) == _order(actual),
        task_start_error={n: actual.tasks[n].start - predicted.tasks[n].start for n in sorted(common)},
        task_end_error={n: actual.tasks[n].end - predicted.tasks[n].end for n in sorted(common)},
    )


# ---------------------------------------------------------------------------
# Side by side: SAGA's prediction.json next to actual.json (v2), no verdicts
# ---------------------------------------------------------------------------

FREE_LINK = 1e12          # = saga_exec.scheduling.FREE_LINK (bridge default for self / intra links)


def _machine(core: str) -> str:
    return core.rpartition("/c")[0] if "/c" in core else core


def saga_link(request: dict, src_core: str, dst_core: str) -> tuple[str, float | None]:
    """SAGA's link between two SAGA nodes, as the bridge builds it from the request:
    same core -> model.selfBandwidth, same machine -> model.intraBandwidth (both
    1e12 if unset), across machines -> min of the two directed bandwidths."""
    a, b = _machine(src_core), _machine(dst_core)
    model = request.get("model") or {}
    if a == b:
        key, kind = ("selfBandwidth", "same-core") if src_core == dst_core else ("intraBandwidth", "intra")
        value = model.get(key)
        return kind, float(value.get(a, FREE_LINK) if isinstance(value, dict) else (value or FREE_LINK))
    bw = {(x["from"], x["to"]): float(x["bytesPerSec"])
          for x in (request.get("clusterState") or {}).get("bandwidth", [])}
    speeds = [v for v in (bw.get((a, b)), bw.get((b, a))) if v is not None]
    return "inter", (min(speeds) if speeds else None)


def _sub(a, b):
    return a - b if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None


def request_edges(request: dict) -> list[tuple[str, str, str | None, int | None]]:
    """(producer, consumer, object, bytes) for every edge of the bridge request."""
    out = []
    for t in (request.get("dag") or {}).get("tasks", []):
        named = {i["producer"]: i for i in t.get("inputs", [])}
        for p in t.get("dependencies", []):
            i = named.get(p, {})
            out.append((p, t["name"], i.get("object"), i.get("bytes")))
    return out


def side_by_side(prediction: dict, actual: dict, request: dict | None = None) -> dict:
    """SAGA-predicted vs actual, per task, per edge and for the makespan, in seconds
    from each record's time 0 (docs/formats.md: both are the first task's start).
    Differences are actual - SAGA. Reads actual.json v2; with a v1 actual (no
    work times or edges) the corresponding fields are None."""
    request = request or prediction.get("request") or {}
    ptasks, atasks = prediction.get("tasks", {}), actual.get("tasks", {})
    tasks = {}
    for name in sorted(ptasks.keys() | atasks.keys(), key=lambda n: (ptasks.get(n, {}).get("start", 1e18), n)):
        p, a = ptasks.get(name), atasks.get(name)
        times = (a or {}).get("times") or {}
        dur = (a or {}).get("durations") or {}
        saga = None if p is None else {"machine": p["machine"], "core": p["node"], "start": p["start"],
                                        "end": p["end"], "duration": p["end"] - p["start"]}
        act = None if a is None else {
            "machine": a.get("machine"), "core": a.get("core"),
            "task_start": a.get("start"), "task_end": a.get("end"), "task_duration": _sub(a.get("end"), a.get("start")),
            "work_start": times.get("work_start"), "work_end": times.get("work_end"), "work": dur.get("work"),
            "before_work": dur.get("before_work"), "outputs": dur.get("outputs"),
            "after_outputs": dur.get("after_outputs")}
        diff = None
        if saga and act:
            diff = {"work_start": _sub(act["work_start"], saga["start"]),
                    "work_end": _sub(act["work_end"], saga["end"]),
                    "work_duration": _sub(act["work"], saga["duration"]),
                    "task_start": _sub(act["task_start"], saga["start"]),
                    "task_end": _sub(act["task_end"], saga["end"]),
                    "task_duration": _sub(act["task_duration"], saga["duration"])}
        tasks[name] = {"saga": saga, "actual": act, "same_machine": (saga and act and
                                                                    saga["machine"] == act["machine"]),
                       "actual_minus_saga": diff}

    edge_keys = {(p, c): (o, b) for p, c, o, b in request_edges(request)}
    for e in (actual.get("edges") or {}).values():
        edge_keys.setdefault((e["producer"], e["consumer"]), (e.get("object"), e.get("bytes")))
    edges = {}
    for (p, c), (obj, nbytes) in sorted(edge_keys.items()):
        e = (actual.get("edges") or {}).get(f"{p}->{c}") or {}
        saga = None
        if p in ptasks and c in ptasks:
            src, dst = ptasks[p]["node"], ptasks[c]["node"]
            kind, speed = saga_link(request, src, dst) if request else (None, None)
            nb = nbytes if nbytes is not None else e.get("bytes")
            saga = {"src_core": src, "dst_core": dst, "kind": kind, "link_bytes_per_s": speed,
                    "comm_time": nb / speed if speed and nb is not None else None,
                    "producer_end": ptasks[p]["end"], "consumer_start": ptasks[c]["start"],
                    "gap": ptasks[c]["start"] - ptasks[p]["end"]}
        dec = e.get("decomposition") or {}
        act = None if not e else {
            "kind": e.get("kind"), "src_machine": e.get("src_machine"), "dst_machine": e.get("dst_machine"),
            "bytes": e.get("bytes"),
            "data_movement": e.get("data_movement"), "work_gap": e.get("work_gap"),
            "producer_write": dec.get("producer_write"), "wait_launch": dec.get("wait_launch"),
            "network": dec.get("network"), "consumer_read": dec.get("consumer_read"),
            "cross_clock": dec.get("cross_clock")}
        diff = None
        if saga and act and saga["comm_time"] is not None:
            diff = {"data_movement_minus_comm": _sub(act["data_movement"], saga["comm_time"]),
                    "work_gap_minus_comm": _sub(act["work_gap"], saga["comm_time"]),
                    "network_minus_comm": _sub(act["network"], saga["comm_time"]),
                    "work_gap_minus_saga_gap": _sub(act["work_gap"], saga["gap"])}
        edges[f"{p}->{c}"] = {"producer": p, "consumer": c, "object": obj, "bytes": nbytes,
                              "saga": saga, "actual": act, "differences": diff}

    pm, am, aw = prediction.get("makespan"), actual.get("makespan"), actual.get("makespan_work")
    return {
        "run": actual.get("run"), "backend": actual.get("backend"), "transport": actual.get("transport"),
        "time_zero": {"saga": "start of the first scheduled task (SAGA time 0)",
                      "actual": actual.get("t0_definition", "first task's start")},
        "makespan": {"saga": pm, "actual_task": am, "actual_work": aw,
                     "actual_first_work_start": actual.get("first_work_start"),
                     "actual_task_minus_saga": _sub(am, pm), "actual_work_minus_saga": _sub(aw, pm)},
        "order": {"saga": prediction.get("order"),
                  "actual_by_work_start": _order_by(atasks, "work_start"),
                  "actual_by_task_start": _order_by(atasks, None)},
        "tasks": tasks,
        "edges": edges,
    }


def _order_by(atasks: dict, key: str | None) -> dict[str, list[str]]:
    def at(t):
        v = (t.get("times") or {}).get(key) if key else t.get("start")
        return v if isinstance(v, (int, float)) else float("inf")
    by: dict[str, list[str]] = {}
    for name, t in sorted(atasks.items(), key=lambda kv: (at(kv[1]), kv[0])):
        by.setdefault(t.get("machine"), []).append(name)
    return by
