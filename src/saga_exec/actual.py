"""actual.json, version 2: one backend-agnostic record of what a run did (docs/formats.md).

Each backend's analyzer turns its own measurements into *normalized* per-task
records (absolute Unix times, each on the clock of the machine that took it)
plus per-edge observations of its data path, and `build_actual` turns those
into actual.json:

    task  = {"machine", "core" (or None), "node" (label), "host", "state",
             "task_start", "task_end",            # the task program's own start / close
             "timeline": {...},                   # fidelity_core.Timeline.to_dict(), or None
             "legacy": {"input_read", "compute", "handoff"},
             "inputs_ready", "launched", "finished",   # optional, absolute
             "clocks": {"launched": "<driver machine>", ...},  # which clock non-task stamps use
             "extra": {...}}                      # backend-specific, copied as is
    edge observation, keyed (producer, consumer):
            {"transfer_start", "transfer_end", "transfer_clock", "transfer_mechanism",
             "transfer_within_consumer_read" (pull: the transfer happens inside the read),
             "available", "available_clock", "available_source", "extra": {...}}

Time 0 (`t0`) is the earliest task start (taskStartUnix) of the run, the same
event SAGA's time 0 stands for (the first task begins), so prediction.json and
actual.json share an origin. All relative times are `absolute - t0`; when the
two stamps come from different machines the difference includes their clock
offset, and every derived interval says which clocks it uses.
"""

from __future__ import annotations

from datetime import datetime, timezone

FORMAT = "saga-exec-actual/2"
UNOBSERVABLE = "unobservable"
T0_DEFINITION = ("earliest task start (the task program's taskStartUnix) over all tasks of the run; "
                 "SAGA's time 0 is also the first task's start")


def _sub(a, b):
    return a - b if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None


def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def edge_kind(src_machine: str | None, dst_machine: str | None, src_core=None, dst_core=None) -> str:
    """Observed edge class: inter (machines differ), same-core / intra (same machine,
    cores known), same-machine (same machine, cores unknown)."""
    if src_machine is None or dst_machine is None:
        return "unknown"
    if src_machine != dst_machine:
        return "inter"
    if src_core is None or dst_core is None:
        return "same-machine"
    return "same-core" if str(src_core) == str(dst_core) else "intra"


def consumer_of(obj: str | None) -> str | None:
    """Output object "to-<consumer>" (saga_exec.scheduling.edge_object) -> consumer."""
    return obj[3:] if obj and obj.startswith("to-") else None


def _task_entry(name: str, t: dict, t0: float) -> dict:
    rel = lambda v: v - t0 if _num(v) else None
    tl = t.get("timeline") or {}
    inputs = [{"producer": i.get("producer"), "object": i.get("object"), "peer": i.get("peer"),
               "bytes": i.get("bytes"),
               "source_machine": i.get("sourceNode"),
               "read_start": rel(i.get("readStartUnix")), "read_end": rel(i.get("readEndUnix")),
               "read": _sub(i.get("readEndUnix"), i.get("readStartUnix")),
               **{k: v for k, v in i.items() if k not in ("producer", "object", "peer", "bytes", "sourceNode",
                                                          "readStartUnix", "readEndUnix")}}
              for i in tl.get("inputs") or []]
    for i in inputs:                                   # transport-specific absolute stamps -> relative
        for k in [k for k in i if k.endswith("Unix")]:
            i[k[:-4] + "_rel"] = rel(i.pop(k))
    outputs = [{"object": o.get("object"), "consumer": consumer_of(o.get("object")), "bytes": o.get("bytes"),
                "generate_start": rel(o.get("generateStartUnix")), "generate_end": rel(o.get("generateEndUnix")),
                "send_start": rel(o.get("sendStartUnix")), "send_end": rel(o.get("sendEndUnix")),
                "generate": _sub(o.get("generateEndUnix"), o.get("generateStartUnix")),
                "send": _sub(o.get("sendEndUnix"), o.get("sendStartUnix")),
                **{k[:-4] + "_rel": rel(v) for k, v in o.items() if k.endswith("Unix") and k not in (
                    "generateStartUnix", "generateEndUnix", "sendStartUnix", "sendEndUnix")},
                **{k: v for k, v in o.items() if not k.endswith("Unix") and k not in ("object", "bytes")}}
               for o in tl.get("outputs") or []]
    ts, te = t.get("task_start"), t.get("task_end")
    ws, we = tl.get("workStartUnix"), tl.get("workEndUnix")
    reads = [i for i in tl.get("inputs") or [] if _num(i.get("readStartUnix"))]
    sends = [o for o in tl.get("outputs") or [] if _num(o.get("sendEndUnix"))]
    first_read = min((i["readStartUnix"] for i in reads), default=None)
    last_read = max((i["readEndUnix"] for i in reads), default=None)
    first_out = min((o.get("generateStartUnix") or o["sendStartUnix"] for o in sends), default=None)
    last_send = max((o["sendEndUnix"] for o in sends), default=None)
    legacy = t.get("legacy") or {}
    ready = t.get("inputs_ready")
    entry = {
        # unchanged meaning (actual.json v1)
        "node": t.get("node") or t.get("machine"), "machine": t.get("machine"),
        "start": rel(ts), "end": rel(te),
        "input_read": legacy.get("input_read"), "compute": legacy.get("compute"),
        "handoff": legacy.get("handoff"), "inputs_ready": rel(ready),
        "delay": _sub(ts, ready),
        # v2
        "core": t.get("core"), "host": t.get("host"), "state": t.get("state"),
        "clock": t.get("machine"),
        "times": {
            "task_start": rel(ts), "inputs_read_start": rel(first_read), "inputs_read_end": rel(last_read),
            "work_start": rel(ws), "work_end": rel(we),
            "outputs_start": rel(first_out), "outputs_end": rel(last_send), "task_end": rel(te),
            "launched": rel(t.get("launched")), "finished": rel(t.get("finished")),
        },
        "durations": {
            "total": _sub(te, ts),
            "before_work": _sub(ws, ts),
            "start_to_first_read": _sub(first_read, ts),
            "input_read": _sub(last_read, first_read),
            "work": _sub(we, ws),
            "generate": sum(o.get("generate") or 0.0 for o in outputs) if outputs else None,
            "send": sum(o.get("send") or 0.0 for o in outputs) if outputs else None,
            "outputs": _sub(last_send, we),
            "after_outputs": _sub(te, last_send),
        },
        "inputs": inputs,
        "outputs": outputs,
        "precise": bool(tl),
    }
    clocks = dict(t.get("clocks") or {})
    if clocks:
        entry["other_clocks"] = clocks
    if t.get("extra"):
        entry["extra"] = t["extra"]
    return entry


def _edge_entry(p: str, c: str, obj: str | None, tasks: dict, obs: dict, transport: str,
                t0: float, nbytes) -> dict:
    """One edge u->v: the instants along its data path and their decomposition."""
    rel = lambda v: v - t0 if _num(v) else None
    P, C = tasks.get(p) or {}, tasks.get(c) or {}
    ptl, ctl = P.get("timeline") or {}, C.get("timeline") or {}
    po = next((o for o in ptl.get("outputs") or [] if o.get("object") == obj), None) or {}
    ci = next((i for i in ctl.get("inputs") or [] if i.get("producer") == p and
               (obj is None or i.get("object") == obj)), None) or {}
    pm, cm = P.get("machine"), C.get("machine")
    kind = edge_kind(pm, cm, P.get("core"), C.get("core"))
    send_start, send_end = po.get("sendStartUnix"), po.get("sendEndUnix")
    read_start, read_end = ci.get("readStartUnix"), ci.get("readEndUnix")
    ts, te = obs.get("transfer_start"), obs.get("transfer_end")
    within = bool(obs.get("transfer_within_consumer_read"))
    tclock = obs.get("transfer_clock")

    # Decomposition: producer_write + wait_launch + network + consumer_read
    # = consumer read end - producer send start (the data-movement interval).
    producer_write = _sub(send_end, send_start)
    network: float | str | None = _sub(te, ts)
    network_note = obs.get("transfer_mechanism")
    if network is None:
        if pm is not None and pm == cm and transport in ("direct", "wayline"):
            network, network_note = 0.0, "none: same machine, local file"
        else:
            network = UNOBSERVABLE
            network_note = ("not separately observable: on a shared FS the network is inside the producer's "
                            "write and/or the consumer's read" if transport == "shared_fs"
                            else "no transfer record for this edge")
    read = _sub(read_end, read_start)
    consumer_read = read - network if (within and read is not None and _num(network)) else read
    total = _sub(read_end, send_start)
    wait = None
    if None not in (total, producer_write, consumer_read):
        wait = total - producer_write - consumer_read - (network if _num(network) else 0.0)
    clocks = {"producer_write": [pm], "consumer_read": [cm] + ([tclock] if within and _num(network) else []),
              "network": [tclock] if _num(te) else [], "wait_launch": sorted({pm, cm} - {None}),
              "data_movement": sorted({pm, cm} - {None})}
    detail = {
        "send_end_to_producer_end": _sub(P.get("task_end"), send_end),
        "producer_end_to_consumer_start": _sub(C.get("task_start"), P.get("task_end")),
        "consumer_start_to_read": _sub(read_start, C.get("task_start")),
    }
    if _num(ts) and not within:
        detail["send_end_to_transfer_start"] = _sub(ts, send_end)
        detail["transfer_end_to_available"] = _sub(obs.get("available"), te)
    if _num(obs.get("available")):
        detail["available_to_consumer_start"] = _sub(C.get("task_start"), obs.get("available"))
    edge = {
        "producer": p, "consumer": c, "object": obj,
        "bytes": nbytes if nbytes is not None else (po.get("bytes") if po else ci.get("bytes")),
        "src_machine": pm, "dst_machine": cm, "src_core": P.get("core"), "dst_core": C.get("core"),
        "kind": kind,
        "times": {
            "producer_work_end": rel(ptl.get("workEndUnix")),
            "producer_output_generated": rel(po.get("generateEndUnix")),
            "producer_output_send_start": rel(send_start),
            "producer_output_written": rel(send_end),
            "producer_task_end": rel(P.get("task_end")),
            "transfer_start": rel(ts), "transfer_end": rel(te),
            "consumer_input_available": rel(obs.get("available")),
            "consumer_task_start": rel(C.get("task_start")),
            "consumer_read_start": rel(read_start), "consumer_read_end": rel(read_end),
            "consumer_work_start": rel(ctl.get("workStartUnix")),
        },
        "transfer": {"mechanism": obs.get("transfer_mechanism"), "clock": tclock,
                     "within_consumer_read": within,
                     **{(k[:-5] if k.endswith("_unix") else k): (rel(v) if k.endswith("_unix") else v)
                        for k, v in (obs.get("extra") or {}).items()}},
        "available": {"source": obs.get("available_source"), "clock": obs.get("available_clock")},
        "data_movement": total,
        "work_gap": _sub(ctl.get("workStartUnix"), ptl.get("workEndUnix")),
        "decomposition": {
            "producer_write": producer_write,
            "wait_launch": wait,
            "network": network,
            "consumer_read": consumer_read,
            "network_note": network_note,
            "wait_launch_detail": detail,
            "clocks": clocks,
            "cross_clock": len(clocks["data_movement"]) > 1,
        },
    }
    if not ptl or not ctl:
        edge["note"] = "no task timeline for " + " and ".join(x for x, tl in ((p, ptl), (c, ctl)) if not tl)
    return edge


def build_actual(*, run: str, backend: str, transport: str, tasks: dict[str, dict],
                 edges: list[tuple] | None = None, observations: dict | None = None,
                 clocks: dict | None = None, meta: dict | None = None) -> dict:
    """Normalized per-task records + edge observations -> actual.json (v2).

    edges: [(producer, consumer, object or None, bytes or None)] known from the plan
    or the run spec; edges seen in the tasks' timelines are added."""
    observations = observations or {}
    starts = [t["task_start"] for t in tasks.values() if _num(t.get("task_start"))]
    if not starts:
        starts = [t["timeline"]["runStartUnix"] for t in tasks.values() if (t.get("timeline") or {}).get("runStartUnix")]
    t0 = min(starts) if starts else 0.0
    first = next((n for n, t in tasks.items() if t.get("task_start") == t0), None)
    ends = [t["task_end"] for t in tasks.values() if _num(t.get("task_end"))]
    work_ends = [t["timeline"]["workEndUnix"] for t in tasks.values()
                 if _num((t.get("timeline") or {}).get("workEndUnix"))]
    work_starts = [t["timeline"]["workStartUnix"] for t in tasks.values()
                   if _num((t.get("timeline") or {}).get("workStartUnix"))]

    edge_list: dict[tuple, tuple] = {}
    for p, c, obj, nbytes in edges or []:
        edge_list[(p, c)] = (obj, nbytes)
    for c, t in tasks.items():
        for i in (t.get("timeline") or {}).get("inputs") or []:
            key = (i.get("producer"), c)
            obj, nbytes = edge_list.get(key, (None, None))
            edge_list[key] = (obj or i.get("object"), nbytes if nbytes is not None else i.get("bytes"))

    out = {
        "format": FORMAT,
        "run": run, "backend": backend, "transport": transport,
        "t0": datetime.fromtimestamp(t0, timezone.utc).isoformat().replace("+00:00", "Z"),
        "t0_unix": t0, "t0_task": first, "t0_clock": (tasks.get(first) or {}).get("machine"),
        "t0_definition": T0_DEFINITION,
        "makespan": max(ends) - t0 if ends else None,
        "makespan_work": max(work_ends) - t0 if work_ends else None,
        "first_work_start": min(work_starts) - t0 if work_starts else None,
        "tasks": {n: _task_entry(n, t, t0) for n, t in sorted(
            tasks.items(), key=lambda kv: (kv[1].get("task_start") or float("inf"), kv[0]))},
        "edges": {f"{p}->{c}": _edge_entry(p, c, obj, tasks, observations.get((p, c), {}),
                                           transport, t0, nbytes)
                  for (p, c), (obj, nbytes) in sorted(edge_list.items())},
        "clocks": {"note": "Absolute times are each machine's own clock; cross-machine intervals include the "
                           "clock offset and are not corrected. Records below are best-effort.",
                   **(clocks or {})},
    }
    if meta:
        out["meta"] = meta
    return out
