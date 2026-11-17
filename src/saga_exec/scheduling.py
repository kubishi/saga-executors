"""The one SAGA scheduling path: instance + testbed + config -> bridge request -> bridge -> prediction.

SAGA never sees our data structures directly. Both backends, and Wayline's
controller in the cluster, describe the problem as the JSON request the
controller POSTs to the saga-sidecar (wayline cmd/odag-controller/saga.go,
sagaAssignTasks) and let Wayline's bridge (saga-sidecar/bridge.py,
`schedule_request`) turn it into a SAGA Network/TaskGraph. So offline and
in-cluster SAGA get identical input and identical model conversion:

    request = build_request(instance, testbed, cfg)   # exactly saga.go's request
    response = run_bridge(request)                    # bridge.schedule_request, PYTHONHASHSEED=0
    prediction = prediction_from_response(request, response)   # prediction.json

The model (ModelConfig, sent as the request's "model"; see bridge.py): each
machine is C SAGA nodes "<machine>/c<i>" that run one task at a time (the
bridge's opt-in "cores" node model; C from machines.yaml), with three link
kinds: self (same core), intra (core to core in one machine), both 1e12 B/s
("free") or machines.yaml self/intra_bandwidth ("measured"), and inter-machine
(measured, min of both directions). Runtimes follow the related-machines model,
runtime = cost * sec_per_cost / speed[machine], so the bridge's rank-1
cost/speed fit is exact (costModelFitRMSE ~ 0; check_fit warns otherwise) and
PyPI SAGA (no capacity model) plans exactly this problem.

Determinism: SAGA breaks ties in str-hash order, so the result depends on
PYTHONHASHSEED. The sidecar deployment pins PYTHONHASHSEED=0; run_bridge always
runs the bridge in a subprocess with PYTHONHASHSEED=0, so callers need not set it.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import subprocess
import sys
import warnings
from dataclasses import asdict, dataclass, field

from saga_exec.instance import Instance
from saga_exec.testbed import Testbed

HASH_SEED = "0"          # same as wayline deployments/odag-controller/deployment.yml
FIT_RMSE_TOLERANCE = 1e-9
ENACT_ORDERS = ("none", "order")
FREE_LINK = 1e12         # "free" self/intra links (the bridge's default, finite on purpose)
CORE_SEP = "/c"          # SAGA core names "<machine>/c<i>" (bridge.CORE_SEP)


def machine_of(core: str) -> str:
    """"cse-isha/c3" -> "cse-isha" (machine names unchanged)."""
    return core.rpartition(CORE_SEP)[0] if CORE_SEP in core else core


@dataclass(frozen=True)
class ModelConfig:
    """Which variant of SAGA's model to plan with (a run-config axis to evaluate).

    nodes             "cores": C one-task-at-a-time SAGA nodes per machine
                      ("capacity": the bridge's default, one node per machine;
                      not used by our experiments, needs SAGA feature/capacity)
    cores             SAGA cores per machine (int or {machine: int}); None =
                      machines.yaml partitions.<p>.saga_nodes_per_machine
    links             "free": self and intra-machine links at 1e12 B/s;
                      "measured": machines.yaml self_bandwidth / intra_bandwidth.
                      Inter-machine links are always the measured network.links.
    transfer_latency  fixed seconds per transfer. Only 0 for now: SAGA's
                      Network edges have a speed but no latency, so a non-zero
                      value needs a SAGA extension (docs/formats.md).
    """
    nodes: str = "cores"
    cores: int | dict[str, int] | None = None
    links: str = "free"
    transfer_latency: float = 0.0

    def __post_init__(self):
        if self.nodes not in ("cores", "capacity"):
            raise ValueError(f"model.nodes must be 'cores' or 'capacity', got {self.nodes!r}")
        if self.links not in ("free", "measured"):
            raise ValueError(f"model.links must be 'free' or 'measured', got {self.links!r}")
        if self.transfer_latency != 0:
            raise NotImplementedError("model.transfer_latency != 0 needs a SAGA Network-edge latency "
                                      "extension (docs/formats.md, model variants)")

    @classmethod
    def coerce(cls, value: "ModelConfig | dict | None") -> "ModelConfig":
        return value if isinstance(value, cls) else cls(**(value or {}))


class BridgeError(RuntimeError):
    """The bridge is missing, cannot schedule (e.g. wrong SAGA), or failed."""


@dataclass(frozen=True)
class SchedulingConfig:
    """What decides SAGA's input, besides the instance and machines.yaml."""
    scheduler: str = "heft"             # bridge name ("heft", "cpop", ...) or dotted path
    partition: str = "k8s"              # machines.yaml partition giving capacities: "k8s" | "slurm"
    sec_per_cost: float = 1.0           # instance cost units -> seconds at speed 1.0
    bytes_per_size: float = 1.0         # instance size units -> bytes
    speed: dict[str, float] | None = None   # per-machine speed override (emulated); None = machines.yaml
    task_cpu: str | None = None         # CPU request per task (k8s quantity); None = partition default
    options: dict = field(default_factory=dict)   # scheduler constructor options (schedulerConfig.options)
    model: ModelConfig = field(default_factory=ModelConfig)

    def __post_init__(self):
        object.__setattr__(self, "model", ModelConfig.coerce(self.model))

    def to_dict(self) -> dict:
        return asdict(self)


def bridge_model(tb: Testbed, cfg: SchedulingConfig) -> dict | None:
    """The request's "model" (= the template's schedulerConfig.model); None for the bridge default."""
    m = cfg.model
    if m.nodes == "capacity":
        return None
    part = tb.partition(cfg.partition)
    cores = dict(part.cores)
    if isinstance(m.cores, dict):
        cores.update(m.cores)
    elif m.cores is not None:
        cores = {k: int(m.cores) for k in cores}
    machines = sorted(tb.machines)
    if m.links == "free":
        self_bw = {k: FREE_LINK for k in machines}
        intra_bw = {k: FREE_LINK for k in machines}
    else:
        missing = [k for k in machines if k not in tb.self_bandwidth or k not in tb.intra_bandwidth]
        if missing:
            raise ValueError(f"model.links 'measured' needs self_bandwidth and intra_bandwidth for {missing}")
        self_bw = {k: tb.self_bandwidth[k] for k in machines}
        intra_bw = {k: tb.intra_bandwidth[k] for k in machines}
    return {"nodeModel": "cores", "cores": {k: cores[k] for k in machines},
            "selfBandwidth": self_bw, "intraBandwidth": intra_bw}


# ---------------------------------------------------------------------------
# Instance -> per-task quantities (shared with the backends' artifacts)
# ---------------------------------------------------------------------------

def cpu_millis(quantity: str) -> int:
    """Kubernetes CPU quantity -> millicores, rounding up like resource.Quantity.MilliValue."""
    q = str(quantity).strip()
    if q.endswith("m"):
        return int(q[:-1])
    return math.ceil(float(q) * 1000 - 1e-9)


def edge_object(consumer: str) -> str:
    """Name of the object a producer emits for one consumer (one output per edge)."""
    return f"to-{consumer}"


def task_inputs(inst: Instance, bytes_per_size: float) -> dict[str, list[tuple[str, str, int]]]:
    """task -> [(producer, object, bytes)] in edge order. Also each task's dependency order."""
    seen, out = set(), {t: [] for t in inst.tasks}
    for src, dst, size in inst.edges:
        if (src, dst) in seen:
            raise ValueError(f"{inst.name}: duplicate edge {src}->{dst}")
        seen.add((src, dst))
        out[dst].append((src, edge_object(dst), int(size * bytes_per_size)))
    return out


def machine_speeds(tb: Testbed, cfg: SchedulingConfig) -> dict[str, float]:
    speed = dict(tb.speed)
    for m, s in (cfg.speed or {}).items():
        if m not in speed:
            raise ValueError(f"speed override for unknown machine {m!r}; machines: {tb.machines}")
        speed[m] = float(s)
    if min(speed.values()) <= 0:
        raise ValueError(f"speeds must be > 0, got {speed}")
    return speed


def runtimes(inst: Instance, tb: Testbed, cfg: SchedulingConfig) -> dict[str, tuple[float, dict[str, float]]]:
    """task -> (seconds at speed 1.0, {machine: seconds}). Related machines: cost / speed."""
    speed = machine_speeds(tb, cfg)
    out = {}
    for name, cost in inst.tasks.items():
        seconds = cost * cfg.sec_per_cost
        out[name] = (seconds, {m: seconds / speed[m] for m in sorted(speed)})
    return out


# ---------------------------------------------------------------------------
# The request (= saga.go sagaScheduleRequest)
# ---------------------------------------------------------------------------

def build_request(inst: Instance, tb: Testbed, cfg: SchedulingConfig, *,
                  mem_bytes: dict[str, int] | None = None) -> dict:
    """The JSON body Wayline's controller would POST for this instance.

    Mirrors sagaAssignTasks for an ODAG from a template with
    profiling.runtimeSource "manual" and bandwidthSource "external" (what the
    Wayline backend generates), where the controller's resolvers return:
      runtimeProfile[n]  = the task's spec runtimeProfile[n]
      dataSizeProfile[n] = the task-level dataSize hint, unset -> defaults.dataSize "0" -> 0
      bandwidth u->v     = wl-network-profile "<u>_to_<v>", else defaultBandwidth
    "model" is the template's schedulerConfig.model, forwarded by the controller.
    Nodes are sorted by name; fields left out where Go's `omitempty` drops them.
    memBytes is sent but not used by the bridge (default 0 offline).
    """
    part = tb.partition(cfg.partition)
    nodes = sorted(tb.machines)
    demand = cpu_millis(cfg.task_cpu or part.task_cpu)
    inputs = task_inputs(inst, cfg.bytes_per_size)
    rts = runtimes(inst, tb, cfg)

    tasks = []
    for name in inst.tasks:
        seconds, profile = rts[name]
        t: dict = {"name": name, "dependencies": [p for p, _, _ in inputs[name]]}
        if inputs[name]:
            t["inputs"] = [{"producer": p, "object": o, "bytes": b} for p, o, b in inputs[name]]
        if seconds:
            t["runtime"] = seconds
        t["runtimeProfile"] = {n: profile[n] for n in nodes}
        t["dataSizeProfile"] = {n: 0 for n in nodes}
        t["cpuMillis"] = demand
        tasks.append(t)

    req: dict = {"algorithm": cfg.scheduler}
    if cfg.options:
        req["options"] = dict(cfg.options)
    model = bridge_model(tb, cfg)
    if model:
        req["model"] = model
    req["dag"] = {"tasks": tasks}
    req["clusterState"] = {
        "nodes": [{"name": n, "ready": True, "cpuMillis": part.cpu_millis[n],
                   "memBytes": (mem_bytes or {}).get(n, 0)} for n in nodes],
        "bandwidth": [{"from": u, "to": v, "bytesPerSec": tb.bandwidth_between(u, v)}
                      for u in nodes for v in nodes if u != v],
    }
    return req


# ---------------------------------------------------------------------------
# Running the bridge
# ---------------------------------------------------------------------------

_RUNNER = """\
import importlib, json, sys
bridge = importlib.import_module(sys.argv[1])
json.dump(bridge.schedule_request(json.load(sys.stdin)), sys.stdout)
"""


def bridge_problem() -> str | None:
    """Why Wayline's bridge cannot be used here at all, or None."""
    if importlib.util.find_spec("bridge") is None:
        return "Wayline's bridge is not installed (dependency wl-saga-sidecar)"
    return None


def saga_has_capacity_model() -> bool:
    """Whether the installed SAGA can plan the bridge's default capacity model
    (feature/capacity). Not needed for model.nodes "cores"."""
    import saga
    return (all(f in saga.TaskGraphNode.model_fields for f in ("demand", "runtimes", "allowed_nodes"))
            and "capacity" in saga.NetworkNode.model_fields)


def run_bridge(request: dict, *, bridge_module: str = "bridge",
               extra_path: tuple[str, ...] = (), check: bool = True) -> dict:
    """bridge.schedule_request(request) in a fresh interpreter with PYTHONHASHSEED=0.

    bridge_module / extra_path exist for tests (a stand-in bridge); real runs
    use Wayline's. Raises BridgeError with the bridge's own error on failure.
    """
    if check and bridge_module == "bridge":
        problem = bridge_problem()
        if problem:
            raise BridgeError(problem)
    env = {**os.environ, "PYTHONHASHSEED": HASH_SEED}
    if extra_path:
        env["PYTHONPATH"] = os.pathsep.join([*extra_path, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    proc = subprocess.run([sys.executable, "-c", _RUNNER, bridge_module], input=json.dumps(request),
                          capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise BridgeError(f"bridge failed: {proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else proc.returncode}")
    return json.loads(proc.stdout)


def check_fit(response: dict, tolerance: float = FIT_RMSE_TOLERANCE) -> bool:
    """Warn unless the runtimes fit SAGA's cost/speed model exactly (they should, by construction)."""
    rmse = response.get("costModelFitRMSE")
    ok = rmse is not None and rmse <= tolerance
    if not ok:
        warnings.warn(f"costModelFitRMSE = {rmse} > {tolerance}: runtimes are not cost/speed "
                      "(related machines); SAGA's heuristics see a fitted model", RuntimeWarning, stacklevel=2)
    return ok


# ---------------------------------------------------------------------------
# Response -> prediction.json
# ---------------------------------------------------------------------------

def node_order(assignments: list[dict]) -> dict[str, list[str]]:
    """Per-machine start order exactly as the controller derives it (saga.go):
    group by machine in response order, stable-sort by (estimatedStart, estimatedFinish)."""
    by: dict[str, list[dict]] = {}
    for a in assignments:
        by.setdefault(a["node"], []).append(a)
    return {n: [a["task"] for a in sorted(xs, key=lambda a: (a["estimatedStart"], a["estimatedFinish"]))]
            for n, xs in by.items()}


def prediction_from_assignments(assignments: list[dict], makespan: float, meta: dict) -> dict:
    """prediction.json body (docs/formats.md) from bridge-shaped assignments."""
    ordered = sorted(assignments, key=lambda a: (a["estimatedStart"], a["task"]))
    return {
        "makespan": makespan,
        "meta": meta,
        "tasks": {a["task"]: {"node": a.get("core", a["node"]), "machine": a["node"],
                              "start": a["estimatedStart"], "end": a["estimatedFinish"]} for a in ordered},
        "order": node_order(assignments),
    }


def prediction_from_response(request: dict, response: dict, meta: dict | None = None) -> dict:
    fit_ok = check_fit(response)
    pred = prediction_from_assignments(response["assignments"], response["estimatedMakespan"], {
        **(meta or {}),
        "source": "bridge-offline",
        "scheduler": request["algorithm"],
        "saga_version": response.get("sagaVersion"),
        "costModelFitRMSE": response.get("costModelFitRMSE"),
        "costModelExact": fit_ok,
        "capacityModel": response.get("capacityModel"),
        "model": request.get("model"),
        "pythonHashSeed": HASH_SEED,
    })
    pred["request"] = request
    pred["response"] = response
    return pred


def schedule(inst: Instance, tb: Testbed, cfg: SchedulingConfig, meta: dict | None = None,
             **bridge_kw) -> dict:
    """build_request -> run_bridge -> prediction_from_response."""
    request = build_request(inst, tb, cfg)
    response = run_bridge(request, **bridge_kw)
    return prediction_from_response(request, response, meta)


def same_schedule(a: dict, b: dict, tol: float = 1e-6) -> list[str]:
    """Differences between two predictions (placement, order, times); [] if they agree."""
    diffs = []
    if abs(a["makespan"] - b["makespan"]) > tol:
        diffs.append(f"makespan {a['makespan']} vs {b['makespan']}")
    for t in sorted(a["tasks"].keys() | b["tasks"].keys()):
        x, y = a["tasks"].get(t), b["tasks"].get(t)
        if x is None or y is None:
            diffs.append(f"{t}: missing in one")
        elif x["node"] != y["node"]:
            diffs.append(f"{t}: {x['node']} vs {y['node']}")
        elif abs(x["start"] - y["start"]) > tol or abs(x["end"] - y["end"]) > tol:
            diffs.append(f"{t}: [{x['start']}, {x['end']}] vs [{y['start']}, {y['end']}]")
    return diffs
