"""E3 offline model variants: replay SAGA's fixed mapping and order under richer models.

No execution and no re-planning of the measured run: for a run's prediction.json
we keep SAGA's mapping (task -> SAGA core) and each core's order (by predicted
start) and recompute the timeline with a simple list-scheduling replay

    ready(v)  = max(end of v's predecessor on its core,
                    max over producers u of end(u) + comm(u, v))
    start(v)  = ready(v) [+ launch(machine of v)] [+ stagger queue]
    end(v)    = start(v) + runtime(v on its machine)

under these model variants (cumulative):

  a  base       SAGA's own model, exactly what the run was planned with: comm =
                bytes / the request's links (self/intra 1e12 "free", inter = min of
                both measured directions). The replay reproduces SAGA's predicted
                start/end (checked: verify_base).
  b  +links     measured data-path bandwidths (E2, Slurm direct transport):
                inter per direction, same machine per machine (MEASURED_LINKS).
  c  +launch    + a per-task launch overhead on the consumer's machine (LAUNCH;
                Slurm step launch + driver noticing the producer's exit +
                interpreter start), added to every task's start.
  d  +stagger   + launch serialization: the driver issues launches one at a time,
                at least STAGGER seconds apart, in ready order.
  e  +serial-io (extra, beyond the PI's a-d) the Slurm direct data path as it
                runs: a consumer is launched after its producers' steps have
                ended, then reads its inputs one after another (bytes / link),
                works, then generates (GEN_RATE, random bytes) and writes each
                output; the core is busy for all of that.

Times are shifted so that the first task starts at 0 (actual.json's t0 = first
task start); for variant a that shift is 0.

The same module re-plans with SAGA under each variant where the bridge can
express it (variant_request): b = measured links in the request (self/intra per
machine, inter per direction; the bridge takes min of directions), c = b + the
launch overhead added to every task's cost (rank-1: + mean(LAUNCH) seconds at
speed 1.0, so cost/speed stays exact); d and e are not expressible and use c.

    uv run python analysis/e3_model.py fit        # refit LAUNCH/STAGGER/GEN_RATE from E0-E2 logs
"""

from __future__ import annotations

import copy
import glob
import heapq
import json
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

LOGS = Path.home() / "slurm-logs" / "saga"
CORE_SEP = "/c"

# --- parameters (bytes/s, seconds) ------------------------------------------------
# E2 (experiments/slurm/NOTES.md): data movement = producer write + consumer read
# through the direct transport ~= 0-3 ms + bytes / bandwidth.
MEASURED_LINKS = {
    "inter": {("cse-lambda1", "cse-isha"): 110e6, ("cse-isha", "cse-lambda1"): 106e6},
    "local": {"cse-lambda1": 1.05e9, "cse-isha": 0.7e9},       # same machine (self = intra on Slurm)
}
# Fit by `fit` below (2026-11-18 logs): median of consumer work start - last
# producer work end over 0-byte dependencies, by consumer machine: E0 (each
# consumer's last producer; lambda1 n=21, isha n=6) + E2 0-byte self/intra/inter
# (n=9 each).
LAUNCH = {"cse-lambda1": 0.108, "cse-isha": 0.126}
# E1 (14 and 28 simultaneous independent tasks, n=6): (last - first task start) / (n - 1).
STAGGER = 0.0077
# Payload generation (repeated 1 MiB random block, lazy; task/fidelity_core.py),
# measured over E3 round 2 (2026-11-18): bytes / generate time per machine.
# Effectively negligible (first block ~3 ms per task). Before the payload fix it
# was random.randbytes at ~354 / 323 MB/s (E2).
GEN_RATE = {"cse-lambda1": 35.0e9, "cse-isha": 25.8e9}


@dataclass(frozen=True)
class Variant:
    name: str
    label: str
    links: str = "request"          # "request" (SAGA's) | "measured"
    launch: dict | None = None
    stagger: float = 0.0
    serial_io: bool = False
    gen_rate: dict | None = None
    note: str = ""


VARIANTS = {
    "a": Variant("a", "base"),
    "b": Variant("b", "+links", links="measured"),
    "c": Variant("c", "+launch", links="measured", launch=LAUNCH),
    "d": Variant("d", "+stagger", links="measured", launch=LAUNCH, stagger=STAGGER),
    "e": Variant("e", "+serial-io", links="measured", launch=LAUNCH, stagger=STAGGER, serial_io=True,
                 gen_rate=GEN_RATE),
}


def machine_of(core: str) -> str:
    return core.rpartition(CORE_SEP)[0] if CORE_SEP in core else core


# ---------------------------------------------------------------------------
# Problem: the fixed mapping/order from a prediction.json
# ---------------------------------------------------------------------------

@dataclass
class Problem:
    tasks: list[str]                                   # request order
    core: dict[str, str]                               # SAGA core "<machine>/c<i>"
    runtime: dict[str, float]                          # seconds on its machine
    inputs: dict[str, list[tuple[str, float]]]         # consumer -> [(producer, bytes)] (read order)
    outputs: dict[str, list[tuple[str, float]]]        # producer -> [(consumer, bytes)] (write order)
    core_prev: dict[str, str | None]
    request: dict = field(repr=False, default_factory=dict)
    predicted: dict = field(repr=False, default_factory=dict)

    def machine(self, t: str) -> str:
        return machine_of(self.core[t])

    @classmethod
    def from_prediction(cls, pred: dict) -> Problem:
        req = pred["request"]
        ptasks = pred["tasks"]
        tasks = [t["name"] for t in req["dag"]["tasks"]]
        core = {t: ptasks[t]["node"] for t in tasks}
        runtime = {}
        inputs: dict[str, list] = {t: [] for t in tasks}
        outputs: dict[str, list] = {t: [] for t in tasks}
        for t in req["dag"]["tasks"]:
            runtime[t["name"]] = float(t["runtimeProfile"][machine_of(core[t["name"]])])
            declared: dict[str, float] = {}
            for i in t.get("inputs", []) or []:
                declared[i["producer"]] = declared.get(i["producer"], 0.0) + float(i.get("bytes", 0) or 0)
            for dep in t.get("dependencies", []) or []:
                inputs[t["name"]].append((dep, declared.get(dep, 0.0)))
        for t in tasks:                                  # saga_slurm.build_plan: consumers in task order
            for p, b in inputs[t]:
                outputs[p].append((t, b))
        index = {t: i for i, t in enumerate(tasks)}
        by_core: dict[str, list] = {}
        for t in tasks:
            by_core.setdefault(core[t], []).append(t)
        core_prev = {}
        for ts in by_core.values():
            ts.sort(key=lambda t: (ptasks[t]["start"], ptasks[t]["end"], index[t]))
            for a, b in zip([None] + ts, ts):
                core_prev[b] = a
        return cls(tasks, core, runtime, inputs, outputs, core_prev, req, pred)

    # links -----------------------------------------------------------------
    def link(self, u: str, v: str, variant: Variant) -> float:
        cu, cv = self.core[u], self.core[v]
        mu, mv = machine_of(cu), machine_of(cv)
        if variant.links == "measured":
            return MEASURED_LINKS["local"][mu] if mu == mv else MEASURED_LINKS["inter"][(mu, mv)]
        model = self.request.get("model") or {}
        if mu == mv:
            value = model.get("selfBandwidth" if cu == cv else "intraBandwidth")
            return float(value.get(mu, 1e12) if isinstance(value, dict) else (value or 1e12))
        bw = {(e["from"], e["to"]): float(e["bytesPerSec"]) for e in self.request["clusterState"]["bandwidth"]}
        cand = [x for x in (bw.get((mu, mv)), bw.get((mv, mu))) if x]
        return min(cand)


def replay(problem: Problem, variant: Variant) -> dict:
    """-> {"tasks": {t: {launch, start, end, done}}, "makespan"}; start/end = work start/end
    (SAGA's start/end), launch = task program start, done = task end incl. outputs."""
    deps = {t: [p for p, _ in problem.inputs[t]] + ([problem.core_prev[t]] if problem.core_prev[t] else [])
            for t in problem.tasks}
    children: dict[str, list] = {t: [] for t in problem.tasks}
    for t, ds in deps.items():
        for d in set(ds):
            children[d].append(t)
    missing = {t: len(set(ds)) for t, ds in deps.items()}
    index = {t: i for i, t in enumerate(problem.tasks)}
    out: dict[str, dict] = {}
    heap = []

    def ready_time(v: str) -> float:
        prev = problem.core_prev[v]
        r = out[prev]["done"] if prev else 0.0
        for u, b in problem.inputs[v]:
            if variant.serial_io:
                r = max(r, out[u]["done"])
            else:
                r = max(r, out[u]["done"] + b / problem.link(u, v, variant))
        return r

    for t in problem.tasks:
        if missing[t] == 0:
            heapq.heappush(heap, (0.0, index[t], t))
    last_issue = None
    while heap:
        r, _, v = heapq.heappop(heap)
        issue = r if (last_issue is None or not variant.stagger) else max(r, last_issue + variant.stagger)
        last_issue = issue
        m = problem.machine(v)
        launch = issue + (variant.launch[m] if variant.launch else 0.0)
        start = launch
        if variant.serial_io:
            start += sum(b / problem.link(u, v, variant) for u, b in problem.inputs[v])
        end = start + problem.runtime[v]
        done = end
        if variant.serial_io:
            done += sum(b / variant.gen_rate[m] for _, b in problem.outputs[v])
        out[v] = {"launch": launch, "start": start, "end": end, "done": done}
        for c in children[v]:
            missing[c] -= 1
            if missing[c] == 0:
                heapq.heappush(heap, (ready_time(c), index[c], c))
    if len(out) != len(problem.tasks):
        raise ValueError("replay: cycle between data and core order (inconsistent prediction)")
    t0 = min(x["launch"] for x in out.values())
    for x in out.values():
        for k in x:
            x[k] -= t0
    return {"tasks": out, "makespan": max(x["done"] for x in out.values())}


def verify_base(pred: dict, tol: float = 1e-6) -> list[str]:
    """Differences between replay(a) and SAGA's own prediction ([] = reproduced)."""
    r = replay(Problem.from_prediction(pred), VARIANTS["a"])
    diffs = []
    if abs(r["makespan"] - pred["makespan"]) > tol * max(1.0, pred["makespan"]):
        diffs.append(f"makespan {r['makespan']} vs SAGA {pred['makespan']}")
    for t, p in pred["tasks"].items():
        x = r["tasks"][t]
        if abs(x["start"] - p["start"]) > tol * max(1.0, p["start"]) or abs(x["end"] - p["end"]) > tol * max(1.0, p["end"]):
            diffs.append(f"{t}: replay [{x['start']}, {x['end']}] vs SAGA [{p['start']}, {p['end']}]")
    return diffs


# ---------------------------------------------------------------------------
# Re-planning with SAGA under a variant (what SAGA would choose)
# ---------------------------------------------------------------------------

def variant_request(request: dict, variant: Variant, algorithm: str | None = None) -> tuple[dict, str]:
    """The bridge request for planning under `variant` (as far as SAGA can express it)
    -> (request, which variant it actually expresses)."""
    req = copy.deepcopy(request)
    if algorithm:
        req["algorithm"] = algorithm
    if variant.links == "request":
        return req, "a"
    model = req.setdefault("model", {})
    machines = [n["name"] for n in req["clusterState"]["nodes"]]
    model["selfBandwidth"] = {m: MEASURED_LINKS["local"][m] for m in machines}
    model["intraBandwidth"] = {m: MEASURED_LINKS["local"][m] for m in machines}
    for e in req["clusterState"]["bandwidth"]:
        e["bytesPerSec"] = MEASURED_LINKS["inter"][(e["from"], e["to"])]
    if not variant.launch:
        return req, "b"
    # rank-1 (cost/speed) must stay exact: add one overhead at speed 1.0 to every cost
    delta = statistics.mean(variant.launch.values())
    speeds = _speeds(req)
    for t in req["dag"]["tasks"]:
        seconds = float(t.get("runtime", 0.0)) + delta
        t["runtime"] = seconds
        t["runtimeProfile"] = {m: seconds / speeds[m] for m in t["runtimeProfile"]}
    return req, "c"


def _speeds(req: dict) -> dict[str, float]:
    """Machine speeds implied by the request (runtime at speed 1.0 / runtimeProfile)."""
    for t in req["dag"]["tasks"]:
        if t.get("runtime"):
            return {m: float(t["runtime"]) / float(v) for m, v in t["runtimeProfile"].items()}
    return {m: 1.0 for m in req["dag"]["tasks"][0]["runtimeProfile"]}


def replan(request: dict, variant: Variant, algorithm: str, cache_dir: Path | None = None) -> dict:
    """SAGA's own predicted makespan when planning under `variant` (bridge, PYTHONHASHSEED=0)."""
    import hashlib

    from saga_exec.scheduling import run_bridge
    req, expressed = variant_request(request, variant, algorithm)
    key = hashlib.sha256(json.dumps(req, sort_keys=True).encode()).hexdigest()[:20]
    path = cache_dir / f"{key}.json" if cache_dir else None
    if path and path.exists():
        resp = json.loads(path.read_text())
    else:
        resp = run_bridge(req)
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(resp))
    return {"makespan": resp["estimatedMakespan"], "expressed": expressed}


# ---------------------------------------------------------------------------
# Fitting the parameters from the E0-E2 logs (read-only)
# ---------------------------------------------------------------------------

def fit(logs: Path = LOGS) -> dict:
    gaps: dict[str, list] = {}
    for d in sorted(logs.glob("2026*")):
        try:
            a = json.loads((d / "actual.json").read_text())
            s = json.loads((d / "summary.json").read_text())
        except (OSError, ValueError):
            continue
        if "/e0/" not in s.get("run_file", ""):
            continue
        prods: dict[str, list] = {}
        for e in a["edges"].values():
            prods.setdefault(e["consumer"], []).append(e["producer"])
        for c, ps in prods.items():
            T = a["tasks"]
            gaps.setdefault(T[c]["machine"], []).append(
                T[c]["times"]["work_start"] - max(T[p]["times"]["work_end"] for p in ps))
    gen: dict[str, list] = {}
    for d in glob.glob(str(logs / "calib-direct-*")):
        m = re.match(r"calib-direct-(\w+)-(cse-\w+)-(\d+)-\d-\d+", Path(d).name)
        if not m:
            continue
        try:
            t = json.loads((Path(d) / "results.json").read_text())["tasks"]
            pt, ct = t["p0"]["timings"]["timeline"], t["c0"]["timings"]["timeline"]
        except (OSError, ValueError, KeyError):
            continue
        size = int(m.group(3))
        if size == 0:
            gaps.setdefault(t["c0"]["node"], []).append(ct["workStartUnix"] - pt["workEndUnix"])
        o = pt["outputs"][0]
        if size >= 10_000_000 and o.get("generateStartUnix"):
            gen.setdefault(t["p0"]["node"], []).append(size / (o["generateEndUnix"] - o["generateStartUnix"]))
    stagger = []
    for d in sorted(logs.glob("2026*")):
        try:
            s = json.loads((d / "summary.json").read_text())
            a = json.loads((d / "actual.json").read_text())
        except (OSError, ValueError):
            continue
        if "/e1/" in s.get("run_file", "") and len(a["tasks"]) >= 14:
            st = sorted(v["times"]["task_start"] for v in a["tasks"].values())
            stagger.append((st[-1] - st[0]) / (len(st) - 1))
    return {"launch": {m: statistics.median(v) for m, v in gaps.items()},
            "launch_n": {m: len(v) for m, v in gaps.items()},
            "stagger": statistics.median(stagger) if stagger else None, "stagger_n": len(stagger),
            "gen_rate": {m: statistics.median(v) for m, v in gen.items()},
            "gen_n": {m: len(v) for m, v in gen.items()}}


if __name__ == "__main__":
    if sys.argv[1:] == ["fit"]:
        print(json.dumps(fit(), indent=2))
    else:
        sys.exit("usage: python analysis/e3_model.py fit")
