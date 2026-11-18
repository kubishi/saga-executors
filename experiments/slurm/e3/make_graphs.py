#!/usr/bin/env python3
"""E3: realistic WfCommons workflows -> graphs/e3-*.json + run files.

    uv run python experiments/slurm/e3/make_graphs.py            # graphs, run files, manifest
    uv run python experiments/slurm/e3/make_graphs.py --check    # only print what would change

Structure and RELATIVE task runtimes / edge sizes come from WfCommons through
SAGA's helpers (saga.schedulers.data.wfcommons): real WfInstances traces
(Pegasus, chameleon-cloud; downloaded to ~/.saga/data by download_repo) when a
trace of a suitable size exists, otherwise a WfChef recipe with a fixed seed.
Each workflow is then rescaled for every (granularity, CCR) cell:

* costs (seconds at speed 1.0): cost_t = runtime_t * g / mean(runtime), so the
  mean task runtime is exactly g; runtimes below MIN_COST seconds are raised to
  MIN_COST before that normalization (the trace minimum is 1e-9 for missing runtimes).
* CCR (this experiment's definition, graph-level, schedule-independent):

      CCR = mean_edges(bytes_e / B_INTER) / mean_tasks(cost_t)

  with B_INTER = 108e6 B/s, the mean of the measured inter-machine bandwidths
  (E2: lambda1->isha ~110 MB/s, isha->lambda1 ~106 MB/s). So the target mean
  edge size is CCR * g * B_INTER bytes. Edge sizes keep their relative sizes
  except where they hit EDGE_CAP (water-filling: capped edges are fixed at the
  cap, the rest are scaled up to keep the mean); if even that cannot reach the
  target, or the total exceeds TOTAL_CAP, the reachable CCR is recorded
  ("ccr", next to "ccr_target") and the cell is flagged; cells whose CCR stays
  below FEASIBLE x target are listed in the manifest but get no graph or run
  files (mostly high CCR at 2-10 s granularity: that would need 50-200 GB/run).
* task names: "<category>-<n>" from the trace (mProject_ID0000012 ->
  mproject-12), lower case Kubernetes DNS labels (so the same graphs can run
  on Wayline); the mapping is kept in the manifest.

The manifest (experiments/slurm/e3/graphs.json) records per graph its source,
achieved granularity / CCR and stats (tasks, edges, depth, width, total bytes,
critical-path length). Run files go to experiments/slurm/e3/runs/.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import tempfile
from pathlib import Path

import networkx as nx
import numpy as np

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
GRAPHS = ROOT / "graphs"
RUNS = HERE / "runs"
MANIFEST = HERE / "graphs.json"

B_INTER = 108e6                 # bytes/s, mean measured inter-machine bandwidth (E2)
EDGE_CAP = 250_000_000          # bytes per edge: payloads are held in memory (mem "2G") and < 2**28
TOTAL_CAP = 25e9                # bytes per run: node-local /tmp scratch, removed only at the end of a run
FEASIBLE = 0.9                  # a cell runs only if its achieved CCR >= FEASIBLE * target (caps)
MIN_COST = 1e-3                 # seconds (relative runtime floor before normalizing)
MEM = "2G"                      # per task: 512 MiB + 2 x largest payload (saga_slurm.check_memory) < 2G
GRANULARITIES = (0.5, 2.0, 10.0)
CCRS = {"lo": 0.1, "hi": 1.0}
SCHEDULERS = ("heft", "cpop")
RECIPE_SEED = 20261118

# (family, size label) -> source. "trace": a WfInstances file (pegasus-instances
# repo, <dir>/chameleon-cloud/<file>); "recipe": WfChef recipe, n tasks, RECIPE_SEED.
WORKFLOWS = {
    ("montage", "s"): {"trace": ("montage", "montage-chameleon-2mass-005d-001.json")},
    ("montage", "l"): {"trace": ("montage", "montage-chameleon-dss-075d-001.json")},
    ("epigenomics", "s"): {"trace": ("epigenomics", "epigenomics-chameleon-hep-1seq-100k-001.json")},
    ("epigenomics", "l"): {"trace": ("epigenomics", "epigenomics-chameleon-ilmn-1seq-100k-001.json")},
    ("genome", "s"): {"trace": ("1000genome", "1000genome-chameleon-2ch-100k-001.json")},
    ("genome", "l"): {"trace": ("1000genome", "1000genome-chameleon-4ch-250k-001.json")},
}
TRACE_REPO = "https://github.com/wfcommons/pegasus-instances.git"


# ---------------------------------------------------------------------------
# Loading (SAGA's WfCommons helpers)
# ---------------------------------------------------------------------------

def load_trace(directory: str, filename: str) -> tuple[nx.DiGraph, dict[str, str]]:
    """A real trace as SAGA reads it (node weight = runtime s, edge weight =
    bytes of matching files), plus id -> "<category>-<n>" names from the JSON."""
    from saga.schedulers.data.wfcommons import download_repo, trace_to_digraph
    path = download_repo(TRACE_REPO) / directory / "chameleon-cloud" / filename
    graph = trace_to_digraph(path, directory)
    raw = json.loads(path.read_text())
    names = {}
    for t in raw["workflow"]["tasks"]:
        tid = t.get("id", t["name"])
        num = re.search(r"(\d+)$", tid)
        names[tid] = f"{t.get('category') or t['name']}-{int(num.group(1)) if num else tid}"
    return graph, names


def load_recipe(family: str, n: int, seed: int = RECIPE_SEED) -> tuple[nx.DiGraph, dict[str, str]]:
    """A WfChef recipe instance with a fixed seed (SAGA's get_workflows, minus its random size)."""
    from saga.schedulers.data.wfcommons import get_num_task_range, recipes, trace_to_digraph
    from wfcommons.wfgen.generator import WorkflowGenerator
    lo, _ = get_num_task_range(family)
    if n < lo:
        raise ValueError(f"recipe {family} needs at least {lo} tasks, got {n}")
    random.seed(seed)
    np.random.seed(seed)
    workflow = WorkflowGenerator(recipes[family](num_tasks=n)).build_workflow()
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "wf.json"
        workflow.write_json(p)
        graph = trace_to_digraph(p, family)
        raw = json.loads(p.read_text())
    names = {}
    for t in raw["workflow"].get("specification", raw["workflow"]).get("tasks", []):
        names[t["id"]] = t.get("name", t["id"])
    if any(graph.edges[e]["weight"] == 1.0 for e in graph.edges):
        print(f"  note: recipe {family} (schema 1.5) has no file sizes; all edges weigh 1 (equal sizes)")
    return graph, names


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------

DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
MAX_NAME = 40                 # leaves room for prefixes (pods, objects "to-<task>") within 63


def sanitize(name: str) -> str:
    """Kubernetes DNS label (lower case, [a-z0-9-], alnum at both ends), at most MAX_NAME chars."""
    s = re.sub(r"[^a-z0-9-]+", "-", name.lower())
    s = re.sub(r"-{2,}", "-", s).strip("-")[:MAX_NAME].strip("-")
    return s or "t"


def sanitize_all(names: dict[str, str]) -> dict[str, str]:
    """original -> unique sanitized name (collisions get -2, -3, ...), in input order."""
    out, used = {}, set()
    for orig, pretty in names.items():
        base = sanitize(pretty)
        cand, k = base, 2
        while cand in used:
            suffix = f"-{k}"
            cand = base[:MAX_NAME - len(suffix)].strip("-") + suffix
            k += 1
        used.add(cand)
        out[orig] = cand
    return out


# ---------------------------------------------------------------------------
# Rescaling
# ---------------------------------------------------------------------------

def scale_costs(runtimes: dict[str, float], granularity: float, floor: float = MIN_COST) -> dict[str, float]:
    """Keep relative runtimes; mean becomes exactly `granularity` (seconds at speed 1.0)."""
    r = {k: max(float(v), floor) for k, v in runtimes.items()}
    f = granularity / (sum(r.values()) / len(r))
    return {k: v * f for k, v in r.items()}


def scale_sizes(rel: dict, target_mean: float, cap: float = EDGE_CAP) -> dict:
    """Water-filling: sizes proportional to rel, mean = target_mean, none above cap.
    Edges that would exceed the cap are fixed at the cap and the others scaled up;
    if all reach the cap the mean is lower than asked (the caller records it)."""
    target_total = target_mean * len(rel)
    sizes, free, remaining = {}, {e for e, w in rel.items() if w > 0}, target_total
    for e, w in rel.items():
        if w <= 0:
            sizes[e] = 0.0
    while free:
        s = remaining / sum(rel[e] for e in free)
        over = {e for e in free if rel[e] * s > cap}
        if not over:
            sizes.update({e: rel[e] * s for e in free})
            break
        for e in over:
            sizes[e] = float(cap)
            remaining -= cap
        free -= over
        if remaining <= 0:
            sizes.update({e: 0.0 for e in free})
            break
    return {e: round(v) for e, v in sizes.items()}


def ccr_of(costs: dict[str, float], edges: list, bandwidth: float = B_INTER) -> float:
    """This experiment's CCR: mean edge transfer time at `bandwidth` / mean task cost (speed 1.0)."""
    if not edges:
        return 0.0
    return (sum(b for _, _, b in edges) / len(edges) / bandwidth) / (sum(costs.values()) / len(costs))


def rescale(graph: nx.DiGraph, names: dict[str, str], granularity: float, ccr: float, *,
            bandwidth: float = B_INTER, edge_cap: float = EDGE_CAP, total_cap: float = TOTAL_CAP) -> dict:
    """-> {"tasks": {name: cost}, "edges": [[u, v, bytes]], "ccr": achieved, "flags": [...]}."""
    ren = sanitize_all({n: names.get(n, str(n)) for n in nx.topological_sort(graph)})
    costs = scale_costs({ren[n]: graph.nodes[n]["weight"] for n in graph.nodes}, granularity)
    target_mean = ccr * granularity * bandwidth
    flags = []
    if target_mean * graph.number_of_edges() > total_cap:
        flags.append(f"total bytes capped at {total_cap:.3g}")
        target_mean = total_cap / graph.number_of_edges()
    rel = {(ren[u], ren[v]): float(graph.edges[u, v]["weight"]) for u, v in graph.edges}
    sizes = scale_sizes(rel, target_mean, edge_cap)
    if any(b >= edge_cap for b in sizes.values()):
        flags.append(f"{sum(b >= edge_cap for b in sizes.values())} edges at the {edge_cap:.3g} B cap")
    edges = [[u, v, sizes[(u, v)]] for (u, v) in rel]
    achieved = ccr_of(costs, edges, bandwidth)
    if achieved < 0.95 * ccr:
        flags.append(f"CCR {achieved:.3g} < target {ccr}")
    return {"tasks": costs, "edges": edges, "ccr": achieved, "flags": flags, "names": ren}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def graph_stats(tasks: dict[str, float], edges: list, bandwidth: float = B_INTER) -> dict:
    g = nx.DiGraph()
    g.add_nodes_from(tasks)
    g.add_edges_from((u, v, {"b": b}) for u, v, b in edges)
    level = {}
    for n in nx.topological_sort(g):
        level[n] = 1 + max((level[p] for p in g.predecessors(n)), default=0)
    width = int(max(np.bincount(list(level.values()))))
    cp, cpc = {}, {}
    for n in nx.topological_sort(g):
        cp[n] = tasks[n] + max((cp[p] for p in g.predecessors(n)), default=0.0)
        cpc[n] = tasks[n] + max((cpc[p] + g.edges[p, n]["b"] / bandwidth for p in g.predecessors(n)),
                                default=0.0)
    total = sum(tasks.values())
    return {
        "tasks": len(tasks), "edges": len(edges), "depth": max(level.values()), "width": width,
        "sources": sum(1 for n in g if g.in_degree(n) == 0), "sinks": sum(1 for n in g if g.out_degree(n) == 0),
        "max_in_degree": max(d for _, d in g.in_degree()), "max_out_degree": max(d for _, d in g.out_degree()),
        "total_work_s": total, "mean_cost_s": total / len(tasks), "max_cost_s": max(tasks.values()),
        "total_bytes": int(sum(b for _, _, b in edges)), "max_edge_bytes": int(max((b for *_, b in edges), default=0)),
        "cp_compute_s": max(cp.values()),                 # longest path of costs (speed 1.0, no comm)
        "cp_with_inter_comm_s": max(cpc.values()),         # + every edge at B_INTER (upper bound)
        "parallelism": total / max(cp.values()),
    }


def gname(family: str, size: str, g: float, ccr_label: str) -> str:
    return f"e3-{family}-{size}-g{g:g}-{ccr_label}".replace(".", "p")


def run_file(graph: str, scheduler: str, family: str, size: str, g: float, ccr_label: str) -> dict:
    return {
        "_note": f"E3: {family} {size}, granularity {g:g}s, CCR {ccr_label} ({CCRS[ccr_label]}); "
                 "made by experiments/slurm/e3/make_graphs.py",
        "instance": f"graphs/{graph}.json",
        "scheduler": scheduler,
        "enact_order": "order",
        "sec_per_cost": 1.0,
        "bytes_per_size": 1.0,
        "mode": "cpu",
        "transport": "direct",
        "mem": MEM,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="print the stats, write nothing")
    args = ap.parse_args()

    manifest = {"_doc": "E3 graphs: made by experiments/slurm/e3/make_graphs.py (see its docstring)",
                "ccr_definition": "mean_edges(bytes / B_INTER) / mean_tasks(cost at speed 1.0)",
                "B_INTER": B_INTER, "EDGE_CAP": EDGE_CAP, "TOTAL_CAP": TOTAL_CAP, "MIN_COST": MIN_COST,
                "workflows": {}, "graphs": {}}
    for (family, size), src in WORKFLOWS.items():
        if "trace" in src:
            graph, names = load_trace(*src["trace"])
            source = {"kind": "trace", "repo": TRACE_REPO, "path": "/".join([src["trace"][0], "chameleon-cloud",
                                                                             src["trace"][1]])}
        else:
            graph, names = load_recipe(family, src["recipe"])
            source = {"kind": "recipe", "recipe": family, "num_tasks": src["recipe"], "seed": RECIPE_SEED}
        ren = sanitize_all({n: names.get(n, str(n)) for n in nx.topological_sort(graph)})
        manifest["workflows"][f"{family}-{size}"] = {"source": source, "names": ren}
        for g in GRANULARITIES:
            for label, ccr in CCRS.items():
                r = rescale(graph, names, g, ccr)
                name = gname(family, size, g, label)
                stats = graph_stats(r["tasks"], r["edges"])
                included = r["ccr"] >= FEASIBLE * ccr
                manifest["graphs"][name] = {"included": included,
                                            "family": family, "size": size, "granularity": g, "ccr_label": label,
                                            "ccr_target": ccr, "ccr": r["ccr"], "flags": r["flags"],
                                            "workflow": f"{family}-{size}", "stats": stats}
                print(f"{name:34s} n={stats['tasks']:3d} e={stats['edges']:3d} depth={stats['depth']:2d} "
                      f"width={stats['width']:3d} cp={stats['cp_compute_s']:7.1f}s work={stats['total_work_s']:7.1f}s "
                      f"bytes={stats['total_bytes'] / 1e9:6.2f}G ccr={r['ccr']:.3f} {'' if included else 'EXCLUDED '}"
                      f"{'; '.join(r['flags'])}")
                if args.check or not included:
                    continue
                inst = {"name": name,
                        "source": f"WfCommons {source.get('path', source)} via saga.schedulers.data.wfcommons; "
                                  f"mean cost {g:g}s, CCR {r['ccr']:.4g} (target {ccr}); "
                                  "experiments/slurm/e3/make_graphs.py",
                        "tasks": {k: round(v, 9) for k, v in r["tasks"].items()},
                        "edges": r["edges"]}
                (GRAPHS / f"{name}.json").write_text(json.dumps(inst, indent=1) + "\n")
                RUNS.mkdir(parents=True, exist_ok=True)
                for s in SCHEDULERS:
                    (RUNS / f"{name}-{s}.json").write_text(
                        json.dumps(run_file(name, s, family, size, g, label), indent=2) + "\n")
    if not args.check:
        MANIFEST.write_text(json.dumps(manifest, indent=1) + "\n")
        n = sum(g["included"] for g in manifest["graphs"].values())
        print(f"wrote {n} graphs ({len(manifest['graphs']) - n} cells excluded), {n * len(SCHEDULERS)} run files, "
              f"{MANIFEST.relative_to(ROOT)}")


if __name__ == "__main__":
    sys.exit(main())
