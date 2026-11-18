"""E3 (experiments/slurm/e3, analysis/e3_*.py): graph rescaling, names, replay, analysis. Offline."""

import importlib.util
import json
import random
import sys

import networkx as nx
import pytest
from conftest import ROOT

from saga_exec.backends.slurm import saga_slurm

sys.path.insert(0, str(ROOT / "analysis"))
import e3_analyze
import e3_model as m

E3 = ROOT / "experiments" / "slurm" / "e3"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mg = _load("e3_make_graphs", E3 / "make_graphs.py")
rm = _load("e3_run_matrix", E3 / "run_matrix.py")


def _toy(n=30, seed=1):
    rng = random.Random(seed)
    g = nx.gnp_random_graph(n, 0.15, seed=seed, directed=True)
    g = nx.DiGraph([(u, v) for u, v in g.edges if u < v])
    g.add_nodes_from(range(n))
    for v in g.nodes:
        g.nodes[v]["weight"] = rng.lognormvariate(0, 1.5)
    for e in g.edges:
        g.edges[e]["weight"] = rng.lognormvariate(14, 2)
    names = {v: f"Task_Type{v % 3}_ID{v:07d}" for v in g.nodes}
    return g, names


# --- names -----------------------------------------------------------------

@pytest.mark.parametrize("raw", ["mProject_ID0000001", "chr21_chr21_ID0000001", "__X__", "A" * 90, "fast2bfq-1", "Ü@#"])
def test_sanitize_is_dns_label(raw):
    s = mg.sanitize(raw)
    assert mg.DNS_LABEL.match(s) and len(s) <= mg.MAX_NAME
    assert saga_slurm.NAME_RE.match(s)


def test_sanitize_all_unique_and_stable():
    names = {i: n for i, n in enumerate(["mProject-1", "mproject_1", "MPROJECT 1", "x" * 60, "x" * 61])}
    out = mg.sanitize_all(names)
    assert len(set(out.values())) == len(out)
    assert out[0] == "mproject-1" and out[1] == "mproject-1-2" and out[2] == "mproject-1-3"
    assert all(mg.DNS_LABEL.match(v) and len(v) <= mg.MAX_NAME for v in out.values())


# --- rescaling ---------------------------------------------------------------

@pytest.mark.parametrize("g", [0.5, 2.0, 10.0])
@pytest.mark.parametrize("ccr", [0.1, 1.0])
def test_rescale_granularity_and_ccr(g, ccr):
    graph, names = _toy()
    r = mg.rescale(graph, names, g, ccr, edge_cap=1e15, total_cap=1e18)
    costs = list(r["tasks"].values())
    assert sum(costs) / len(costs) == pytest.approx(g, rel=1e-9)
    sizes = [b for *_, b in r["edges"]]
    assert sum(sizes) / len(sizes) / mg.B_INTER / g == pytest.approx(ccr, rel=1e-6)
    assert r["ccr"] == pytest.approx(ccr, rel=1e-6) and not r["flags"]
    # relative costs and sizes are kept
    by_orig = {r["names"][v]: v for v in graph.nodes}
    a, b = sorted(r["tasks"])[:2]
    wa, wb = graph.nodes[by_orig[a]]["weight"], graph.nodes[by_orig[b]]["weight"]
    if min(wa, wb) > mg.MIN_COST:
        assert r["tasks"][a] / r["tasks"][b] == pytest.approx(wa / wb, rel=1e-9)
    (u, v, x), (u2, v2, y) = r["edges"][:2]
    assert x / y == pytest.approx(graph.edges[by_orig[u], by_orig[v]]["weight"]
                                  / graph.edges[by_orig[u2], by_orig[v2]]["weight"], rel=1e-5)
    assert set(r["tasks"]) == set(r["names"].values()) and len(r["edges"]) == graph.number_of_edges()


def test_rescale_edge_cap_waterfills():
    graph, names = _toy()
    cap = 5e7
    r = mg.rescale(graph, names, 2.0, 1.0, edge_cap=cap, total_cap=1e18)
    sizes = [b for *_, b in r["edges"]]
    assert max(sizes) <= cap
    assert r["ccr"] == pytest.approx(1.0, rel=1e-6) or r["ccr"] < 1.0 and all(s == cap for s in sizes)


def test_scale_sizes_unreachable_target():
    sizes = mg.scale_sizes({"a": 1.0, "b": 2.0}, target_mean=10.0, cap=4)
    assert sizes == {"a": 4, "b": 4}


def test_rescale_total_cap_flags():
    graph, names = _toy()
    r = mg.rescale(graph, names, 10.0, 1.0, edge_cap=1e15, total_cap=1e9)
    assert sum(b for *_, b in r["edges"]) <= 1e9 + len(r["edges"])
    assert r["ccr"] < 1.0 and any("total" in f for f in r["flags"])


def test_graph_stats():
    s = mg.graph_stats({"a": 1.0, "b": 2.0, "c": 3.0, "d": 1.0},
                       [["a", "b", 0], ["a", "c", int(mg.B_INTER)], ["b", "d", 0], ["c", "d", 0]])
    assert (s["tasks"], s["edges"], s["depth"], s["width"]) == (4, 4, 3, 2)
    assert s["cp_compute_s"] == 5.0 and s["cp_with_inter_comm_s"] == 6.0 and s["total_bytes"] == int(mg.B_INTER)


# --- committed graphs and run files ------------------------------------------

def test_committed_e3_graphs_match_manifest():
    meta = json.loads((E3 / "graphs.json").read_text())
    included = {k: v for k, v in meta["graphs"].items() if v["included"]}
    assert included
    for name, g in included.items():
        inst = json.loads((ROOT / "graphs" / f"{name}.json").read_text())
        costs = list(inst["tasks"].values())
        assert sum(costs) / len(costs) == pytest.approx(g["granularity"], rel=1e-6)
        assert mg.ccr_of(inst["tasks"], inst["edges"]) == pytest.approx(g["ccr"], rel=1e-6)
        assert g["ccr"] >= mg.FEASIBLE * g["ccr_target"]
        assert all(mg.DNS_LABEL.match(t) for t in inst["tasks"])
        assert max((b for *_, b in inst["edges"]), default=0) <= mg.EDGE_CAP
        for s in mg.SCHEDULERS:
            assert (E3 / "runs" / f"{name}-{s}.json").exists()


@pytest.mark.parametrize("path", sorted((E3 / "runs").glob("*.json")), ids=lambda p: p.stem)
def test_e3_run_files_load(path):
    config = saga_slurm.load_run_file(str(path))
    assert saga_slurm.instance_path(config).exists()
    assert (config.mode, config.transport, config.enact_order, config.mem) == ("cpu", "direct", "order", "2G")


def test_runner_subsets_and_resume(tmp_path):
    full, core = rm.select("full", None), rm.select("core", None)
    assert set(core) < set(full)
    assert all("-l-g10-" not in p.stem for p in core)
    assert rm.select("full", ["e3-montage-s-*-heft"]) and all(
        p.stem.startswith("e3-montage-s-") and p.stem.endswith("-heft") for p in rm.select("full", ["e3-montage-s-*-heft"]))
    logs = tmp_path / "logs"
    (logs / "r1").mkdir(parents=True)
    (logs / "r1" / "actual.json").write_text("{}")
    man = tmp_path / "runs.csv"
    rm.append_manifest(man, {"run_file": "x.json", "rep": 0, "run_id": "r1", "status": "ok"})
    rm.append_manifest(man, {"run_file": "x.json", "rep": 1, "run_id": "r2", "status": "ok"})      # no actual.json
    rm.append_manifest(man, {"run_file": "y.json", "rep": 0, "run_id": "", "status": "failed"})
    assert rm.done(rm.read_manifest(man), logs) == {("x.json", 0)}


# --- replay -----------------------------------------------------------------

@pytest.fixture(scope="module")
def predictions():
    """SAGA (bridge) schedules of one small E3 graph, HEFT and CPOP."""
    out = {}
    for s in ("heft", "cpop"):
        p = rm.prepare_offline(E3 / "runs" / f"e3-genome-s-g0p5-hi-{s}.json")
        out[s] = p.prediction
    return out


def test_replay_base_reproduces_saga(predictions):
    for pred in predictions.values():
        assert m.verify_base(pred) == []
        r = m.replay(m.Problem.from_prediction(pred), m.VARIANTS["a"])
        assert r["makespan"] == pytest.approx(pred["makespan"], abs=1e-9)


def test_replay_base_on_e0_graphs():
    for run in ("experiments/slurm/e0/diamond-heft.json", "experiments/slurm/e0/chain-heft.json",
                "experiments/slurm/runs/heft_v_cpop.json"):
        p = saga_slurm.prepare(str(ROOT / run))
        assert m.verify_base(p.prediction) == []


def test_variants_only_add_time(predictions):
    prob = m.Problem.from_prediction(predictions["heft"])
    mk = {k: m.replay(prob, v)["makespan"] for k, v in m.VARIANTS.items()}
    assert mk["a"] <= mk["c"] <= mk["d"] <= mk["e"]
    assert mk["b"] > mk["a"]          # measured links are slower than 1e12 same-machine links


def test_launch_on_chain():
    """Chain of 3 on one core: (c) adds the launch overhead per dependency, (d) nothing more."""
    pred = {"makespan": 3.0, "tasks": {t: {"node": "cse-isha/c0", "machine": "cse-isha", "start": i, "end": i + 1}
                                       for i, t in enumerate("xyz")},
            "request": {"dag": {"tasks": [
                {"name": "x", "dependencies": [], "runtimeProfile": {"cse-isha": 1.0}},
                {"name": "y", "dependencies": ["x"], "runtimeProfile": {"cse-isha": 1.0}},
                {"name": "z", "dependencies": ["y"], "runtimeProfile": {"cse-isha": 1.0}}]},
                "clusterState": {"bandwidth": []}, "model": {}}}
    prob = m.Problem.from_prediction(pred)
    assert m.replay(prob, m.VARIANTS["a"])["makespan"] == pytest.approx(3.0)
    assert m.replay(prob, m.VARIANTS["c"])["makespan"] == pytest.approx(3.0 + 2 * m.LAUNCH["cse-isha"])
    assert m.replay(prob, m.VARIANTS["d"])["makespan"] == pytest.approx(3.0 + 2 * m.LAUNCH["cse-isha"])


def test_stagger_on_independent_tasks():
    n = 5
    pred = {"makespan": 1.0, "tasks": {f"t{i}": {"node": f"cse-lambda1/c{i}", "machine": "cse-lambda1",
                                                 "start": 0.0, "end": 1.0} for i in range(n)},
            "request": {"dag": {"tasks": [{"name": f"t{i}", "dependencies": [], "runtimeProfile": {"cse-lambda1": 1.0}}
                                          for i in range(n)]}, "clusterState": {"bandwidth": []}, "model": {}}}
    prob = m.Problem.from_prediction(pred)
    assert m.replay(prob, m.VARIANTS["c"])["makespan"] == pytest.approx(1.0)
    assert m.replay(prob, m.VARIANTS["d"])["makespan"] == pytest.approx(1.0 + (n - 1) * m.STAGGER)


def test_variant_request_keeps_rank1(predictions):
    req = predictions["heft"]["request"]
    for k, expect in (("a", "a"), ("b", "b"), ("c", "c"), ("d", "c"), ("e", "c")):
        r, expressed = m.variant_request(req, m.VARIANTS[k], "cpop")
        assert expressed == expect and r["algorithm"] == "cpop"
        if expect == "c":
            ratios = {round(t["runtimeProfile"]["cse-lambda1"] / t["runtimeProfile"]["cse-isha"], 9)
                      for t in r["dag"]["tasks"]}
            assert len(ratios) == 1
    assert req["algorithm"] == "heft"                  # input untouched


# --- analysis on fake runs -----------------------------------------------------

def _fake_run(dirpath, pred, variant, noise, seed):
    """A run folder whose actual.json is the replay of `variant` (+ noise)."""
    rng = random.Random(seed)
    r = m.replay(m.Problem.from_prediction(pred), m.VARIANTS[variant])
    tasks = {}
    for t, x in r["tasks"].items():
        j = rng.uniform(0, noise)
        tasks[t] = {"machine": pred["tasks"][t]["machine"],
                    "times": {"task_start": x["launch"] + j, "work_start": x["start"] + j, "work_end": x["end"] + j},
                    "durations": {"work": x["end"] - x["start"]}}
    dirpath.mkdir(parents=True)
    (dirpath / "prediction.json").write_text(json.dumps(pred))
    (dirpath / "actual.json").write_text(json.dumps({"format": "saga-exec-actual/2", "makespan": r["makespan"] + noise,
                                                     "makespan_work": r["makespan"], "tasks": tasks, "edges": {}}))
    (dirpath / "summary.json").write_text(json.dumps({"scheduler": pred["meta"]["scheduler"],
                                                      "run_file": "fake"}))


def test_analysis_on_fake_runs(tmp_path, predictions):
    dirs = []
    for s, pred in predictions.items():
        for rep in range(2):
            d = tmp_path / "logs" / f"{s}-{rep}"
            _fake_run(d, pred, "e", 0.0, rep)
            dirs.append(d)
    out = tmp_path / "out"
    assert e3_analyze.main(["--runs", *map(str, dirs), "--out", str(out)]) == 0
    rows = list(__import__("csv").DictReader((out / "per_run.csv").open()))
    assert len(rows) == 4 * len(m.VARIANTS)
    e_rows = [r for r in rows if r["variant"] == "e"]
    assert all(abs(float(r["rel_err"])) < 1e-9 and float(r["mean_abs_start_err"]) < 1e-9 for r in e_rows)
    a_rows = [r for r in rows if r["variant"] == "a"]
    assert all(float(r["rel_err"]) < 0 for r in a_rows) and all(r["replay_a_equals_saga"] == "True" for r in rows)
    assert all(r["family"] == "genome" and r["granularity"] == "0.5" for r in rows)
    rank = list(__import__("csv").DictReader((out / "ranking.csv").open()))
    assert {r["variant"] for r in rank} == set(m.VARIANTS)
    assert all(r["agree"] == "True" for r in rank if r["variant"] == "e")
    summ = list(__import__("csv").DictReader((out / "summary.csv").open()))
    allrow = {r["variant"]: r for r in summ if r["family"] == "all" and not r["granularity"] and not r["ccr_label"]}
    assert float(allrow["e"]["mean_abs_rel_err"]) < float(allrow["a"]["mean_abs_rel_err"])
    assert (out / "fig_makespan_error.png").exists() and (out / "fig_term_improvement.png").exists()
