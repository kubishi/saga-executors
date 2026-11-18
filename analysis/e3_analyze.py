#!/usr/bin/env python3
"""E3 analysis: measured Slurm runs vs SAGA's model variants (analysis/e3_model.py).

    uv run python analysis/e3_analyze.py                         # runs listed in results/e3/runs.csv
    uv run python analysis/e3_analyze.py --runs ~/slurm-logs/saga/2026111*   # any run folders
    uv run python analysis/e3_analyze.py --replan                # + SAGA re-planning under each variant

Reads each run's prediction.json / actual.json / summary.json (read-only) and, for
every variant a-e, replays the run's fixed mapping and per-core order. Writes to
--out (default results/e3/, gitignored):

  per_run.csv   one row per run x variant: predicted vs measured makespan, relative
                error (pred - meas) / meas, mean |start error| (replay work start vs
                measured work start, s and / measured makespan), mean relative work
                error (runtime vs measured work; the same for every variant)
  summary.csv   mean relative / absolute error per family, size, granularity, CCR and
                variant, and the improvement of each added term (|err| of the previous
                variant - |err| of this one, in percentage points)
  ranking.csv   per graph (cell): HEFT vs CPOP as measured (mean over reps) and as
                predicted by each variant, via the replay of the run schedules
                ("replay") and, with --replan, by SAGA planning under the variant
                ("replan"); ties = difference below --tie of the mean
  fig_*.png     error by variant and granularity / CCR, term improvements, predicted vs measured

Runs without E3 metadata (e.g. E0/E1) are labelled by their instance name.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import e3_model as m

LOGS = Path.home() / "slurm-logs" / "saga"
GRAPHS_MANIFEST = ROOT / "experiments" / "slurm" / "e3" / "graphs.json"
COLORS = {"a": "#86b6ef", "b": "#5598e7", "c": "#2a78d6", "d": "#1c5cab", "e": "#0d366b"}   # one hue, cumulative
KEYS = ("family", "size", "granularity", "ccr_label")


def load_meta() -> dict:
    try:
        return json.loads(GRAPHS_MANIFEST.read_text())["graphs"]
    except (OSError, ValueError):
        return {}


def run_dirs_from_manifest(path: Path, logs: Path) -> list[Path]:
    with path.open() as f:
        return [logs / r["run_id"] for r in csv.DictReader(f) if r["status"] == "ok" and r["run_id"]]


def load_run(d: Path, meta: dict) -> dict | None:
    try:
        pred = json.loads((d / "prediction.json").read_text())
        actual = json.loads((d / "actual.json").read_text())
    except (OSError, ValueError):
        return None
    try:
        summary = json.loads((d / "summary.json").read_text())
    except (OSError, ValueError):
        summary = {}
    instance = pred.get("meta", {}).get("instance") or summary.get("instance", {}).get("name") or d.name
    scheduler = pred.get("meta", {}).get("scheduler") or summary.get("scheduler", "?")
    g = meta.get(instance, {})
    return {"run_id": d.name, "dir": d, "pred": pred, "actual": actual, "instance": instance,
            "scheduler": scheduler,
            "family": g.get("family", instance), "size": g.get("size", ""),
            "granularity": g.get("granularity", ""), "ccr_label": g.get("ccr_label", ""),
            "ccr": g.get("ccr", "")}


def analyze_run(run: dict, variants=m.VARIANTS) -> list[dict]:
    pred, actual = run["pred"], run["actual"]
    prob = m.Problem.from_prediction(pred)
    meas = actual["makespan"]
    tasks = actual["tasks"]
    work_err = []
    for t, a in tasks.items():
        w = (a.get("durations") or {}).get("work")
        if w is not None and prob.runtime[t] > 0:
            work_err.append((prob.runtime[t] - w) / w if w > 0 else 0.0)
    rows = []
    base_diffs = m.verify_base(pred)
    for k, v in variants.items():
        r = m.replay(prob, v)
        errs = [r["tasks"][t]["start"] - a["times"]["work_start"] for t, a in tasks.items()
                if (a.get("times") or {}).get("work_start") is not None]
        rows.append({
            "run_id": run["run_id"], "instance": run["instance"], "scheduler": run["scheduler"],
            **{key: run[key] for key in KEYS}, "ccr": run["ccr"], "tasks": len(prob.tasks),
            "variant": k, "variant_label": v.label,
            "saga_makespan": pred["makespan"], "pred_makespan": r["makespan"], "meas_makespan": meas,
            "meas_makespan_work": actual.get("makespan_work"),
            "rel_err": (r["makespan"] - meas) / meas if meas else None,
            "mean_abs_start_err": statistics.mean(abs(e) for e in errs) if errs else None,
            "mean_abs_start_err_rel": (statistics.mean(abs(e) for e in errs) / meas) if errs and meas else None,
            "mean_start_err": statistics.mean(errs) if errs else None,
            "mean_rel_work_err": statistics.mean(work_err) if work_err else None,
            "replay_a_equals_saga": not base_diffs,
        })
    return rows


def _mean(xs):
    xs = [x for x in xs if x is not None and x != ""]
    return statistics.mean(xs) if xs else None


def summarize(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        for key in [tuple(r[k] for k in KEYS), ("all", "", "", ""),
                    ("all", "", r["granularity"], ""), ("all", "", "", r["ccr_label"]),
                    ("all", "", r["granularity"], r["ccr_label"])]:
            groups[key + (r["variant"],)].append(r)
    out = []
    for key, rs in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        out.append({**dict(zip(KEYS + ("variant",), key)), "n_runs": len(rs),
                    "mean_rel_err": _mean(r["rel_err"] for r in rs),
                    "mean_abs_rel_err": _mean(abs(r["rel_err"]) for r in rs if r["rel_err"] is not None),
                    "mean_abs_start_err_rel": _mean(r["mean_abs_start_err_rel"] for r in rs),
                    "mean_rel_work_err": _mean(r["mean_rel_work_err"] for r in rs)})
    by = {tuple(o[k] for k in KEYS + ("variant",)): o for o in out}
    order = list(m.VARIANTS)
    for o in out:
        i = order.index(o["variant"])
        prev = by.get(tuple(o[k] for k in KEYS) + (order[i - 1],)) if i else None
        o["improvement_pp"] = (100 * (prev["mean_abs_rel_err"] - o["mean_abs_rel_err"])
                               if prev and prev["mean_abs_rel_err"] is not None else None)
    return out


def winner(heft: float, cpop: float, tie: float) -> str:
    if abs(heft - cpop) <= tie * (heft + cpop) / 2:
        return "tie"
    return "heft" if heft < cpop else "cpop"


def ranking(runs: list[dict], rows: list[dict], tie: float, replan: bool, cache: Path) -> list[dict]:
    by_cell: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by_cell[r["instance"]][(r["scheduler"], r["variant"])].append(r)
    requests = {}
    for run in runs:
        requests.setdefault(run["instance"], run["pred"]["request"])
    out = []
    for inst, d in sorted(by_cell.items()):
        scheds = {s for s, _ in d}
        if not {"heft", "cpop"} <= scheds:
            continue
        meas = {s: statistics.mean(r["meas_makespan"] for r in d[(s, "a")]) for s in ("heft", "cpop")}
        mw = winner(meas["heft"], meas["cpop"], tie)
        first = d[("heft", "a")][0]
        for k, v in m.VARIANTS.items():
            pr = {s: d[(s, k)][0]["pred_makespan"] for s in ("heft", "cpop")}
            methods = [("replay", pr, k)]
            if replan:
                rp = {s: m.replan(requests[inst], v, s, cache) for s in ("heft", "cpop")}
                methods.append(("replan", {s: rp[s]["makespan"] for s in rp}, rp["heft"]["expressed"]))
            for method, p, expressed in methods:
                pw = winner(p["heft"], p["cpop"], tie)
                out.append({"instance": inst, **{key: first[key] for key in KEYS}, "variant": k,
                            "method": method, "expressed_as": expressed,
                            "pred_heft": p["heft"], "pred_cpop": p["cpop"], "pred_winner": pw,
                            "meas_heft": meas["heft"], "meas_cpop": meas["cpop"], "meas_winner": mw,
                            "n_heft": len(d[("heft", "a")]), "n_cpop": len(d[("cpop", "a")]),
                            "agree": pw == mw})
    return out


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def figures(rows: list[dict], summary: list[dict], out: Path) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
                         "grid.color": "#e4e3df", "grid.linewidth": 0.6, "axes.axisbelow": True,
                         "font.size": 9})
    variants = list(m.VARIANTS)
    labels = {k: f"{k} {v.label}" for k, v in m.VARIANTS.items()}
    paths = []

    def bars(ax, rows, grp, groups, metric, title, ylabel):
        width = 0.8 / len(variants)
        for i, k in enumerate(variants):
            ys = [_mean(metric(r) for r in rows if r["variant"] == k and grp(r) == g) for g in groups]
            ax.bar([j + (i - (len(variants) - 1) / 2) * width for j in range(len(groups))],
                   [100 * (y or 0) for y in ys], width * 0.9, color=COLORS[k], label=labels[k])
        ax.set_xticks(range(len(groups)), [str(g) for g in groups])
        ax.set_title(title, loc="left")
        ax.set_ylabel(ylabel)
        ax.axhline(0, color="#888", lw=0.8)

    # 1. |makespan error| by granularity, one panel per CCR
    ccrs = sorted({r["ccr_label"] for r in rows}, key=str)
    fig, axes = plt.subplots(1, len(ccrs), figsize=(4.5 * len(ccrs), 3.4), squeeze=False, sharey=True)
    for ax, c in zip(axes[0], ccrs):
        sub = [r for r in rows if r["ccr_label"] == c]
        groups = sorted({r["granularity"] for r in sub}, key=str)
        bars(ax, sub, lambda r: r["granularity"], groups,
             lambda r: abs(r["rel_err"]) if r["rel_err"] is not None else None,
             f"CCR {c or '-'}", "mean |makespan error| (%)")
        ax.set_xlabel("granularity (s)")
    axes[0][0].legend(frameon=False, fontsize=8)
    fig.suptitle("Makespan error by model variant", x=0.01, ha="left")
    fig.tight_layout()
    p = out / "fig_makespan_error.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    paths.append(p)

    # 2. signed error by family/size
    fig, ax = plt.subplots(figsize=(8, 3.4))
    grp = lambda r: f"{r['family']} {r['size']}".strip()
    bars(ax, rows, grp, sorted({grp(r) for r in rows}), lambda r: r["rel_err"], "Signed makespan error by workflow",
         "mean (pred - meas) / meas (%)")
    ax.legend(frameon=False, fontsize=8, ncol=5)
    fig.tight_layout()
    p = out / "fig_error_by_workflow.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    paths.append(p)

    # 3. improvement of each added term, by granularity (all CCRs)
    sub = [s for s in summary if s["family"] == "all" and s["ccr_label"] == "" and s["granularity"] != ""
           and s["variant"] != "a"]
    gs = sorted({s["granularity"] for s in sub}, key=str)
    if sub:
        fig, ax = plt.subplots(figsize=(6, 3.4))
        width = 0.8 / (len(variants) - 1)
        for i, k in enumerate(variants[1:]):
            ys = [next((s["improvement_pp"] for s in sub if s["variant"] == k and s["granularity"] == g), None)
                  for g in gs]
            ax.bar([j + (i - (len(variants) - 2) / 2) * width for j in range(len(gs))],
                   [y or 0 for y in ys], width * 0.9, color=COLORS[k], label=labels[k])
        ax.set_xticks(range(len(gs)), [str(g) for g in gs])
        ax.axhline(0, color="#888", lw=0.8)
        ax.set_xlabel("granularity (s)")
        ax.set_ylabel("|error| reduction (pp)")
        ax.set_title("What each added term buys", loc="left")
        ax.legend(frameon=False, fontsize=8)
        fig.tight_layout()
        p = out / "fig_term_improvement.png"
        fig.savefig(p, dpi=150)
        plt.close(fig)
        paths.append(p)

    # 4. predicted vs measured
    fig, ax = plt.subplots(figsize=(4.6, 4.2))
    for k in ("a", "c", "e"):
        xs = [r["meas_makespan"] for r in rows if r["variant"] == k]
        ys = [r["pred_makespan"] for r in rows if r["variant"] == k]
        ax.scatter(xs, ys, s=18, color=COLORS[k], label=labels[k], edgecolor="white", linewidth=0.6)
    lim = max([r["meas_makespan"] for r in rows] + [r["pred_makespan"] for r in rows]) * 1.1
    ax.plot([0.5, lim], [0.5, lim], color="#888", lw=0.8)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("measured makespan (s)")
    ax.set_ylabel("predicted makespan (s)")
    ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    p = out / "fig_pred_vs_meas.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    paths.append(p)
    return paths


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs", nargs="*", type=Path, help="run folders (default: the manifest's ok runs)")
    ap.add_argument("--manifest", type=Path, default=ROOT / "results" / "e3" / "runs.csv")
    ap.add_argument("--logs", type=Path, default=LOGS)
    ap.add_argument("--out", type=Path, default=ROOT / "results" / "e3")
    ap.add_argument("--tie", type=float, default=0.01, help="relative difference treated as a tie")
    ap.add_argument("--replan", action="store_true", help="also re-plan with SAGA under each variant (bridge)")
    ap.add_argument("--no-figures", action="store_true")
    args = ap.parse_args(argv)

    dirs = args.runs if args.runs else run_dirs_from_manifest(args.manifest, args.logs)
    meta = load_meta()
    runs = [r for r in (load_run(Path(d), meta) for d in dirs) if r is not None]
    if not runs:
        sys.exit("no runs with prediction.json + actual.json")
    rows = [row for run in runs for row in analyze_run(run)]
    summary = summarize(rows)
    rank = ranking(runs, rows, args.tie, args.replan, args.out / "replan-cache")
    args.out.mkdir(parents=True, exist_ok=True)
    write_csv(args.out / "per_run.csv", rows)
    write_csv(args.out / "summary.csv", summary)
    write_csv(args.out / "ranking.csv", rank)
    figs = [] if args.no_figures else figures(rows, summary, args.out)

    bad = sorted({r["run_id"] for r in rows if not r["replay_a_equals_saga"]})
    print(f"{len(runs)} runs; replay(a) = SAGA's prediction in {len(runs) - len(bad)}/{len(runs)}"
          + (f" (differs: {bad})" if bad else ""))
    print(f"{'variant':12s} {'mean err':>9s} {'mean |err|':>10s} {'|start err|/mk':>15s}")
    for s in summary:
        if s["family"] == "all" and s["granularity"] == "" and s["ccr_label"] == "":
            print(f"{s['variant'] + ' ' + m.VARIANTS[s['variant']].label:12s} {100 * s['mean_rel_err']:8.1f}% "
                  f"{100 * s['mean_abs_rel_err']:9.1f}% {100 * (s['mean_abs_start_err_rel'] or 0):14.1f}%")
    if rank:
        for method in ("replay", "replan"):
            for k in m.VARIANTS:
                rs = [r for r in rank if r["method"] == method and r["variant"] == k]
                if rs:
                    print(f"rank agreement {method:6s} {k}: {sum(r['agree'] for r in rs)}/{len(rs)} cells")
    print(f"wrote {args.out}/per_run.csv, summary.csv, ranking.csv" + (f", {len(figs)} figures" if figs else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
