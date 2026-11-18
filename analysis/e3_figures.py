#!/usr/bin/env python3
"""Figures for the Slurm preliminary experiments (E0, E2, E3) and the Wayline smoke runs.

Reads results/e3/{per_run,ranking}.csv (written by analysis/e3_analyze.py), the E3
run folders in ~/slurm-logs/saga/ (for link load), and the Wayline smoke runs in
results/wayline/. Writes PNGs to results/e3/figures/.

    uv run python analysis/e3_figures.py
"""
from __future__ import annotations

import csv
import json
import re
import statistics as st
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
E3 = ROOT / "results" / "e3"
OUT = E3 / "figures"
LOGS = Path.home() / "slurm-logs" / "saga"

# Reference palette (dataviz skill, light mode).
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e1e0d9", "#fcfcfb"
CAT = ["#2a78d6", "#eb6834", "#1baf7a"]                     # blue, orange, aqua (all-pairs safe)
GRAN_COLOR = {"0.5": "#5598e7", "2.0": "#256abf", "10.0": "#0d366b"}   # sequential blue: small -> large
VARIANTS = ["a", "b", "c", "d", "e"]
VLABEL = {"a": "base", "b": "+ measured\nlinks", "c": "+ launch\noverhead", "d": "+ launch\nstagger",
          "e": "+ serial\nreads/writes"}
INTER_BW = 108e6                                             # measured lambda1<->isha, B/s (E2 mean)

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.titleweight": "bold",
    "axes.titlesize": 11, "axes.titlelocation": "left", "font.size": 10, "legend.frameon": False,
    "lines.linewidth": 2, "lines.markersize": 7,
})


def gran_label(g: str) -> str:
    return f"{float(g):g} s tasks"


def load_rows() -> list[dict]:
    return list(csv.DictReader(open(E3 / "per_run.csv")))


def save(fig, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / name, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", OUT / name)


# ---------------------------------------------------------------------------

def fig_ladder(rows) -> None:
    """Mean |makespan error| as model terms are added, per granularity; panels by CCR."""
    err = defaultdict(list)
    for r in rows:
        err[(r["ccr_label"], r["granularity"], r["variant"])].append(abs(float(r["rel_err"])))
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), sharey=True)
    for ax, ccr, title in zip(axes, ("lo", "hi"), ("Low communication (CCR ~0.1)", "High communication (CCR ~1)")):
        for g in ("0.5", "2.0", "10.0"):
            ys = [100 * st.mean(err[(ccr, g, v)]) if err[(ccr, g, v)] else None for v in VARIANTS]
            if all(y is None for y in ys):
                continue
            xs = range(len(VARIANTS))
            ax.plot(xs, ys, marker="o", color=GRAN_COLOR[g], markeredgecolor=SURFACE, markeredgewidth=1.5)
            ax.annotate(gran_label(g), (len(VARIANTS) - 1, ys[-1]), xytext=(8, 0), textcoords="offset points",
                        va="center", color=INK2, fontsize=9)
        ax.set_xticks(range(len(VARIANTS)), [VLABEL[v] for v in VARIANTS], fontsize=8.5)
        ax.set_title(title)
        ax.set_xlim(-0.3, len(VARIANTS) - 0.3)
    top = max(100 * st.mean(v) for v in err.values() if v)
    axes[0].set_ylim(0, top * 1.08)
    axes[0].set_ylabel("Mean |makespan error| (%)")
    fig.suptitle("Prediction error as terms are added to SAGA's model (each adds to the previous)",
                 x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.text(0.01, -0.04, "E3 on Slurm: 6 WfCommons workflows, HEFT + CPOP, 3 reps each. "
             "SAGA under-predicts in every run. No 10 s cells at high CCR (would need 50-200 GB per run).",
             color=INK2, fontsize=8.5)
    fig.tight_layout()
    save(fig, "1_model_ladder.png")


def fig_launch_term(rows) -> None:
    """Improvement from the launch-overhead term (b -> c), by granularity and workflow size."""
    by = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by[(r["run_id"])][r["variant"]] = r
    imp = defaultdict(list)
    for rid, vs in by.items():
        if "b" in vs and "c" in vs:
            b, c = vs["b"], vs["c"]
            imp[(b["granularity"], b["size"])].append(100 * (abs(float(b["rel_err"])) - abs(float(c["rel_err"]))))
    grans = ["0.5", "2.0", "10.0"]
    fig, ax = plt.subplots(figsize=(6.4, 4))
    width = 0.36
    for i, (size, label) in enumerate((("s", "small workflows (41-58 tasks)"), ("l", "large workflows (125-178 tasks)"))):
        xs, ys = [], []
        for j, g in enumerate(grans):
            if imp[(g, size)]:
                xs.append(j + (i - 0.5) * width)
                ys.append(st.mean(imp[(g, size)]))
        bars = ax.bar(xs, ys, width=width - 0.04, color=CAT[i], label=label, edgecolor=SURFACE, linewidth=2, zorder=3)
        for x, y in zip(xs, ys):
            ax.annotate(f"{y:.1f}", (x, y), xytext=(0, 3), textcoords="offset points", ha="center",
                        fontsize=8.5, color=INK2)
    ax.set_xticks(range(len(grans)), [gran_label(g) for g in grans])
    ax.set_ylabel("Error reduction (percentage points)")
    ax.set_title("What modelling Slurm's per-task launch overhead buys")
    ax.legend(loc="upper right")
    ax.grid(axis="x", visible=False)
    fig.text(0.01, -0.06, "Mean reduction in |makespan error| from adding a per-task launch overhead "
             "(108 ms lambda1, 126 ms isha) to the measured-links model.\nNo large workflows at 10 s.",
             color=INK2, fontsize=8.5)
    fig.tight_layout()
    save(fig, "2_launch_term_by_granularity.png")


def fig_pred_vs_meas(rows) -> None:
    fams = sorted({r["family"] for r in rows})
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.6), sharex=True, sharey=True)
    for ax, v, title in zip(axes, ("a", "e"), ("SAGA's model (base)", "With launch overhead, stagger and serial I/O")):
        for i, f in enumerate(fams):
            pts = [(float(r["meas_makespan"]), float(r["pred_makespan"])) for r in rows
                   if r["variant"] == v and r["family"] == f]
            ax.scatter(*zip(*pts), s=28, color=CAT[i], edgecolor=SURFACE, linewidth=1, label=f, zorder=3)
        lo, hi = 1.5, 150
        ax.plot([lo, hi], [lo, hi], color=INK2, linewidth=1, zorder=2)
        ax.annotate("perfect prediction", (40, 40), xytext=(6, -14), textcoords="offset points",
                    color=INK2, fontsize=8.5, rotation=36)
        ax.set_xscale("log"); ax.set_yscale("log")
        ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)
        ax.set_title(title)
        ax.set_xlabel("Measured makespan (s)")
    axes[0].set_ylabel("Predicted makespan (s)")
    axes[0].legend(loc="upper left", title="workflow", title_fontsize=9)
    fig.text(0.01, -0.03, "One point per E3 run (144). Points below the line: SAGA predicted too short. "
             "Prediction replays the run's own mapping and per-machine order.", color=INK2, fontsize=8.5)
    fig.tight_layout()
    save(fig, "3_predicted_vs_measured.png")


def link_load(run_id: str) -> dict | None:
    """Cross-machine pulls in a direct-transport run: busiest-direction peak concurrency
    and bytes over the link as a fraction of what the link could carry in the makespan."""
    d = LOGS / run_id
    try:
        res = json.loads((d / "results.json").read_text())
        act = json.loads((d / "actual.json").read_text())
    except (OSError, ValueError):
        return None
    xs = [x for x in res.get("transfers", []) if x.get("bytes")]
    by_dir = defaultdict(list)
    for x in xs:
        by_dir[x["server_node"]].append(x)
    peak, demand = 0, 0.0
    for dir_xs in by_dir.values():
        ev = sorted([(x["start"], 1) for x in dir_xs] + [(x["end"], -1) for x in dir_xs])
        cur = 0
        for _, k in ev:
            cur += k
            peak = max(peak, cur)
        demand = max(demand, sum(x["bytes"] for x in dir_xs) / INTER_BW / act["makespan"])
    return {"peak": peak, "demand": demand}


def fig_contention(rows) -> None:
    """Remaining error (most detailed variant) vs load on the lambda1<->isha link."""
    fams = sorted({r["family"] for r in rows})
    fig, ax = plt.subplots(figsize=(6.6, 4.4))
    allx, ally = [], []
    for i, f in enumerate(fams):
        pts, local = [], []
        for r in rows:
            if r["variant"] != "e" or r["family"] != f:
                continue
            ll = link_load(r["run_id"])
            if ll:
                (pts if ll["peak"] else local).append((100 * ll["demand"], 100 * abs(float(r["rel_err"]))))
        if pts:
            ax.scatter(*zip(*pts), s=28, color=CAT[i], edgecolor=SURFACE, linewidth=1, label=f, zorder=3)
        if local:      # all tasks on one machine: no link traffic (hollow)
            ax.scatter(*zip(*local), s=30, facecolor=SURFACE, edgecolor=CAT[i], linewidth=1.5, zorder=3)
        allx += [p[0] for p in pts + local]; ally += [p[1] for p in pts + local]
    ax.annotate("hollow: SAGA put every task on one machine (no link traffic);\n"
                "high-data ones still miss by 20-50%, varying a lot between reps",
                (1.5, 62), color=INK2, fontsize=8, va="bottom")
    if len(allx) > 2:
        r = st.correlation(allx, ally)
        ax.annotate(f"Pearson r = {r:.2f} (n = {len(allx)})", (0.98, 0.04), xycoords="axes fraction",
                    ha="right", color=INK2, fontsize=9)
    ax.set_xlabel("Link demand on the busiest direction (% of link capacity over the makespan)")
    ax.set_ylabel("Remaining |makespan error| (%)")
    ax.set_title("What the detailed model still misses tracks network load")
    ax.legend(loc="lower right", bbox_to_anchor=(1.0, 0.08), title="workflow", title_fontsize=9)
    ax.set_xlim(0, None); ax.set_ylim(0, None)
    fig.text(0.01, -0.05, "Remaining error = model with measured links, launch overhead, stagger and serial I/O. "
             "No variant models\ncontention: SAGA gives every transfer the full ~108 MB/s link at once.",
             color=INK2, fontsize=8.5)
    fig.tight_layout()
    save(fig, "4_residual_error_vs_link_load.png")


def fig_ranking() -> None:
    rows = [r for r in csv.DictReader(open(E3 / "ranking.csv")) if r["variant"] == "a" and r["method"] == "replay"]
    fig, ax = plt.subplots(figsize=(6, 5))
    lim = 0
    for r in rows:
        px = float(r["pred_cpop"]) / float(r["pred_heft"])
        my = float(r["meas_cpop"]) / float(r["meas_heft"])
        lim = max(lim, abs(px - 1), abs(my - 1))
        reversed_ = (px - 1) * (my - 1) < 0 and abs(px - 1) > 0.01 and abs(my - 1) > 0.01
        ax.scatter(px, my, s=40 if reversed_ else 28, color=CAT[1] if reversed_ else CAT[0],
                   edgecolor=SURFACE, linewidth=1, zorder=3)
        if reversed_:
            ax.annotate(r["instance"].removeprefix("e3-"), (px, my), xytext=(7, -12), textcoords="offset points",
                        fontsize=8, color=INK2)
    lim = lim * 1.15
    ax.axhline(1, color=INK2, linewidth=1); ax.axvline(1, color=INK2, linewidth=1)
    ax.set_xlim(1 - lim, 1 + lim); ax.set_ylim(1 - lim, 1 + lim)
    ax.set_xlabel("Predicted makespan ratio, CPOP schedule / HEFT schedule")
    ax.set_ylabel("Measured makespan ratio, CPOP / HEFT")
    ax.set_title("Does the model order the two schedules correctly?")
    for (x, y, t) in ((1 + lim * 0.55, 1 + lim * 0.9, "both say HEFT's\nschedule is faster"),
                      (1 - lim * 0.55, 1 - lim * 0.9, "both say CPOP's\nschedule is faster")):
        ax.annotate(t, (x, y), ha="center", va="center", color=INK2, fontsize=8.5)
    fig.text(0.01, -0.06, "One point per E3 configuration (24; base model, mean of 3 runs per schedule). "
             "Orange: order reversed\n(both ratios off 1 by more than 1%). Points on the vertical line: "
             "SAGA predicts the same makespan for both.", color=INK2, fontsize=8.5)
    fig.tight_layout()
    save(fig, "5_schedule_ordering.png")


def fig_overhead() -> None:
    """Per-dependency overhead with no data: Slurm (E0, E2 0-byte) vs Wayline (smoke)."""
    slurm, wayline = [], []
    for d in sorted(LOGS.glob("calib-direct-*")):
        m = re.match(r"calib-direct-(self|intra|inter)-cse-[\w]+-(\d+)-\d+-\d+$", d.name)
        if not m or m.group(2) != "0":              # 0-byte chains only
            continue
        try:
            res = json.loads((d / "results.json").read_text())["tasks"]
        except (OSError, ValueError, KeyError):
            continue
        for p in [k for k in res if k.startswith("p")]:
            c = res.get("c" + p[1:], {})
            if res[p].get("state") == "COMPLETED" and c.get("state") == "COMPLETED":
                pt, ct = res[p]["timings"]["timeline"], c["timings"]["timeline"]
                slurm.append(ct["workStartUnix"] - pt["workEndUnix"])
    for f in (ROOT / "results" / "wayline").glob("*/*/runs/*/compare.json"):
        c = json.loads(f.read_text())
        edges = c["edges"].values() if isinstance(c["edges"], dict) else c["edges"]
        for e in edges:
            if e["saga"]["gap"] < 1e-3:           # no planned slack (only ~1e-5 s transfers): the gap is overhead
                wayline.append(e["actual"]["work_gap"])
    if not slurm or not wayline:
        print("skip overhead figure (missing runs)")
        return
    fig, ax = plt.subplots(figsize=(6, 2.8))
    vals = [("Slurm (srun step)", slurm), ("Wayline (pod)", wayline)]
    for i, (name, xs) in enumerate(vals):
        m = st.median(xs)
        ax.barh(i, m, height=0.5, color=CAT[0], edgecolor=SURFACE, linewidth=2, zorder=3)
        ax.annotate(f"{1000 * m:.0f} ms  (n = {len(xs)})", (m, i), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK2)
    ax.set_yticks(range(len(vals)), [v[0] for v in vals])
    ax.invert_yaxis()
    ax.set_xlabel("Median gap between a producer finishing and its consumer starting work (s)")
    ax.set_title("Overhead per dependency, no data, nothing else running")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, max(st.median(x) for _, x in vals) * 1.35)
    fig.text(0.01, -0.12, "Slurm: E2 0-byte chains (all link kinds). Wayline: smoke diamond edges with no planned "
             "slack (2 runs, preliminary).", color=INK2, fontsize=8.5)
    fig.tight_layout()
    save(fig, "6_overhead_per_dependency.png")


def main() -> None:
    rows = load_rows()
    fig_ladder(rows)
    fig_launch_term(rows)
    fig_pred_vs_meas(rows)
    fig_contention(rows)
    fig_ranking()
    fig_overhead()


if __name__ == "__main__":
    main()
