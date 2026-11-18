#!/usr/bin/env python3
"""E3 runner: run the matrix of experiments/slurm/e3/runs/*.json sequentially, N reps.

    # offline (never touches Slurm):
    uv run python experiments/slurm/e3/run_matrix.py --estimate [--subset core]
    uv run python experiments/slurm/e3/run_matrix.py --validate          # dry run of every run file
    # on cse-lambda1 (SUBMITS Slurm jobs, one at a time):
    uv run python experiments/slurm/e3/run_matrix.py --subset core --reps 3

Each run is `python -m saga_exec.backends.slurm.saga_slurm <run file>` (= saga-slurm),
results in ~/slurm-logs/saga/<run_id>/. The manifest (default results/e3/runs.csv)
gets one row per finished attempt: run file, rep, run id, status. Resumable: a
(run file, rep) whose row says "ok" and whose actual.json exists is skipped, so
rerunning the same command continues where it stopped. The order is shuffled with
a fixed seed (interleaving HEFT/CPOP and cells, so slow drift does not line up
with a factor); every rep finishes before the next starts.

Subsets: "full" = every run file; "core" = everything except the 10 s cells of
the large graphs (see README.md, time estimate). --only takes fnmatch patterns
on the run file stem (e.g. 'e3-montage-*-heft').
"""

from __future__ import annotations

import argparse
import csv
import fnmatch
import json
import random
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
RUNS = HERE / "runs"
LOGS = Path.home() / "slurm-logs" / "saga"
DEFAULT_MANIFEST = ROOT / "results" / "e3" / "runs.csv"
JOB_OVERHEAD = 8.0          # s per run besides the makespan: SAGA (bridge), sbatch start, sbcast, servers, polling
FIELDS = ["run_file", "rep", "run_id", "status", "started", "finished", "seconds", "note"]

sys.path.insert(0, str(ROOT / "analysis"))


def graph_meta() -> dict:
    return json.loads((HERE / "graphs.json").read_text())["graphs"]


def in_subset(stem: str, subset: str, meta: dict) -> bool:
    graph = stem.rsplit("-", 1)[0]
    g = meta.get(graph)
    if g is None:
        return False
    if subset == "full":
        return True
    if subset == "core":
        return not (g["size"] == "l" and g["granularity"] >= 10)
    raise ValueError(f"unknown subset {subset!r}")


def select(subset: str, only: list[str] | None) -> list[Path]:
    meta = graph_meta()
    files = [p for p in sorted(RUNS.glob("*.json")) if in_subset(p.stem, subset, meta)]
    if only:
        files = [p for p in files if any(fnmatch.fnmatch(p.stem, pat) for pat in only)]
    return files


def read_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def append_manifest(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in FIELDS})


def done(rows: list[dict], logs: Path = LOGS) -> set[tuple[str, int]]:
    return {(r["run_file"], int(r["rep"])) for r in rows
            if r["status"] == "ok" and (logs / r["run_id"] / "actual.json").exists()}


def rel(p: Path) -> str:
    return str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p)


# ---------------------------------------------------------------------------
# Offline: estimate and validate
# ---------------------------------------------------------------------------

def prepare_offline(path: Path, dry_run: bool = True):
    """saga_slurm.prepare on a copy of the run file with dry_run set (nothing is submitted)."""
    import tempfile

    from saga_exec.backends.slurm import saga_slurm
    spec = json.loads(path.read_text())
    spec["dry_run"] = dry_run
    spec["instance"] = str(ROOT / spec["instance"])
    with tempfile.NamedTemporaryFile("w", suffix=".json", prefix=path.stem + "-") as f:
        json.dump(spec, f)
        f.flush()
        p = saga_slurm.prepare(f.name)
    p.config.run_file = rel(path)
    return p


def estimate(files: list[Path], reps: int) -> list[dict]:
    import e3_model as m
    rows = []
    for path in files:
        p = prepare_offline(path)
        prob = m.Problem.from_prediction(p.prediction)
        r = {"run_file": path.stem, "saga": p.prediction["makespan"]}
        for k in ("c", "d", "e"):
            r[k] = m.replay(prob, m.VARIANTS[k])["makespan"]
        r["seconds"] = max(r["saga"], r["e"]) + JOB_OVERHEAD
        rows.append(r)
        print(f"{path.stem:40s} SAGA {r['saga']:7.1f}s  replay c {r['c']:7.1f}  d {r['d']:7.1f}  "
              f"e {r['e']:7.1f}  -> ~{r['seconds']:6.1f}s per run", flush=True)
    total = sum(r["seconds"] for r in rows)
    print(f"\n{len(rows)} run files: ~{total / 60:.1f} min per rep, ~{total * reps / 3600:.2f} h for {reps} reps "
          f"(estimate = max(SAGA, replay e) + {JOB_OVERHEAD:g}s per run; link contention not modelled)")
    return rows


def validate(files: list[Path]) -> int:
    """Dry run (job.sbatch + driver commands, not printed) and replay(a) == SAGA for each file."""
    import e3_model as m

    from saga_exec.backends.slurm.allocation import allocation_files
    bad = 0
    for path in files:
        p = prepare_offline(path)
        assert p.config.dry_run
        plan_json, script = allocation_files("dry-run", LOGS / "dry-run", p.plan, p.config, p.testbed)
        diffs = m.verify_base(p.prediction)
        mem = [t["mem"] for t in plan_json["tasks"]]
        ok = not diffs and "#SBATCH" in script and len(plan_json["tasks"]) == len(p.instance.tasks)
        bad += not ok
        print(f"{'ok ' if ok else 'BAD'} {path.stem:40s} tasks {len(p.plan):3d}  SAGA {p.prediction['makespan']:7.2f}s  "
              f"mem {mem[0]}  replay(a) {'= SAGA' if not diffs else diffs[:2]}", flush=True)
    print(f"{len(files) - bad}/{len(files)} run files valid")
    return bad


# ---------------------------------------------------------------------------
# Running (submits Slurm jobs)
# ---------------------------------------------------------------------------

SAVED = re.compile(r"Saved (\S+)/summary\.json")


def run_one(path: Path, rep: int, log_dir: Path) -> dict:
    log_dir.mkdir(parents=True, exist_ok=True)
    log = log_dir / f"{path.stem}-rep{rep}.log"
    started = datetime.now().isoformat(timespec="seconds")
    t = time.time()
    proc = subprocess.run([sys.executable, "-m", "saga_exec.backends.slurm.saga_slurm", rel(path)],
                          cwd=ROOT, capture_output=True, text=True, check=False)
    log.write_text(proc.stdout + "\n--- stderr ---\n" + proc.stderr)
    m = SAVED.search(proc.stdout)
    run_id = Path(m.group(1)).name if m else ""
    ok = proc.returncode == 0 and run_id and (LOGS / run_id / "actual.json").exists()
    return {"run_file": rel(path), "rep": rep, "run_id": run_id, "status": "ok" if ok else "failed",
            "started": started, "finished": datetime.now().isoformat(timespec="seconds"),
            "seconds": round(time.time() - t, 1),
            "note": "" if ok else f"exit {proc.returncode}; see {rel(log)}"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--subset", default="core", choices=["core", "full"])
    ap.add_argument("--only", nargs="*", help="fnmatch patterns on run file stems")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--seed", type=int, default=3, help="shuffle seed for the run order")
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ap.add_argument("--estimate", action="store_true", help="offline time estimate, then exit")
    ap.add_argument("--validate", action="store_true", help="offline dry run of every selected run file, then exit")
    ap.add_argument("--list", action="store_true", help="print the selected runs (and which are done), then exit")
    args = ap.parse_args()

    files = select(args.subset, args.only)
    if not files:
        sys.exit("no run files selected (make them with experiments/slurm/e3/make_graphs.py)")
    if args.estimate:
        estimate(files, args.reps)
        return 0
    if args.validate:
        return 1 if validate(files) else 0

    finished = done(read_manifest(args.manifest))
    todo = []
    for rep in range(args.reps):
        batch = [(p, rep) for p in files if (rel(p), rep) not in finished]
        random.Random(args.seed * 1000 + rep).shuffle(batch)
        todo += batch
    print(f"{len(files)} run files x {args.reps} reps: {len(todo)} to run, "
          f"{len(files) * args.reps - len(todo)} already done ({rel(args.manifest)})")
    if args.list:
        for p, rep in todo:
            print(f"  rep {rep}  {rel(p)}")
        return 0
    log_dir = args.manifest.parent / "logs"
    for i, (path, rep) in enumerate(todo, 1):
        print(f"[{i}/{len(todo)}] rep {rep} {path.stem} ...", end=" ", flush=True)
        row = run_one(path, rep, log_dir)
        append_manifest(args.manifest, row)
        print(f"{row['status']} {row['run_id']} ({row['seconds']}s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
