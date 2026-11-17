"""Legacy offline SAGA (students' core-node models) vs Wayline's bridge, same inputs.

Same instance (graphs/smoke.json), same speeds (1.0), same cross-machine link
(min of both directions, 117000000 B/s), C cores per machine:

  legacy Wayline (Nick)    C SAGA nodes "<m>-c<i>", 1e12 B/s between cores and on self-loops
  legacy Slurm (Sebastian) C SAGA nodes "<m>:c<i>", inf B/s between cores and on self-loops
  bridge, cores model      C SAGA nodes "<m>/c<i>" (model.nodeModel "cores"), links
                           "free": self and intra-machine 1e12 B/s

Findings (docs/formats.md, "Legacy vs bridge"):
  * legacy Wayline == bridge: identical makespan, identical machine mapping up
    to swapping the two identical machines (tie-breaks follow str-hash order
    over different node names). The bridge's rank-1 fit is exact (RMSE 0), and
    with PyPI SAGA the bridge plans with cost/speed, i.e. the same problem.
  * legacy Slurm is lower by exactly the same-machine transfer time at 1e12 B/s
    (smoke: 10 MB + 5 MB on the critical path = 1.5e-5 s): inf makes them free.
"""

import json
import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import pytest
from conftest import ROOT

from saga_exec.instance import load_instance
from saga_exec.legacy import slurm_saga
from saga_exec.scheduling import SchedulingConfig, schedule
from saga_exec.testbed import load_testbed

SMOKE = ROOT / "graphs" / "smoke.json"
LEGACY_LOCAL_TRANSFER = (10_000_000 + 5_000_000) / 1e12     # a->b and b->d stay on one machine


def legacy_wayline(scheduler: str, C: int) -> dict:
    """Nick's converter, in a fresh interpreter with PYTHONHASHSEED=0 (as he ran it)."""
    code = ("import sys, json; from pathlib import Path\n"
            "from saga_exec.legacy.wayline_convert import Config, convert\n"
            "out = convert(Path(sys.argv[1]), Config(scheduler=sys.argv[2], C=int(sys.argv[3])), Path(sys.argv[4]))\n"
            "print(json.dumps(json.loads((out / 'prediction.json').read_text())))\n")
    with tempfile.TemporaryDirectory() as d:
        out = subprocess.run([sys.executable, "-c", code, str(SMOKE), scheduler, str(C), d], check=True,
                             capture_output=True, text=True, env={**os.environ, "PYTHONHASHSEED": "0"})
    return json.loads(out.stdout.strip().splitlines()[-1])


def legacy_slurm(scheduler: str, C: int) -> dict:
    """Sebastian's construction with the equivalent run-file settings."""
    inst = load_instance(SMOKE)
    config = SimpleNamespace(tasks=inst.tasks, dependencies=[list(e) for e in inst.edges], graph=None,
                             nodes={}, cores=C, bandwidth=117_000_000)
    sched = slurm_saga.scheduler_registry()[scheduler]().schedule(
        slurm_saga.build_network(config), slurm_saga.build_task_graph(config))
    tasks = slurm_saga.real_tasks(sched)
    return {"makespan": sched.makespan,
            "tasks": {n: {"machine": t.node.split(":")[0], "start": t.start, "end": t.end} for n, t in tasks.items()}}


def machines_up_to_relabeling(a: dict, b: dict) -> bool:
    ma = {t: x["machine"] for t, x in a["tasks"].items()}
    mb = {t: x["machine"] for t, x in b["tasks"].items()}
    groups = lambda m: sorted(sorted(t for t in m if m[t] == x) for x in set(m.values()))
    return groups(ma) == groups(mb)


@pytest.mark.parametrize("scheduler", ["heft", "cpop"])
@pytest.mark.parametrize("C", [8, 14])
def test_legacy_models_differ_only_by_local_bandwidth(scheduler, C):
    """Runs everywhere: the two students' models agree up to 1e12-vs-inf local links."""
    w, s = legacy_wayline(scheduler, C), legacy_slurm(scheduler, C)
    assert w["makespan"] - s["makespan"] == pytest.approx(LEGACY_LOCAL_TRANSFER, abs=1e-12)
    assert machines_up_to_relabeling(w, s)


@pytest.mark.parametrize("scheduler", ["heft", "cpop"])
@pytest.mark.parametrize("C", [8, 14])
def test_legacy_vs_bridge(scheduler, C):
    bridge = schedule(load_instance(SMOKE), load_testbed(),
                      SchedulingConfig(scheduler=scheduler, model={"cores": C, "links": "free"}))
    legacy = legacy_wayline(scheduler, C)
    assert bridge["meta"]["costModelFitRMSE"] == pytest.approx(0, abs=1e-12)
    assert bridge["makespan"] == pytest.approx(legacy["makespan"], rel=1e-12)
    assert machines_up_to_relabeling(bridge, legacy)
    for name, t in bridge["tasks"].items():               # same start/end per task, too
        assert t["start"] == pytest.approx(legacy["tasks"][name]["start"], rel=1e-12, abs=1e-12)
        assert t["end"] == pytest.approx(legacy["tasks"][name]["end"], rel=1e-12, abs=1e-12)
    assert bridge["makespan"] - legacy_slurm(scheduler, C)["makespan"] == pytest.approx(LEGACY_LOCAL_TRANSFER, abs=1e-12)


@pytest.mark.parametrize("path", sorted((ROOT / "graphs").glob("slurm-*.json")), ids=lambda p: p.stem)
def test_legacy_wayline_vs_bridge_on_all_graphs(path, tmp_path):
    """Beyond smoke: makespans agree on every shared graph (HEFT, C=14, isha at half speed)."""
    inst = load_instance(path)
    bridge = schedule(inst, load_testbed(), SchedulingConfig(partition="slurm", speed={"cse-isha": 0.5},
                                                             model={"cores": 14}))
    from saga_exec.legacy.wayline_convert import build_network, run_scheduler, to_task_graph
    legacy = run_scheduler("heft", build_network(14, {"cse-lambda1": 1.0, "cse-isha": 0.5}),
                           to_task_graph(inst.to_dict(), 1.0, 1.0))
    assert bridge["makespan"] == pytest.approx(legacy.makespan, rel=1e-9)
