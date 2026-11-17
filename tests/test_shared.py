"""Shared draft modules: instance, network, schedule, compare."""

import json
import math
from pathlib import Path

import pytest
from saga.schedulers import HeftScheduler

from saga_exec.compare import compare
from saga_exec.instance import load_instance
from saga_exec.network import build_core_network, load_machines, machine_of
from saga_exec.schedule import ScheduleRecord, prediction_from_saga

ROOT = Path(__file__).resolve().parents[1]


def test_machines_yaml():
    m = load_machines(ROOT / "testbed" / "machines.yaml")
    assert set(m.speed) == {"cse-lambda1", "cse-isha"}
    assert m.bandwidth[("cse-lambda1", "cse-isha")] == 117_000_000
    assert m.bandwidth[("cse-isha", "cse-lambda1")] == 117_625_000
    assert m.link("cse-isha", "cse-lambda1") == 117_000_000
    assert m.local_bandwidth == math.inf


@pytest.mark.parametrize("path", sorted((ROOT / "graphs").glob("*.json")), ids=lambda p: p.name)
def test_graphs_load(path):
    inst = load_instance(path)
    assert inst.tasks
    inst.to_task_graph()


@pytest.mark.parametrize("sep,local", [("-", 1e12), (":", math.inf)])
def test_core_network_and_schedule(sep, local):
    """The shared builder covers both backends' conventions and gives a usable network."""
    m = load_machines(ROOT / "testbed" / "machines.yaml")
    net = build_core_network(m, 2, sep=sep, local_bandwidth=local)
    assert len(list(net.nodes)) == 4
    assert net.get_edge(f"cse-lambda1{sep}c0", f"cse-lambda1{sep}c1").speed == local
    assert net.get_edge(f"cse-lambda1{sep}c0", f"cse-isha{sep}c1").speed == 117_000_000

    tg = load_instance(ROOT / "graphs" / "smoke.json").to_task_graph()
    pred = prediction_from_saga(HeftScheduler().schedule(net, tg), lambda n: machine_of(n, sep))
    assert set(pred.tasks) == {"a", "b", "c", "d"}
    assert pred.makespan == pytest.approx(7.0, abs=1e-3)


def test_wayline_shared_network_matches_backend():
    """Shared builder with Wayline's settings == convert.build_network (same nodes/links)."""
    from saga_exec.backends.wayline import convert

    m = load_machines(ROOT / "testbed" / "machines.yaml")
    shared = build_core_network(m, 3, sep="-", local_bandwidth=convert.LOCAL_BW)
    theirs = convert.build_network(3, {"cse-lambda1": 1.0, "cse-isha": 1.0})
    assert {n.name: n.speed for n in shared.nodes} == {n.name: n.speed for n in theirs.nodes}
    for e in theirs.edges:
        assert shared.get_edge(e.source, e.target).speed == e.speed


def test_compare_on_wayline_example():
    ex = ROOT / "results" / "examples" / "wayline" / "smoke-diamond" / "heft-c8-sleep-a5faff"
    pred = ScheduleRecord.load(ex / "prediction.json")
    actual = ScheduleRecord.from_dict(json.loads(next(ex.glob("runs/*/actual.json")).read_text()))
    c = compare(pred, actual)
    assert c.predicted_makespan == pytest.approx(7.000015)
    assert c.actual_makespan == pytest.approx(9.20999, abs=1e-4)
    assert c.right_machine
    assert set(c.task_end_error) == {"a", "b", "c", "d"}
