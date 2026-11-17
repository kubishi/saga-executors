"""The shared SAGA path (saga_exec.scheduling): request, bridge, prediction."""

import json
import warnings
from pathlib import Path

import pytest
import yaml
from conftest import ROOT

from saga_exec.instance import load_instance
from saga_exec.scheduling import (
    BridgeError,
    ModelConfig,
    SchedulingConfig,
    bridge_model,
    build_request,
    check_fit,
    cpu_millis,
    node_order,
    prediction_from_response,
    run_bridge,
    saga_has_capacity_model,
    same_schedule,
    schedule,
)
from saga_exec.testbed import load_testbed

GOLDEN = Path(__file__).resolve().parent / "data" / "golden"
SMOKE = ROOT / "graphs" / "smoke.json"


@pytest.mark.parametrize("case", sorted(p.name for p in GOLDEN.iterdir() if p.is_dir()))
def test_request_matches_controller(case):
    """build_request == the body Wayline's controller sends for the generated template.

    controller-request.json was produced by running the controller's own code
    (extractTasks, the resolvers, sagaAssignTasks; wayline saga-executor @ 9fd3d35)
    on template.yml + network-profile.yml with 12000 millicores per node; see
    tests/data/golden/README.md.
    """
    from saga_exec.backends.wayline.convert import Config

    cfg = Config(**json.loads((GOLDEN / case / "config.json").read_text()))
    tb = load_testbed(GOLDEN / "machines.yaml")
    ours = build_request(load_instance(SMOKE), tb, cfg.scheduling())
    theirs = json.loads((GOLDEN / case / "controller-request.json").read_text())
    assert ours == theirs


def test_golden_templates_are_current():
    """The golden templates are what convert.to_template still generates."""
    from saga_exec.backends.wayline.convert import Config, network_configmap, to_template

    tb = load_testbed(GOLDEN / "machines.yaml")
    for case in (p for p in GOLDEN.iterdir() if p.is_dir()):
        cfg = Config(**json.loads((case / "config.json").read_text()))
        assert to_template(load_instance(SMOKE), cfg, tb) == yaml.safe_load((case / "template.yml").read_text())
        assert network_configmap(tb) == yaml.safe_load((case / "network-profile.yml").read_text())


def test_request_shape():
    tb = load_testbed()
    req = build_request(load_instance(SMOKE), tb, SchedulingConfig(partition="slurm", speed={"cse-isha": 0.5},
                                                                   sec_per_cost=3))
    a, b = req["dag"]["tasks"][:2]
    assert a["dependencies"] == [] and "inputs" not in a
    assert b["inputs"] == [{"producer": "a", "object": "to-b", "bytes": 10_000_000}]
    assert b["runtimeProfile"] == {"cse-isha": 24.0, "cse-lambda1": 12.0}      # 4 * 3 / speed
    assert [n["cpuMillis"] for n in req["clusterState"]["nodes"]] == [14000, 14000]
    assert len(req["clusterState"]["bandwidth"]) == 2


@pytest.mark.parametrize("q,m", [("1", 1000), ("2", 2000), ("500m", 500), ("1.5", 1500), ("0.1", 100)])
def test_cpu_millis(q, m):
    assert cpu_millis(q) == m


def test_node_order_like_controller():
    """Group in response order, stable sort by (start, finish) -- saga.go."""
    asg = [{"task": "c", "node": "n1", "estimatedStart": 1, "estimatedFinish": 3},
           {"task": "a", "node": "n1", "estimatedStart": 1, "estimatedFinish": 2},
           {"task": "b", "node": "n1", "estimatedStart": 1, "estimatedFinish": 2},
           {"task": "z", "node": "n2", "estimatedStart": 0, "estimatedFinish": 1}]
    assert node_order(asg) == {"n1": ["a", "b", "c"], "n2": ["z"]}


def test_check_fit_warns():
    assert check_fit({"costModelFitRMSE": 0.0})
    with pytest.warns(RuntimeWarning, match="costModelFitRMSE"):
        assert not check_fit({"costModelFitRMSE": 0.3})


def test_capacity_model_refused_without_feature_capacity():
    """The bridge's default model needs SAGA feature/capacity; without it the bridge refuses."""
    if saga_has_capacity_model():
        pytest.skip("SAGA with the capacity model is installed")
    req = build_request(load_instance(SMOKE), load_testbed(), SchedulingConfig(model={"nodes": "capacity"}))
    assert "model" not in req
    with pytest.raises(BridgeError, match="capacity model"):
        run_bridge(req)


def test_model_variants():
    tb = load_testbed()
    free = bridge_model(tb, SchedulingConfig(partition="slurm"))
    assert free == {"nodeModel": "cores", "cores": {"cse-isha": 14, "cse-lambda1": 14},
                    "selfBandwidth": {"cse-isha": 1e12, "cse-lambda1": 1e12},
                    "intraBandwidth": {"cse-isha": 1e12, "cse-lambda1": 1e12}}
    measured = bridge_model(tb, SchedulingConfig(model={"links": "measured", "cores": 4}))
    assert measured["cores"] == {"cse-isha": 4, "cse-lambda1": 4}
    assert measured["selfBandwidth"] == {m: tb.self_bandwidth[m] for m in tb.machines}
    assert measured["intraBandwidth"] == {m: tb.intra_bandwidth[m] for m in tb.machines}
    with pytest.raises(NotImplementedError, match="latency"):
        ModelConfig(transfer_latency=0.001)
    with pytest.raises(ValueError):
        ModelConfig(links="fast")


def test_prediction_from_bridge():
    pred = schedule(load_instance(SMOKE), load_testbed(), SchedulingConfig(), {"backend": "test"})
    assert pred["makespan"] == pytest.approx(7.000015)
    assert set(pred["tasks"]) == {"a", "b", "c", "d"}
    for t in pred["tasks"].values():
        assert t["machine"] in {"cse-lambda1", "cse-isha"}
        assert t["node"].startswith(t["machine"] + "/c")          # SAGA core "<machine>/c<i>"
    assert pred["meta"]["capacityModel"] == "cores"
    assert pred["meta"]["costModelExact"] and pred["meta"]["pythonHashSeed"] == "0"
    assert pred["request"]["algorithm"] == "heft" and pred["response"]["assignments"]
    assert sum(len(v) for v in pred["order"].values()) == 4
    assert same_schedule(pred, pred) == []


def test_bridge_runs_with_fixed_hash_seed():
    """Same request -> same response, whatever this process's hash seed."""
    req = build_request(load_instance(ROOT / "graphs" / "slurm-wide.json"), load_testbed(),
                        SchedulingConfig(partition="slurm", speed={"cse-isha": 0.5}))
    assert run_bridge(req) == run_bridge(req)


def test_bridge_error_is_reported():
    req = build_request(load_instance(SMOKE), load_testbed(), SchedulingConfig(scheduler="nope"))
    with pytest.raises(BridgeError, match="unknown algorithm"):
        run_bridge(req)


def test_prediction_records_misfit():
    req = {"algorithm": "heft"}
    resp = {"assignments": [{"task": "a", "node": "m", "estimatedStart": 0, "estimatedFinish": 1}],
            "estimatedMakespan": 1, "costModelFitRMSE": 0.5}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pred = prediction_from_response(req, resp)
    assert pred["meta"]["costModelExact"] is False
