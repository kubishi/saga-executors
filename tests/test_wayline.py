"""Wayline backend, offline: no kubectl, no cluster."""

import copy
import json

import pytest
import yaml
from conftest import ROOT

from saga_exec.backends.wayline import analyze, run  # noqa: F401  (run: import must not need kubectl)
from saga_exec.backends.wayline.convert import Config, convert
from saga_exec.scheduling import saga_has_capacity_model

EXAMPLES = ROOT / "results" / "examples" / "wayline" / "smoke-diamond"
SMOKE = ROOT / "graphs" / "smoke.json"


def test_template_asks_saga_in_the_cluster(tmp_path):
    out = convert(SMOKE, Config(scheduler="cpop", enact_order="none", cpu="2"), tmp_path)
    t = yaml.safe_load((out / "template.yml").read_text())
    spec = t["spec"]
    assert spec["scheduler"] == "saga/cpop"
    assert spec["schedulerConfig"]["enactOrder"] == "none"
    assert spec["schedulerConfig"]["model"]["nodeModel"] == "cores"
    assert spec["schedulerConfig"]["model"]["cores"] == {"cse-isha": 8, "cse-lambda1": 8}
    assert spec["profiling"]["runtimeSource"] == "manual"
    assert spec["profiling"]["bandwidthSource"] == "external"
    for task in spec["tasks"]:
        assert "constraints" not in task                     # SAGA places, nothing is pinned
        assert task["resources"]["cpu"] == "2"
        assert set(task["runtimeProfile"]) == {"cse-lambda1", "cse-isha"}
    cm = yaml.safe_load((out / "network-profile.yml").read_text())
    assert cm["metadata"] == {"name": "wl-network-profile", "namespace": "wl-system"}
    assert cm["data"]["cse-lambda1_to_cse-isha"] == "117000000"

    request = json.loads((out / "request.json").read_text())
    assert [n["cpuMillis"] for n in request["clusterState"]["nodes"]] == [12000, 12000]
    assert request["dag"]["tasks"][0]["cpuMillis"] == 2000
    prediction = json.loads((out / "prediction.json").read_text())
    assert prediction["request"] == request
    assert prediction["meta"]["backend"] == "wayline"


@pytest.mark.skipif(saga_has_capacity_model(), reason="SAGA has the capacity model")
def test_convert_when_bridge_refuses(tmp_path):
    with pytest.warns(UserWarning, match="no offline prediction"):
        out = convert(SMOKE, Config(model={"nodes": "capacity"}), tmp_path)
    assert (out / "template.yml").exists() and (out / "request.json").exists()
    assert not (out / "prediction.json").exists()


def test_config_validation():
    with pytest.raises(ValueError, match="enact_order"):
        Config(enact_order="serial")
    assert Config(scheduler="mypkg.sched.My_Heft").run_name().startswith("mypkg-sched-my-heft-order-sleep-")


def test_analyze_reproduces_actual_json():
    status_paths = sorted(EXAMPLES.glob("*/runs/*/status.json"))
    assert status_paths
    for status_path in status_paths:
        actual = analyze.actual_from_status(json.loads(status_path.read_text()))
        expected = json.loads((status_path.parent / "actual.json").read_text())
        assert actual["tasks"].keys() == expected["tasks"].keys()
        assert actual["makespan"] == pytest.approx(expected["makespan"])
        for name, t in expected["tasks"].items():
            assert actual["tasks"][name]["start"] == pytest.approx(t["start"])
            assert actual["tasks"][name]["end"] == pytest.approx(t["end"])


def _status_with_saga(prediction: dict) -> dict:
    """The example run's status, as if SAGA had placed it with this prediction
    (what the controller on branch saga-executor writes to status.scheduling)."""
    status = json.loads(next(EXAMPLES.glob("heft-*/runs/*/status.json")).read_text())
    status = copy.deepcopy(status)
    status["status"]["scheduling"] = {
        "requested": "saga/heft", "used": "saga/heft",
        "externalEstimatedMakespan": prediction["makespan"],
        "externalAssignments": prediction["response"]["assignments"],
        "externalCostModelFitRMSE": 0.0, "externalCapacityModel": "cores",
        "externalConstraintOverrides": [], "externalSagaVersion": prediction["meta"]["saga_version"],
        "externalNodeCPUMillis": {"cse-isha": 12000, "cse-lambda1": 12000},
    }
    return status


def test_analyze_in_cluster_prediction(tmp_path):
    out = convert(SMOKE, Config(), tmp_path)
    offline = json.loads((out / "prediction.json").read_text())
    status = _status_with_saga(offline)
    run_dir = out / "runs" / status["metadata"]["name"]
    run_dir.mkdir(parents=True)
    (run_dir / "status.json").write_text(json.dumps(status))

    comparison = analyze.analyze(run_dir / "status.json")
    assert comparison["same_schedule"] and comparison["same_node_cpu"] and comparison["same_saga_version"]
    in_cluster = json.loads((run_dir / "prediction.json").read_text())
    assert in_cluster["meta"]["source"] == "wayline-status"
    assert in_cluster["tasks"] == offline["tasks"] and in_cluster["order"] == offline["order"]
    assert (run_dir / "actual.json").exists() and (run_dir / "gantt.png").exists()

    # A different node CPU in the cluster is flagged.
    status["status"]["scheduling"]["externalNodeCPUMillis"]["cse-isha"] = 11650
    (run_dir / "status.json").write_text(json.dumps(status))
    assert not analyze.analyze(run_dir / "status.json")["same_node_cpu"]


def test_fallback_runs_are_rejected(tmp_path):
    out = convert(SMOKE, Config(), tmp_path)
    status = _status_with_saga(json.loads((out / "prediction.json").read_text()))
    status["status"]["scheduling"] = {"requested": "saga/heft", "used": "random",
                                      "fallbackError": "sidecar unreachable: connection refused"}
    with pytest.raises(analyze.RejectedRun, match="random.*connection refused"):
        analyze.prediction_from_status(status)
    status["status"]["scheduling"] = {"requested": "saga/heft", "used": "saga/heft",
                                      "externalEstimatedMakespan": 7.0}
    with pytest.raises(analyze.RejectedRun, match="saga-executor"):
        analyze.prediction_from_status(status)
    status["status"]["scheduling"] = {"requested": "random", "used": "random"}
    with pytest.raises(analyze.RejectedRun, match="not a SAGA scheduler"):
        analyze.prediction_from_status(status)
