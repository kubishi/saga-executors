"""Slurm backend, offline: no sbatch/srun. SAGA via the shared path (fake bridge here)."""

import json
from pathlib import Path

import pytest
from conftest import ROOT

from saga_exec.backends.slurm import driver, saga_slurm
from saga_exec.backends.slurm.allocation import allocation_files
from saga_exec.testbed import load_testbed

RUNS = ROOT / "experiments" / "slurm" / "runs"


@pytest.mark.parametrize("path", sorted(RUNS.rglob("*.json")), ids=lambda p: str(p.relative_to(RUNS)))
def test_run_files_load(path):
    config = saga_slurm.load_run_file(str(path))
    assert saga_slurm.instance_path(config).exists()
    assert config.enact_order == "order"


def _write(tmp_path, **spec) -> str:
    p = tmp_path / "run.json"
    p.write_text(json.dumps({"instance": "graphs/smoke.json", **spec}))
    return str(p)


def test_strict_order_is_deprecated(tmp_path):
    with pytest.warns(DeprecationWarning, match="strict_order"):
        assert saga_slurm.load_run_file(_write(tmp_path, strict_order=False)).enact_order == "none"
    with pytest.warns(DeprecationWarning):
        assert saga_slurm.load_run_file(_write(tmp_path, strict_order=True)).enact_order == "order"


@pytest.mark.parametrize("spec", [{"time_scale": 3}, {"bandwidth": 1e9}, {"cores": 8}, {"enact_order": "serial"},
                                  {"tpyo": 1}, {"instance": None}, {"model": {"links": "x"}},
                                  {"model": {"transfer_latency": 0.01}}, {"model": {"typo": 1}}])
def test_bad_run_files_stop(tmp_path, spec):
    with pytest.raises(SystemExit):
        saga_slurm.load_run_file(_write(tmp_path, **spec))


def test_prepare_and_plan(tmp_path):
    run_file = _write(tmp_path, scheduler="heft", speed={"cse-isha": 0.5}, sec_per_cost=2, _why="comment")
    p = saga_slurm.prepare(run_file)
    assert p.prediction["meta"]["backend"] == "slurm"
    assert p.prediction["request"]["clusterState"]["nodes"][0]["cpuMillis"] == 14000
    plan = p.plan
    assert list(plan) == sorted(plan, key=lambda n: (plan[n]["start"], n))
    for name, t in plan.items():
        speed = 0.5 if t["machine"] == "cse-isha" else 1.0
        assert t["duration"] == pytest.approx(p.instance.tasks[name] * 2 / speed)
        assert t["end"] - t["start"] == pytest.approx(t["duration"])      # bridge: exact runtimes
    assert plan["d"]["waits_for"] == ["b", "c"]
    # enact_order "order": each task starts after its predecessor on the machine
    for order in p.prediction["order"].values():
        assert [plan[t]["start_after"] for t in order] == [[]] + [[a] for a in order[:-1]]

    script_plan, script = allocation_files("rid", Path("/tmp/x"), plan, p.config, p.testbed)
    assert "#SBATCH --cpus-per-task=2" in script
    by_name = {t["name"]: t for t in script_plan["tasks"]}
    assert by_name["d"]["start_after"] == plan["d"]["start_after"]
    assert by_name["a"]["node"] in ("cse-lambda1", "cse-isha")


def test_transfer_seconds_follow_the_bridge(tmp_path):
    """Simulated transfer = bytes / what SAGA assumed: min of both directions across machines."""
    p = saga_slurm.prepare(str(RUNS / "transfer_test.json"))
    tb = load_testbed()
    for t in p.plan.values():
        for parent, seconds in t["transfer_from"].items():
            if p.plan[parent]["machine"] != t["machine"]:
                assert seconds == pytest.approx(3.0)        # 3 units * 117e6 B / 117e6 B/s
            else:
                assert seconds < 1e-3
    assert saga_slurm.link_speed(tb, None, "cse-isha/c0", "cse-lambda1/c3") == 117_000_000
    model = {"selfBandwidth": {"cse-isha": 5.0}, "intraBandwidth": 7.0}
    assert saga_slurm.link_speed(tb, model, "cse-isha/c1", "cse-isha/c1") == 5.0       # same core
    assert saga_slurm.link_speed(tb, model, "cse-isha/c1", "cse-isha/c2") == 7.0       # same machine
    assert saga_slurm.link_speed(tb, model, "cse-lambda1/c1", "cse-lambda1/c1") == 1e12  # unset -> free


def test_measured_links_reach_the_plan(tmp_path):
    p = saga_slurm.prepare(_write(tmp_path, model={"links": "measured", "cores": 2}))
    assert p.prediction["request"]["model"]["intraBandwidth"] == {"cse-isha": 1e9, "cse-lambda1": 1e9}
    sizes = {(s, d): z for s, d, z in p.instance.edges}
    for name, t in p.plan.items():
        assert t["core"].rpartition("/c")[0] == t["machine"]
        for parent, seconds in t["transfer_from"].items():
            if p.plan[parent]["machine"] == t["machine"]:       # self or intra: both 1e9 placeholders
                assert seconds == pytest.approx(sizes[(parent, name)] / 1e9)


def test_no_enacted_order(tmp_path):
    p = saga_slurm.prepare(_write(tmp_path, enact_order="none"))
    assert all(t["start_after"] == [] for t in p.plan.values())


def test_driver_start_after():
    t = {"start_after": ["a"]}
    assert not driver.started_after(t, {"a", "b"})
    assert driver.started_after(t, {"b"})
    assert driver.started_after({}, {"a"})                  # plans without start_after (old plan.json)
