import re
"""Calibration, offline: machines.yaml writing, link plans and fits, the hash-rate worker."""

import json
import os
import subprocess
import sys

import pytest
from conftest import ROOT

from saga_exec.backends.slurm.allocation import build_task_pyz
from saga_exec.calibrate import cpu, links
from saga_exec.testbed import load_testbed

ROWS = [
    {"machine": "cse-lambda1", "single": 2.0e6, "loaded": 1.6e6, "loaded_min_process": 1.5e6, "cores": 14,
     "python": "3.12.8", "openssl": "OpenSSL 3"},
    {"machine": "cse-isha", "single": 1.8e6, "loaded": 1.2e6, "loaded_min_process": 1.1e6, "cores": 14,
     "python": "3.12.8", "openssl": "OpenSSL 3"},
]


def test_summarize_speeds_and_hash_rate():
    cal = cpu.summarize(ROWS, basis="loaded", reference="cse-lambda1", measured="2026-11-17T10:00:00")
    assert cal["hash_rate"] == 1.6e6
    assert cal["speed"] == {"cse-lambda1": 1.0, "cse-isha": 0.75}
    assert cpu.summarize(ROWS, basis="single", reference="cse-lambda1")["speed"]["cse-isha"] == 0.9
    with pytest.raises(ValueError):
        cpu.summarize(ROWS, reference="nope")


def test_machines_yaml_update_keeps_the_file(tmp_path):
    path = tmp_path / "machines.yaml"
    original = (ROOT / "testbed" / "machines.yaml").read_text()
    # start from an uncalibrated file, whatever the repo's machines.yaml holds now
    original = re.sub(r"\n# BEGIN calibration.*?# END calibration\n?", "\n", original, flags=re.S)
    path.write_text(original)
    assert load_testbed(path).hash_rate("slurm") is None
    cpu.update_machines_yaml(ROWS, path)
    text = path.read_text()
    tb = load_testbed(path)
    assert tb.hash_rate("slurm") == 1.6e6 and tb.speed == {"cse-lambda1": 1.0, "cse-isha": 0.75}
    assert "# calibrated: calibration.cpu.slurm (loaded" in text
    assert text.count(cpu.BEGIN) == 1 and "iperf3 pod-to-pod" in text          # comments kept
    # again (e.g. k8s later): the block is replaced and merged, not duplicated
    cal = cpu.summarize(ROWS, reference="cse-lambda1", backend="k8s")
    path.write_text(cpu.apply_to_yaml(path.read_text(), cal, set_speed=False))
    tb = load_testbed(path)
    assert path.read_text().count(cpu.BEGIN) == 1
    assert tb.hash_rate("slurm") == tb.hash_rate("k8s") == 1.6e6
    # nothing else changed: before the block, only the speed lines differ
    def rest(t):
        return [ln for ln in t.split(cpu.BEGIN)[0].rstrip().splitlines() if "    speed:" not in ln]
    assert rest(path.read_text()) == rest(original)


def test_hash_rate_worker_runs(tmp_path):
    """task.pyz calibrate on ONE cpu, briefly (this may run on a shared machine)."""
    pyz = build_task_pyz(tmp_path / "task.pyz")
    cpu0 = sorted(os.sched_getaffinity(0))[:1]
    out = subprocess.run([sys.executable, str(pyz), "calibrate", "--seconds", "0.05", "--reps", "1"],
                         capture_output=True, text=True, timeout=60,
                         preexec_fn=lambda: os.sched_setaffinity(0, cpu0))
    assert out.returncode == 0, out.stderr
    r = json.loads(out.stdout)
    assert r["cores"] == 1 and r["single"] > 0 and r["loaded"] > 0 and r["python"]


def test_slurm_link_plans_and_fit():
    plan = links.slurm_chain_plan("cse-isha", 1000, "intra")
    assert set(plan) == {"p0", "c0", "p1", "c1"} and {t["machine"] for t in plan.values()} == {"cse-isha"}
    assert plan["c1"]["inputs"] == [{"producer": "p1", "object": "to-c1", "bytes": 1000}]
    plan = links.slurm_chain_plan("cse-isha", 5, "inter", "cse-lambda1")
    assert plan["p0"]["outputs"][0]["consumer_node"] == "cse-lambda1"
    with pytest.raises(ValueError):
        links.slurm_chain_plan("cse-isha", 5, "inter")
    results = {"p0": {"state": "COMPLETED", "timings": {"timeline": {"outputs": [{"sendStartUnix": 10.0}]}}},
               "c0": {"state": "COMPLETED", "timings": {"timeline": {"workStartUnix": 10.6,
                                                                     "inputs": [{"readEndUnix": 10.5}]}}}}
    assert links.transfer_seconds(results) == [0.5]
    latency, bw = links.fit([0, 1e6, 2e6], [0.2, 0.21, 0.22])
    assert latency == pytest.approx(0.2) and bw == pytest.approx(1e8)
