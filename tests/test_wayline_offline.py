"""Wayline backend, offline: no kubectl, no cluster.

- convert() on graphs/smoke.json reproduces Nick's committed prediction/template.
- analyze.actual_from_status() reproduces the committed actual.json from status.json.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from saga_exec.backends.wayline import analyze, run  # noqa: F401  (run: import must not need kubectl)
from saga_exec.backends.wayline.convert import Config, convert

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "results" / "examples" / "wayline" / "smoke-diamond"


@pytest.mark.parametrize("scheduler", ["heft", "cpop"])
def test_convert_smoke_prediction(tmp_path, scheduler):
    """In-process: same makespan and tasks as the committed example. Placement can
    differ between processes because SAGA breaks ties in str-hash order."""
    out = convert(ROOT / "graphs" / "smoke.json", Config(scheduler=scheduler, C=8), tmp_path)
    expected = json.loads((EXAMPLES / out.name / "prediction.json").read_text())
    prediction = json.loads((out / "prediction.json").read_text())

    assert prediction["makespan"] == pytest.approx(expected["makespan"])
    assert set(prediction["tasks"]) == {"a", "b", "c", "d"}
    for t in prediction["tasks"].values():
        assert t["machine"] in ("cse-lambda1", "cse-isha")
        assert t["end"] >= t["start"]
    template = yaml.safe_load((out / "template.yml").read_text())
    assert template["kind"] == "ODAGTemplate"
    assert len(template["spec"]["tasks"]) == 4


def test_convert_smoke_reproduces_example_exactly(tmp_path):
    """With PYTHONHASHSEED=0 (as Nick's README prescribes) the output is byte-identical."""
    code = (
        "import sys; from pathlib import Path\n"
        "from saga_exec.backends.wayline.convert import Config, convert\n"
        "for s in ('heft', 'cpop'):\n"
        "    convert(Path(sys.argv[1]), Config(scheduler=s, C=8), Path(sys.argv[2]))\n"
    )
    env = {**os.environ, "PYTHONHASHSEED": "0"}
    subprocess.run([sys.executable, "-c", code, str(ROOT / "graphs" / "smoke.json"), str(tmp_path)],
                   check=True, env=env, capture_output=True)
    for expected in EXAMPLES.iterdir():
        if not expected.is_dir():
            continue
        got = tmp_path / "smoke-diamond" / expected.name
        for f in ("prediction.json", "template.yml", "config.json", "instance.json"):
            assert (got / f).read_text() == (expected / f).read_text(), f"{expected.name}/{f}"


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
