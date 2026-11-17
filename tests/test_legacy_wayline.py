"""Nick's LEGACY Wayline converter (saga_exec.legacy.wayline_convert), offline.

convert() on graphs/smoke.json reproduces Nick's committed prediction/template in
results/examples/wayline/ (made with the legacy converter, SAGA on core-nodes).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from saga_exec.legacy.wayline_convert import Config, convert

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
        "from saga_exec.legacy.wayline_convert import Config, convert\n"
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
