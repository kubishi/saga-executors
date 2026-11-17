#!/usr/bin/env python3
"""Convert datasets to Wayline templates: edit the calls below, then run this.

Each call converts one graph with its own settings (see Config in
saga_exec/backends/wayline/convert.py). SAGA itself runs in the cluster
(spec.scheduler saga/<alg>); here the same request is also scheduled offline
through Wayline's bridge for comparison (prediction.json).

Run from the repo root:  uv run python experiments/wayline/generate.py
(the bridge runs with PYTHONHASHSEED=0 on its own; no need to set it)
The last argument is where its output goes, so use subfolders to organize them.
"""
from pathlib import Path

from saga_exec.backends.wayline.convert import Config, convert

ROOT = Path(__file__).resolve().parents[2]   # repo root
GRAPHS = ROOT / "graphs"
OUT = ROOT / "results" / "wayline"

convert(GRAPHS / "smoke.json", Config(scheduler="heft"), OUT)
convert(GRAPHS / "smoke.json", Config(scheduler="cpop"), OUT)

# A whole dataset, all with the same settings:
# for graph in (GRAPHS / "wfcommons").glob("*.json"):
#     convert(graph, Config(scheduler="heft", enact_order="none"), OUT / "wfcommons")
