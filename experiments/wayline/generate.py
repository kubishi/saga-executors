#!/usr/bin/env python3
"""Convert datasets to Wayline templates: edit the calls below, then run this.

Each call converts one graph with its own settings (see Config in
saga_exec/backends/wayline/convert.py).

Run from the repo root:  PYTHONHASHSEED=0 uv run python experiments/wayline/generate.py
(originally eval/experiments/fidelity/generate.py in Nick's Wayline fork)
The last argument is where its output goes, so use subfolders to organize them.
"""
from pathlib import Path

from saga_exec.backends.wayline.convert import Config, convert

ROOT = Path(__file__).resolve().parents[2]   # repo root
GRAPHS = ROOT / "graphs"
OUT = ROOT / "results" / "wayline"

convert(GRAPHS / "smoke.json", Config(scheduler="heft", C=8), OUT)
convert(GRAPHS / "smoke.json", Config(scheduler="cpop", C=8), OUT)

# A whole dataset, all with the same settings:
# for graph in (GRAPHS / "wfcommons").glob("*.json"):
#     convert(graph, Config(scheduler="heft", C=4), OUT / "wfcommons")
