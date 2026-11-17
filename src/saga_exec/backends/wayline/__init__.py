"""Wayline (Kubernetes) backend (owner: Nick Laus).

Imported as-is from Nick's Wayline fork (github.com/nick23447/wayline, branch
`lab`, eval/experiments/fidelity/) at aff6fa8a698c1ca2cda6ecd6bb10948ef54df5a8;
only imports, paths and usage strings were changed. See README.md in this folder
(Nick's original) and testbed/k8s/README.md.

  convert.py   instance + Config -> SAGA schedule, prediction.json, ODAGTemplate (offline)
  run.py       applies templates with kubectl, waits, saves status/logs, calls analyze
  analyze.py   status.json -> actual.json + gantt.png

The task program run inside each pod is task/ at the repo root (needs the
Wayline python SDK `wl`, baked into the image; not a dependency of this package).
"""
