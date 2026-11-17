"""Wayline (Kubernetes) backend (owner: Nick Laus).

SAGA runs inside the cluster: templates ask for spec.scheduler "saga/<alg>", the
controller sends the problem to the saga-sidecar (Wayline's bridge), enacts the
placement (and, with enact_order "order", the per-machine start order) and
records SAGA's prediction in status.scheduling. This backend only builds the
template, runs it and measures it:

  convert.py   instance + Config -> template.yml, network-profile.yml, request.json,
               offline bridge prediction.json (no cluster)
  run.py       applies them with kubectl, waits, saves status/logs, calls analyze
  analyze.py   status.json -> in-cluster prediction.json (rejects fallback runs),
               actual.json, gantt.png

Nick's original converter (SAGA offline on core-nodes, placement pinned) is
saga_exec.legacy.wayline_convert. The task program run inside each pod is task/
at the repo root (needs the Wayline python SDK `wl`, baked into the image).
"""
