"""LEGACY scheduling code from before SAGA scheduling moved to Wayline's bridge.

  wayline_convert   Nick Laus's offline converter (C core-nodes per machine; template pins placement)
  slurm_saga        Sebastian Lange's problem construction (core-nodes "<m>:c<i>", run-file settings)
  core_network      the shared per-core network draft (was saga_exec.network)

Not used by the backends. Kept importable so old results stay reproducible
(tests/test_legacy_wayline.py) and so the core-node model can be compared with
the bridge's capacity model (tests/test_legacy_vs_bridge.py, docs/formats.md).
"""
