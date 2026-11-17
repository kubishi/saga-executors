"""saga_exec: execute SAGA schedules on real systems and compare predicted vs. actual.

One SAGA path: instance (graphs/*.json) + testbed (testbed/machines.yaml) + config
-> Wayline's bridge request -> bridge (saga-sidecar/bridge.py) -> prediction.json.

  instance, testbed  inputs
  scheduling         the SAGA path (same request as Wayline's controller sends)
  schedule, compare  prediction.json / actual.json records and metrics
  backends.wayline   enacts + measures on Kubernetes/Wayline (Nick Laus)
  backends.slurm     enacts + measures on Slurm (Sebastian Lange)
  legacy             the students' original SAGA problem construction
  calibrate          measuring node speeds / link bandwidths (placeholder)
"""

__version__ = "0.1.0"
