"""saga_exec: execute SAGA schedules on real systems and compare predicted vs. actual.

Subpackages:
  backends.wayline   Kubernetes / Wayline backend (Nick Laus)
  backends.slurm     Slurm backend (Sebastian Lange)
  calibrate          measuring node speeds / link bandwidths (draft)

Shared drafts (instance, network, schedule, compare, backends.base) are not yet
used by the backends; see docs/formats.md.
"""

__version__ = "0.1.0"
