"""Backend protocol (DRAFT; neither backend subclasses it yet).

SAGA scheduling is NOT a backend concern: saga_exec.scheduling turns an
instance + testbed/machines.yaml + config into Wayline's bridge request and
prediction.json, offline and (for Wayline) in the cluster. A backend only
enacts SAGA's placement (and, with enact_order "order", per-machine start
order) and measures what happened:

    prepare(instance, config) -> Prepared     # offline: template.yml / plan + job.sbatch, prediction.json
    submit(prepared) -> Handle                # touches the cluster
    collect(handle) -> ScheduleRecord         # actual.json (Wayline also: SAGA's in-cluster prediction)

Mapping today:
  wayline: prepare = convert.convert; submit = run.start_run; collect = run.wait_for + analyze.analyze
  slurm:   prepare = saga_slurm.prepare + allocation.allocation_files;
           submit + collect = allocation.run_allocation (+ actual.json in saga_slurm.main)
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from saga_exec.instance import Instance
from saga_exec.schedule import ScheduleRecord


@runtime_checkable
class Backend(Protocol):
    name: str

    def prepare(self, instance: Instance, config: Any) -> Any:
        """Build what the system executes (and the offline prediction). Must not touch the cluster."""
        ...

    def submit(self, prepared: Any) -> Any:
        """Start it on the real system; return a handle."""
        ...

    def collect(self, handle: Any) -> ScheduleRecord:
        """Wait for completion and return the measured schedule (actual.json)."""
        ...
