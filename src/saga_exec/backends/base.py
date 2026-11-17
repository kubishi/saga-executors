"""Backend protocol (DRAFT).

Pipeline per backend (README): network info -> saga.Network, schedule with SAGA,
convert the schedule to something executable, run it, compare.

    prepare(instance, schedule, config) -> Prepared     # offline: templates / plan + sbatch
    submit(prepared) -> Handle                          # touches the cluster
    collect(handle) -> ScheduleRecord                   # actual.json

Neither backend implements this yet:
  TODO(wayline): prepare = convert.to_template + write_outputs; submit = run.start_run;
                 collect = run.wait_for + analyze.actual_from_status.
  TODO(slurm):   prepare = allocation.allocation_files; submit = sbatch in run_allocation;
                 collect = wait_for_jobs + results.json -> ScheduleRecord.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from saga import Network, Schedule

from saga_exec.instance import Instance
from saga_exec.schedule import ScheduleRecord


@runtime_checkable
class Backend(Protocol):
    name: str

    def network(self) -> Network:
        """SAGA network for this backend's share of the testbed (cores x machines)."""
        ...

    def prepare(self, instance: Instance, schedule: Schedule, config: Any) -> Any:
        """Turn a SAGA schedule into something executable. Must not touch the cluster."""
        ...

    def submit(self, prepared: Any) -> Any:
        """Start it on the real system; return a handle."""
        ...

    def collect(self, handle: Any) -> ScheduleRecord:
        """Wait for completion and return the measured schedule (actual.json)."""
        ...
