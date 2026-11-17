"""Predicted vs. actual (DRAFT).

Metrics both backends already report in some form:
  - makespan error (actual - predicted), relative error
  - per-task start/end deviation
  - placement check (same machine), order check (same per-machine start order)

Both backends now write prediction.json / actual.json in the shared shape; the
Gantt pairs and tables are still per backend (analyze.py, saga_slurm.py).
"""

from __future__ import annotations

from dataclasses import dataclass

from saga_exec.schedule import ScheduleRecord


@dataclass
class Comparison:
    predicted_makespan: float
    actual_makespan: float
    right_machine: bool
    right_order: bool
    task_start_error: dict[str, float]
    task_end_error: dict[str, float]

    @property
    def makespan_error(self) -> float:
        return self.actual_makespan - self.predicted_makespan

    @property
    def relative_error(self) -> float:
        return self.makespan_error / self.predicted_makespan if self.predicted_makespan else float("nan")


def _order(rec: ScheduleRecord) -> dict[str, list[str]]:
    by = {}
    for name, t in sorted(rec.tasks.items(), key=lambda kv: (kv[1].start, kv[0])):
        by.setdefault(t.machine, []).append(name)
    return by


def compare(predicted: ScheduleRecord, actual: ScheduleRecord) -> Comparison:
    common = predicted.tasks.keys() & actual.tasks.keys()
    return Comparison(
        predicted_makespan=predicted.makespan,
        actual_makespan=actual.makespan,
        right_machine=all(predicted.tasks[n].machine == actual.tasks[n].machine for n in common),
        right_order=_order(predicted) == _order(actual),
        task_start_error={n: actual.tasks[n].start - predicted.tasks[n].start for n in sorted(common)},
        task_end_error={n: actual.tasks[n].end - predicted.tasks[n].end for n in sorted(common)},
    )
