"""Shared schedule record (DRAFT): prediction.json / actual.json.

Shape follows Nick's prediction.json/actual.json (docs/formats.md):

    {"makespan": 7.0,
     "tasks": {"a": {"node": "cse-isha-c6", "machine": "cse-isha",
                     "start": 0.0, "end": 2.0,
                     # actual only, optional:
                     "input_read": ..., "compute": ..., "handoff": ...}}}

Times are seconds from the first task's start. `node` is the SAGA core in a
prediction; in an actual it is whatever the backend can observe (Wayline: the
machine; Slurm: machine + physical core).

TODO(wayline): convert.write_outputs / analyze.actual_from_status build these dicts inline.
TODO(slurm):   saga_slurm.main writes summary.json with a different shape (rows list).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from saga import Schedule

SUPER_TASKS = ("__super_source__", "__super_sink__")


@dataclass
class TaskRecord:
    node: str
    machine: str
    start: float
    end: float
    input_read: float | None = None
    compute: float | None = None
    handoff: float | None = None


@dataclass
class ScheduleRecord:
    makespan: float
    tasks: dict[str, TaskRecord] = field(default_factory=dict)
    meta: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = {"makespan": self.makespan,
             "tasks": {n: {k: v for k, v in asdict(t).items() if v is not None}
                       for n, t in self.tasks.items()}}
        if self.meta:
            d["meta"] = self.meta
        return d

    @classmethod
    def from_dict(cls, d: dict) -> ScheduleRecord:
        tasks = {n: TaskRecord(**{k: t.get(k) for k in TaskRecord.__dataclass_fields__})
                 for n, t in d["tasks"].items()}
        return cls(makespan=float(d["makespan"]), tasks=tasks, meta=d.get("meta", {}))

    def save(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), indent=2) + "\n")

    @classmethod
    def load(cls, path: str | Path) -> ScheduleRecord:
        return cls.from_dict(json.loads(Path(path).read_text()))


def prediction_from_saga(schedule: Schedule, machine_of, time_scale: float = 1.0) -> ScheduleRecord:
    """SAGA Schedule -> ScheduleRecord, dropping SAGA's super source/sink."""
    tasks = {}
    real = [t for ts in schedule.mapping.values() for t in ts if t.name not in SUPER_TASKS]
    for t in sorted(real, key=lambda t: (t.start, t.name)):
        tasks[t.name] = TaskRecord(node=t.node, machine=machine_of(t.node),
                                   start=t.start * time_scale, end=t.end * time_scale)
    return ScheduleRecord(makespan=schedule.makespan * time_scale, tasks=tasks)
