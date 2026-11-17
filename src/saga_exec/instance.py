"""Shared problem instance (DRAFT).

Format = Nick's graphs/*.json (docs/formats.md, "Instance"):

    {"name": "smoke-diamond", "source": "...",
     "tasks": {"a": 2.0, ...},                 # task -> cost (cost units)
     "edges": [["a", "b", 10000000], ...]}     # [producer, consumer, size] (size units)

Not used by the backends yet:
  TODO(wayline): backends/wayline/convert.py has its own load_instance/to_task_graph.
  TODO(slurm):   backends/slurm/saga_slurm.py reads run files (graph + settings mixed).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from saga import TaskGraph


@dataclass
class Instance:
    name: str
    tasks: dict[str, float]
    edges: list[tuple[str, str, float]] = field(default_factory=list)
    source: str = ""

    def validate(self) -> None:
        for src, dst, size in self.edges:
            for t in (src, dst):
                if t not in self.tasks:
                    raise ValueError(f"{self.name}: edge endpoint {t!r} is not a task")
            if size < 0:
                raise ValueError(f"{self.name}: negative size on {src}->{dst}")

    def to_task_graph(self, sec_per_cost: float = 1.0, bytes_per_size: float = 1.0) -> TaskGraph:
        """saga.TaskGraph with costs in seconds (at speed 1.0) and sizes in bytes."""
        return TaskGraph.create(
            tasks=[(n, c * sec_per_cost) for n, c in self.tasks.items()],
            dependencies=[(s, d, z * bytes_per_size) for s, d, z in self.edges],
        )

    def to_dict(self) -> dict:
        return {"name": self.name, "source": self.source, "tasks": dict(self.tasks),
                "edges": [list(e) for e in self.edges]}


def load_instance(path: str | Path) -> Instance:
    d = json.loads(Path(path).read_text())
    inst = Instance(
        name=d["name"],
        tasks={k: float(v) for k, v in d["tasks"].items()},
        edges=[(s, t, float(z)) for s, t, z in d.get("edges", [])],
        source=d.get("source", ""),
    )
    inst.validate()
    return inst


def save_instance(inst: Instance, path: str | Path) -> None:
    Path(path).write_text(json.dumps(inst.to_dict(), indent=2) + "\n")
