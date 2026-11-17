"""LEGACY: per-core SAGA network (the students' shared model, as a draft builder).

Was saga_exec.network. Both students modelled a machine with C cores as C SAGA
nodes, free links between cores of one machine and measured bandwidth across
machines. SAGA scheduling now goes through Wayline's bridge, which models a
machine as ONE SAGA node with capacity = free cores (saga_exec.scheduling);
this module is kept for the legacy-vs-bridge comparison
(tests/test_legacy_vs_bridge.py). Testbed loading moved to saga_exec.testbed.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from saga import Network

DEFAULT_MACHINES_YAML = Path(__file__).resolve().parents[2] / "testbed" / "machines.yaml"


@dataclass
class Machines:
    """What saga_exec needs from testbed/machines.yaml."""
    speed: dict[str, float]
    bandwidth: dict[tuple[str, str], float]          # directed, bytes/s
    local_bandwidth: float = math.inf
    cores: dict[str, int] = field(default_factory=dict)   # SAGA nodes per machine, per partition

    def link(self, a: str, b: str) -> float:
        """Undirected link speed: min of both directions (what Wayline's bridge does)."""
        vals = [v for k, v in self.bandwidth.items() if k in ((a, b), (b, a))]
        if not vals:
            raise KeyError(f"no bandwidth between {a} and {b}")
        return min(vals)


def load_machines(path: str | Path = DEFAULT_MACHINES_YAML) -> Machines:
    d = yaml.safe_load(Path(path).read_text())
    speed = {m: float(v.get("speed", 1.0)) for m, v in d["machines"].items()}
    bw = {(link["src"], link["dst"]): float(link["bandwidth"]) for link in d["network"]["links"]}
    local = float(d["network"].get("local_bandwidth", math.inf))
    cores = {p: int(v["saga_nodes_per_machine"]) for p, v in d.get("partitions", {}).items()
             if isinstance(v, dict) and "saga_nodes_per_machine" in v}
    return Machines(speed=speed, bandwidth=bw, local_bandwidth=local, cores=cores)


def core_name(machine: str, i: int, sep: str = "-") -> str:
    return f"{machine}{sep}c{i}"


def machine_of(node: str, sep: str = "-") -> str:
    return node.rsplit(f"{sep}c", 1)[0]


def build_core_network(
    machines: Machines,
    cores_per_machine: int | dict[str, int],
    *,
    sep: str = "-",
    local_bandwidth: float | None = None,
    speed: dict[str, float] | None = None,
) -> Network:
    """C SAGA nodes per machine, named f"{machine}{sep}c{i}".

    Same-machine pairs (including self-loops) get local_bandwidth (default
    machines.local_bandwidth); cross-machine pairs get machines.link(a, b).
    `speed` overrides machines.speed (e.g. emulated speeds in sleep mode).
    """
    speed = speed or machines.speed
    local = machines.local_bandwidth if local_bandwidth is None else local_bandwidth
    if isinstance(cores_per_machine, int):
        cores_per_machine = {m: cores_per_machine for m in speed}

    nodes, names = [], []
    for m, s in speed.items():
        for i in range(cores_per_machine[m]):
            names.append(core_name(m, i, sep))
            nodes.append((names[-1], s))
    owner = {n: machine_of(n, sep) for n in names}

    edges = []
    for a, b in itertools.combinations_with_replacement(names, 2):
        ma, mb = owner[a], owner[b]
        edges.append((a, b, local if ma == mb else machines.link(ma, mb)))
    return Network.create(nodes=nodes, edges=edges)
