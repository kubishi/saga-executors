"""The testbed (testbed/machines.yaml): machines, speeds, links, per-backend capacity.

Everything SAGA learns about the hardware comes from here, for both backends:

    tb = load_testbed()
    tb.speed["cse-isha"]                 # relative speed
    tb.bandwidth_between("a", "b")       # directed bytes/s (default_bandwidth if unlisted)
    tb.self_bandwidth["cse-isha"]        # same-core link (UNMEASURED placeholder)
    tb.intra_bandwidth["cse-isha"]       # core-to-core link within a machine (placeholder)
    tb.partition("k8s").cores            # SAGA cores (one task at a time) per machine
    tb.partition("k8s").cpu_millis       # node CPU as the controller reports it
    tb.partition("slurm").task_cpu       # default per-task CPU request ("1")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_MACHINES_YAML = Path(__file__).resolve().parents[2] / "testbed" / "machines.yaml"
DEFAULT_BANDWIDTH = 125_000_000.0   # Wayline's default (bandwidth.go heftDefaultBandwidth)


@dataclass(frozen=True)
class Partition:
    """One backend's share of the machines, as SAGA sees it."""
    name: str
    cores: dict[str, int]               # machine -> SAGA cores (saga_nodes_per_machine)
    cpu_millis: dict[str, int]          # machine -> node CPU reported in the request
    task_cpu: str = "1"                 # default CPU request per task (Kubernetes quantity)
    raw: dict = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class Testbed:
    machines: list[str]                         # in file order
    speed: dict[str, float]
    bandwidth: dict[tuple[str, str], float]     # directed, bytes/s
    default_bandwidth: float = DEFAULT_BANDWIDTH
    partitions: dict[str, Partition] = field(default_factory=dict)
    self_bandwidth: dict[str, float] = field(default_factory=dict)    # same core, bytes/s
    intra_bandwidth: dict[str, float] = field(default_factory=dict)   # core to core, bytes/s

    def bandwidth_between(self, src: str, dst: str) -> float:
        """Directed link src -> dst, as Wayline's "external" bandwidth source resolves it."""
        return self.bandwidth.get((src, dst), self.default_bandwidth)

    def partition(self, name: str) -> Partition:
        try:
            return self.partitions[name]
        except KeyError:
            raise KeyError(f"no partition {name!r} with saga_cpu_millis in machines.yaml; "
                           f"have {sorted(self.partitions)}") from None


def load_testbed(path: str | Path = DEFAULT_MACHINES_YAML) -> Testbed:
    d = yaml.safe_load(Path(path).read_text())
    machines = list(d["machines"])
    speed = {m: float(v.get("speed", 1.0)) for m, v in d["machines"].items()}
    net = d.get("network", {})
    bw = {(link["src"], link["dst"]): float(link["bandwidth"]) for link in net.get("links", [])}
    for a, b in bw:
        for m in (a, b):
            if m not in speed:
                raise ValueError(f"{path}: link endpoint {m!r} is not a machine")
    partitions = {}
    for name, p in (d.get("partitions") or {}).items():
        if isinstance(p, dict) and "saga_cpu_millis" in p:
            cpu = {m: int(v) for m, v in p["saga_cpu_millis"].items()}
            if set(cpu) != set(machines):
                raise ValueError(f"{path}: partitions.{name}.saga_cpu_millis must list {machines}")
            n = p.get("saga_nodes_per_machine", 1)
            cores = {m: int(n[m] if isinstance(n, dict) else n) for m in machines}
            partitions[name] = Partition(name=name, cores=cores, cpu_millis=cpu,
                                         task_cpu=str(p.get("task_cpu", "1")), raw=p)
    local = {k: {m: float(v[k]) for m, v in d["machines"].items() if k in v}
             for k in ("self_bandwidth", "intra_bandwidth")}
    return Testbed(machines=machines, speed=speed, bandwidth=bw,
                   default_bandwidth=float(net.get("default_bandwidth", DEFAULT_BANDWIDTH)),
                   partitions=partitions, self_bandwidth=local["self_bandwidth"],
                   intra_bandwidth=local["intra_bandwidth"])
