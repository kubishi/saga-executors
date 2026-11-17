#!/usr/bin/env python3
"""Instance -> Wayline ODAGTemplate that SAGA schedules in the cluster, + offline prediction.

Nothing here touches the cluster. Wayline's controller runs SAGA itself
(spec.scheduler "saga/<alg>" -> saga-sidecar -> bridge.py) when a run starts;
the template only carries what SAGA needs to see, in the form the controller
reads it:

  per task   runtime (s at speed 1.0) + runtimeProfile {machine: s} = cost / speed,
             resources.cpu (SAGA demand), dependencies, one named output per edge
             (dataSize = bytes) and the matching inputs (per-edge sizes)
  spec       scheduler saga/<alg>, schedulerConfig {enactOrder, options, model}
             (model: the bridge's "cores" node model -- C one-task-at-a-time
             SAGA nodes per machine -- and the self/intra-machine link speeds),
             profiling {runtimeSource: manual, bandwidthSource: external}
             (manual: the controller ignores profiler data, so SAGA sees exactly
             our runtimes; external: bandwidths from the wl-network-profile ConfigMap)

No placement or order is pinned: whatever SAGA decides in the cluster is
enacted (placement always; per-machine start order with enact_order "order").
The controller records SAGA's prediction in status.scheduling; analyze.py reads
it from there. At generate time the same request is also scheduled offline
through the same bridge (saga_exec.scheduling), so the two can be compared.

Output, per instance and Config, in <out_root>/<graph>/<run name>/:
  template.yml           kubectl apply this (run.py does)
  network-profile.yml    wl-network-profile ConfigMap from machines.yaml (apply once)
  request.json           the bridge request the controller should send
  prediction.json        offline bridge prediction (skipped, with a warning, if the bridge can't run)
  instance.json, config.json
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import warnings
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml

from saga_exec.instance import Instance, load_instance
from saga_exec.scheduling import (
    ENACT_ORDERS,
    BridgeError,
    ModelConfig,
    SchedulingConfig,
    bridge_model,
    build_request,
    prediction_from_response,
    run_bridge,
    runtimes,
    task_inputs,
)
from saga_exec.testbed import Testbed, load_testbed

IMAGE = "cse-lambda1:5000/wl-fidelity-task:latest"
NAMESPACE = "wl-runs"
CONFIGMAP_NAME = "wl-network-profile"   # wayline bandwidth.go
CONFIGMAP_NAMESPACE = "wl-system"


@dataclass(frozen=True)
class Config:
    """Settings for one conversion. Saved as config.json with each output."""
    scheduler: str = "heft"         # SAGA algorithm in the bridge (spec.scheduler "saga/<scheduler>")
    enact_order: str = "order"      # "none": placement only; "order": per-machine start order too
    mode: str = "sleep"             # task.py: "sleep" (emulated speed) or "cpu" (real work)
    speed: dict[str, float] = field(default_factory=dict)   # overrides machines.yaml speeds
    sec_per_cost: float = 1.0
    bytes_per_size: float = 1.0
    cpu: str = "1"                  # CPU request per task (pod resources; SAGA's cores model ignores it)
    hash_rate: float = 0.0          # cpu mode: hashes/s of the reference machine (speed 1.0)
    memory: str | None = None       # e.g. "2Gi" for every task; None = calculated per task
    options: dict = field(default_factory=dict)   # SAGA scheduler constructor options
    model: ModelConfig = field(default_factory=ModelConfig)   # SAGA model variant (cores, links, ...)

    def __post_init__(self):
        object.__setattr__(self, "model", ModelConfig.coerce(self.model))
        if self.mode not in ("sleep", "cpu"):
            raise ValueError(f"Unknown mode {self.mode!r}: use sleep or cpu")
        if self.mode == "cpu" and self.hash_rate <= 0:
            raise ValueError("cpu mode needs hash_rate > 0, or tasks do no work")
        if self.enact_order not in ENACT_ORDERS:
            raise ValueError(f"enact_order must be one of {ENACT_ORDERS}, got {self.enact_order!r}")

    def scheduling(self) -> SchedulingConfig:
        return SchedulingConfig(scheduler=self.scheduler, partition="k8s", sec_per_cost=self.sec_per_cost,
                                bytes_per_size=self.bytes_per_size, speed=dict(self.speed) or None,
                                task_cpu=self.cpu, options=dict(self.options), model=self.model)

    def run_name(self) -> str:
        """Readable settings plus a hash of all of them, so that two Configs
        differing only in speed or scaling never share a template name."""
        digest = hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()
        alg = re.sub(r"[^a-z0-9]+", "-", self.scheduler.lower()).strip("-")[-20:].strip("-")
        return f"{alg}-{self.enact_order}-{self.mode}-{digest[:6]}"


def check_k8s_name(name: str, max_len: int = 63) -> None:
    """Pods and containers are named after tasks, so names must be DNS labels."""
    if len(name) > max_len or not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", name):
        raise ValueError(f"{name!r} is not a valid Kubernetes name (max {max_len} chars)")


def to_template(inst: Instance, cfg: Config, tb: Testbed) -> dict:
    """ODAGTemplate (as a dict). Task fields feed the controller's SAGA request
    (saga.go); env feeds task/task.py (FID_*), unchanged from the legacy converter."""
    sched = cfg.scheduling()
    speed = {m: (sched.speed or {}).get(m, s) for m, s in tb.speed.items()}
    fid_speed = ",".join(f"{m}={s}" for m, s in speed.items())
    inputs = task_inputs(inst, cfg.bytes_per_size)
    outputs: dict[str, list[tuple[str, int]]] = {}
    for ins in inputs.values():
        for src, obj, nbytes in ins:
            outputs.setdefault(src, []).append((obj, nbytes))
    rts = runtimes(inst, tb, sched)

    tasks = []
    for name in inst.tasks:
        check_k8s_name(name)
        seconds, profile = rts[name]
        task_inputs_ = inputs[name]
        task_outputs = outputs.get(name, [])
        memory = cfg.memory
        if memory is None:
            # task.py holds payloads in memory, and the limit is hard (OOM kill).
            # 512 MiB + 2x is a safety margin, not a measurement.
            held = sum(n for *_, n in task_inputs_) + sum(n for _, n in task_outputs)
            memory = f"{512 + math.ceil(2 * held / 2**20)}Mi"
        task = {
            "name": name,
            "image": IMAGE,
            "command": ["python", "task.py"],
            "dependencies": [p for p, _, _ in task_inputs_],
            "resources": {"cpu": cfg.cpu, "memory": memory},
            "runtime": seconds,
            "runtimeProfile": profile,
            "env": [
                {"name": "FID_MODE", "value": cfg.mode},
                {"name": "FID_SECONDS", "value": str(seconds)},
                {"name": "FID_SPEED", "value": fid_speed},
                {"name": "FID_HASH_RATE", "value": str(cfg.hash_rate)},
                {"name": "FID_INPUTS", "value": ",".join(f"{p}.{o}" for p, o, _ in task_inputs_)},
                {"name": "FID_OUTPUTS", "value": ",".join(f"{o}:{n}" for o, n in task_outputs)},
            ],
        }
        if task_inputs_:
            task["inputs"] = [{"producer": p, "object": o} for p, o, _ in task_inputs_]
        if task_outputs:
            task["outputs"] = [{"name": o, "dataSize": str(n)} for o, n in task_outputs]
        tasks.append(task)

    # Runs are named <template>-run-xxxxx and that name becomes a label value
    # (max 63), so leave room for the 10-char suffix.
    template_name = f"{inst.name}-{cfg.run_name()}"
    check_k8s_name(template_name, max_len=53)

    scheduler_config: dict = {"enactOrder": cfg.enact_order}
    if cfg.options:
        scheduler_config["options"] = dict(cfg.options)
    model = bridge_model(tb, sched)
    if model:
        scheduler_config["model"] = model
    return {
        "apiVersion": "wl.io/v1",
        "kind": "ODAGTemplate",
        "metadata": {
            "name": template_name,
            "namespace": NAMESPACE,
            "labels": {
                "fidelity/scheduler": re.sub(r"[^A-Za-z0-9_.-]", "-", cfg.scheduler)[:63],
                "fidelity/enact-order": cfg.enact_order,
                "fidelity/model-nodes": cfg.model.nodes,
                "fidelity/model-links": cfg.model.links,
                "fidelity/mode": cfg.mode,
                "fidelity/cpu": cfg.cpu,
                "fidelity/sec-per-cost": str(cfg.sec_per_cost),
                "fidelity/bytes-per-size": str(cfg.bytes_per_size),
                "fidelity/hash-rate": str(cfg.hash_rate),
            },
            # "=" and "," are not allowed in label values
            "annotations": {"fidelity/speed": fid_speed},
        },
        "spec": {
            "scheduler": f"saga/{cfg.scheduler}",
            "schedulerConfig": scheduler_config,
            "profiling": {
                "enabled": False,
                "runtimeSource": "manual",
                "bandwidthSource": "external",
            },
            "retention": {"data": {"policy": "keepLatest", "keepRuns": 1}},
            "tasks": tasks,
        },
    }


def network_configmap(tb: Testbed) -> dict:
    """wl-network-profile ConfigMap (bandwidthSource external) from machines.yaml."""
    data = {"defaultBandwidth": str(int(tb.default_bandwidth))}
    for (src, dst), bw in tb.bandwidth.items():
        data[f"{src}_to_{dst}"] = str(int(bw)) if float(bw).is_integer() else str(bw)
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": CONFIGMAP_NAME, "namespace": CONFIGMAP_NAMESPACE},
            "data": data}


def convert(instance: Path, cfg: Config, out_root: Path, *, testbed: Testbed | None = None,
            require_prediction: bool = False, **bridge_kw) -> Path:
    """One instance + one Config -> <out_root>/<graph name>/<run name>/. Returns that folder.

    The offline prediction needs a bridge that can schedule (SAGA with the
    capacity model); without one it is skipped with a warning, unless
    require_prediction. The template does not depend on it.
    """
    tb = testbed or load_testbed()
    inst = load_instance(instance)
    template = to_template(inst, cfg, tb)
    request = build_request(inst, tb, cfg.scheduling())

    out = out_root / inst.name / cfg.run_name()
    (out / "runs").mkdir(parents=True, exist_ok=True)   # run.py saves each run here
    (out / "template.yml").write_text(yaml.safe_dump(template, sort_keys=False))
    (out / "network-profile.yml").write_text(yaml.safe_dump(network_configmap(tb), sort_keys=False))
    (out / "instance.json").write_text(json.dumps(inst.to_dict(), indent=2) + "\n")
    (out / "config.json").write_text(json.dumps(asdict(cfg), indent=2) + "\n")
    (out / "request.json").write_text(json.dumps(request, indent=2) + "\n")
    try:
        response = run_bridge(request, **bridge_kw)
    except BridgeError as e:
        if require_prediction:
            raise
        warnings.warn(f"{out.name}: no offline prediction.json ({e})", stacklevel=2)
        (out / "prediction.json").unlink(missing_ok=True)
    else:
        prediction = prediction_from_response(request, response, {
            "backend": "wayline", "instance": inst.name, "config": cfg.run_name(),
            "enact_order": cfg.enact_order})
        (out / "prediction.json").write_text(json.dumps(prediction, indent=2) + "\n")
    print(f"wrote {out}")
    return out
