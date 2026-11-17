"""OUTLINE (not run yet): measure self and intra-machine link bandwidth through the real data path.

SAGA's model (saga_exec.scheduling.ModelConfig, links "measured") uses, per machine,
  self_bandwidth   producer and consumer on the SAME core, back to back: Wayline
                   still moves the payload through its data path (task -> data
                   agent -> next task), so this is not free
  intra_bandwidth  producer and consumer on different cores of the same machine
and the iperf3 numbers in machines.yaml network.links across machines. The
first two are placeholders in testbed/machines.yaml until this is done.

Plan (Wayline; Slurm moves no data, so its transfers are simulated with whatever
SAGA assumed):
  1. For each machine m and payload size s in SIZES, a 2-task chain p -> c with one
     edge of s bytes, both pinned to m (constraints.nodeNames: [m]), scheduler
     "random" (placement is pinned anyway), enactOrder "serial" for the "self"
     case (c cannot overlap p, the closest Kubernetes gets to "same core"; true
     core pinning would need the static CPU manager) and two independent chains
     started together for the "intra" case. sleep mode with FID_SECONDS ~ 0.
  2. Run each REPS times (run.py), read the SDK phases from status.json:
     handoff (p's output write) + input_read (c's read) = transfer time.
  3. Fit time = latency + s / bandwidth per (machine, case) by least squares;
     bandwidth -> machines.yaml self_bandwidth / intra_bandwidth, latency -> the
     future model.transfer_latency (docs/formats.md, model variants).
  4. Repeat the cross-machine case (p on one machine, c on the other) to check the
     iperf3 numbers against the data path.

build_chain_template() is the offline part; run()/fit() are left to do.
"""

from __future__ import annotations

SIZES = [0, 1_000_000, 10_000_000, 100_000_000, 500_000_000]   # bytes
REPS = 10
CASES = ("self", "intra", "inter")


def build_chain_template(machine: str, size: int, case: str, consumer_machine: str | None = None) -> dict:
    """ODAGTemplate for one measurement point (offline; not applied anywhere)."""
    if case not in CASES:
        raise ValueError(f"case must be one of {CASES}")
    from saga_exec.backends.wayline.convert import IMAGE, NAMESPACE

    def task(name, deps, node, outputs=(), inputs=()):
        t = {"name": name, "image": IMAGE, "command": ["python", "task.py"], "dependencies": list(deps),
             "resources": {"cpu": "1", "memory": f"{512 + 2 * size // 2**20 + 1}Mi"},
             "constraints": {"nodeNames": [node]},
             "env": [{"name": "FID_MODE", "value": "sleep"}, {"name": "FID_SECONDS", "value": "0"},
                     {"name": "FID_SPEED", "value": f"{node}=1.0"}, {"name": "FID_HASH_RATE", "value": "0"},
                     {"name": "FID_INPUTS", "value": ",".join(f"{p}.{o}" for p, o in inputs)},
                     {"name": "FID_OUTPUTS", "value": ",".join(f"{o}:{size}" for o in outputs)}]}
        if outputs:
            t["outputs"] = [{"name": o, "dataSize": str(size)} for o in outputs]
        if inputs:
            t["inputs"] = [{"producer": p, "object": o} for p, o in inputs]
        return t

    dst = consumer_machine if case == "inter" else machine
    chains = 2 if case == "intra" else 1
    tasks = []
    for k in range(chains):
        tasks.append(task(f"p{k}", [], machine, outputs=[f"to-c{k}"]))
        tasks.append(task(f"c{k}", [f"p{k}"], dst, inputs=[(f"p{k}", f"to-c{k}")]))
    return {"apiVersion": "wl.io/v1", "kind": "ODAGTemplate",
            "metadata": {"name": f"calib-{case}-{machine}-{size}", "namespace": NAMESPACE},
            "spec": {"scheduler": "random",
                     "schedulerConfig": {"enactOrder": "serial" if case == "self" else "none"},
                     "profiling": {"enabled": False, "runtimeSource": "manual", "bandwidthSource": "external"},
                     "tasks": tasks}}


def run(*_args, **_kwargs):
    raise NotImplementedError("calibration runs are not implemented yet; see the module docstring")


def fit(*_args, **_kwargs):
    raise NotImplementedError("fit time = latency + size / bandwidth per (machine, case); see the module docstring")
