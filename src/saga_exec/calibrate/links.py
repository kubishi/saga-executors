"""OUTLINE (not run yet): self / intra / inter link speeds through each backend's real data path.

SAGA's model (saga_exec.scheduling.ModelConfig, links "measured") uses, per machine,
  self_bandwidth   producer and consumer on the SAME core, back to back (the
                   payload still goes through the backend's data path: Wayline's
                   agent, a local file (Slurm direct) or the shared FS (Slurm shared_fs))
  intra_bandwidth  producer and consumer on different cores of the same machine
and network.links across machines (iperf3 so far). The first two are
placeholders in testbed/machines.yaml until this is done.

What a "transfer" is, on both backends: everything between the producer's
compute end and the consumer's compute start,
    transfer = consumer.computeStartUnix - producer.computeEndUnix
             = producer handoff (+ push request, close, exit)
               + [Wayline: cross-machine push] + consumer start-up (pod / srun step
               + interpreter) + consumer input_read
which is exactly the gap SAGA's model fills with size / bandwidth. What it
contains differs by data path (docs/data-movement.md): Wayline pushes between
the two tasks; Slurm "direct" pulls inside the consumer's input_read; Slurm
"shared_fs" writes over NFS in the producer's handoff and reads in input_read.
So each (backend, transport) gets its own fit. Fitting
    transfer(size) = latency + size / bandwidth
per (backend, machine, case) separates the per-transfer fixed cost (pod or
step start-up; the future model.transfer_latency) from the bandwidth.

Experiments: for each machine m, payload size s in SIZES, case in CASES, REPS times:
  self   chain p -> c, both on m, c after p (one task at a time on m)
  intra  two chains p0 -> c0, p1 -> c1 on m at once (cores busy in parallel)
  inter  p on m, c on the other machine
sleep mode with ~0 runtime.

Wayline: build_chain_template() (offline) -> run.py -> status.json. Transfer from
the SDK fields (status gives taskStartTime + the phase seconds). Not implemented.

Slurm: slurm_chain_plan() builds the plan in saga_slurm.build_plan's format, run
through the real backend (allocation.run_allocation: one job per point, real
transfers with the chosen transport, direct or shared_fs); transfer_seconds() reads results.json;
fit() is the least-squares fit; run_slurm() loops (submits jobs!). Writing the
fitted bandwidths into machines.yaml self_bandwidth / intra_bandwidth is left to
do after looking at the first results (latency may dominate small sizes).

Usage (on cse-lambda1, submits Slurm jobs):
    uv run saga-calibrate-links --machine cse-isha [--case inter --consumer-machine cse-lambda1]
                                [--transport direct|shared_fs]
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
    raise NotImplementedError("Wayline calibration runs are not implemented yet; see the module docstring")


# ---------------------------------------------------------------------------
# Slurm (through the real transport)
# ---------------------------------------------------------------------------

def slurm_chain_plan(machine: str, size: int, case: str, consumer_machine: str | None = None,
                     seconds: float = 0.0) -> dict:
    """Plan (saga_slurm.build_plan format) for one measurement point."""
    from saga_exec.backends.slurm.saga_slurm import manual_plan
    if case not in CASES:
        raise ValueError(f"case must be one of {CASES}")
    if case == "inter" and (not consumer_machine or consumer_machine == machine):
        raise ValueError("inter needs a consumer_machine different from machine")
    dst = consumer_machine if case == "inter" else machine
    chains = 2 if case == "intra" else 1
    tasks, edges = {}, []
    for k in range(chains):
        tasks[f"p{k}"] = (machine, seconds)
        tasks[f"c{k}"] = (dst, seconds)
        edges.append((f"p{k}", f"c{k}", size))
    return manual_plan(tasks, edges)


def transfer_seconds(results: dict) -> list[float]:
    """results.json "tasks" -> consumer compute start - producer compute end, per chain."""
    out = []
    for name in sorted(results):
        if name.startswith("p"):
            p, c = results[name]["timings"], results["c" + name[1:]]["timings"]
            out.append(c["computeStartUnix"] - p["computeEndUnix"])
    return out


def fit(sizes, times) -> tuple[float, float]:
    """Least squares time = latency + size / bandwidth. Returns (latency s, bandwidth B/s)."""
    import numpy as np
    x, y = np.asarray(sizes, dtype=float), np.asarray(times, dtype=float)
    if len(set(x.tolist())) < 2:
        raise ValueError("need at least two different sizes")
    slope, latency = np.polyfit(x, y, 1)
    return float(latency), (float("inf") if slope <= 0 else float(1.0 / slope))


def run_slurm(machine: str, case: str, consumer_machine: str | None = None, sizes=SIZES, reps: int = REPS,
              mem: str = "2G", transport: str = "direct") -> dict:
    """Submit one Slurm job per (size, rep) and fit. NOT RUN YET."""
    import json
    from datetime import datetime
    from types import SimpleNamespace

    from saga_exec.backends.slurm.allocation import run_allocation
    from saga_exec.backends.slurm.slurm_common import RUNS_DIR
    from saga_exec.testbed import load_testbed

    tb = load_testbed()
    config = SimpleNamespace(cpus=1, mem=mem, mode="sleep", hash_rate=None, machine_speed=dict(tb.speed),
                             task_python="$HOME/.local/bin/python3.12", transport=transport,
                             workdir=tb.partition("slurm").raw.get("shared_workdir"), latency_wait=5.0,
                             keep_data=False, scratch=None, server_port=0, server_bind={}, poll=1.0)
    points = []
    for size in sizes:
        for rep in range(reps):
            run_id = f"calib-{transport}-{case}-{machine}-{size}-{rep}-{datetime.now():%H%M%S}"
            plan = slurm_chain_plan(machine, size, case, consumer_machine)
            run_allocation(run_id, plan, config, tb)
            results = json.loads((RUNS_DIR / run_id / "results.json").read_text())["tasks"]
            points += [(size, t) for t in transfer_seconds(results)]
    latency, bandwidth = fit([s for s, _ in points], [t for _, t in points])
    return {"machine": machine, "case": case, "consumer_machine": consumer_machine, "transport": transport,
            "latency": latency, "bandwidth": bandwidth, "points": points}


def main() -> None:
    import argparse
    import json
    ap = argparse.ArgumentParser(prog="saga-calibrate-links", description="Slurm link calibration (submits jobs)")
    ap.add_argument("--machine", required=True)
    ap.add_argument("--case", choices=CASES, default="self")
    ap.add_argument("--consumer-machine")
    ap.add_argument("--reps", type=int, default=REPS)
    ap.add_argument("--transport", choices=("direct", "shared_fs"), default="direct")
    a = ap.parse_args()
    print(json.dumps(run_slurm(a.machine, a.case, a.consumer_machine, reps=a.reps, transport=a.transport), indent=2))
