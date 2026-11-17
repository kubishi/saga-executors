"""Hash rate of the cpu-mode loop (fidelity_core.hash_loop) on this machine. Stdlib only.

Run as a Slurm step on each machine by saga_exec.calibrate.cpu (via task.pyz,
so it measures the very code and interpreter the tasks use):

  single  one process on one core, the rest of the allocation idle
  loaded  one process per physical core of the allocation, all at once, each
          pinned to both hardware threads of its own core (= one Slurm task
          with --cpus-per-task=2), as when SAGA fills every core

Prints one JSON line. Rates are sha256 iterations per second per task slot.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import platform
import socket
import ssl
import statistics
import time
from pathlib import Path

from fidelity_core import hash_loop


def physical_cores(cpus: list[int]) -> list[list[int]]:
    """Group Linux CPUs by physical core (sysfs topology); unknown -> one CPU per core."""
    groups: dict[tuple[str, str], list[int]] = {}
    for c in cpus:
        base = Path(f"/sys/devices/system/cpu/cpu{c}/topology")
        try:
            key = ((base / "physical_package_id").read_text().strip(), (base / "core_id").read_text().strip())
        except OSError:
            key = ("?", str(c))
        groups.setdefault(key, []).append(c)
    return sorted(groups.values())


def _worker(cpus, iterations, barrier, out) -> None:
    os.sched_setaffinity(0, cpus)
    barrier.wait()
    t = time.perf_counter()
    hash_loop(iterations)
    out.put(iterations / (time.perf_counter() - t))


def timed(cores: list[list[int]], iterations: int) -> list[float]:
    """Run one pinned process per core at once; per-process rates."""
    ctx = mp.get_context("fork")
    barrier, out = ctx.Barrier(len(cores)), ctx.Queue()
    procs = [ctx.Process(target=_worker, args=(c, iterations, barrier, out)) for c in cores]
    for p in procs:
        p.start()
    rates = [out.get() for _ in procs]
    for p in procs:
        p.join()
    return rates


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="task.pyz calibrate")
    ap.add_argument("--seconds", type=float, default=10.0, help="target length of one measurement")
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args(argv)

    cpus = sorted(os.sched_getaffinity(0))
    cores = physical_cores(cpus)
    probe = timed(cores[:1], 200_000)[0]
    iterations = max(1, int(probe * a.seconds))
    single = [timed(cores[:1], iterations)[0] for _ in range(a.reps)]
    loaded_reps = [timed(cores, iterations) for _ in range(a.reps)]
    loaded = [statistics.mean(r) for r in loaded_reps]
    print(json.dumps({
        "machine": socket.gethostname(),
        "python": platform.python_version(),
        "python_build": " ".join(platform.python_build()),
        "openssl": ssl.OPENSSL_VERSION,
        "linux_cpus": ",".join(map(str, cpus)),
        "cores": len(cores),
        "iterations": iterations,
        "single": statistics.median(single),
        "loaded": statistics.median(loaded),
        "loaded_min_process": min(min(r) for r in loaded_reps),
        "single_reps": single,
        "loaded_reps": loaded,
    }), flush=True)


if __name__ == "__main__":
    main()
