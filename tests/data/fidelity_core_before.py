# Frozen copy of task/fidelity_core.py before the timing instrumentation (commit e46872d).
# tests/test_timeline.py runs it next to the current core to check that the task
# program performs exactly the same operations in the same order. Do not edit.
"""Fidelity task core: read inputs, do work, write outputs. Backend-agnostic, stdlib only.

The same core runs in Wayline's task image and as a Slurm step (on cse-isha with
nothing but a uv-managed Python, no venv). Only the transport differs:

  transport_wayline.WaylineTransport   Wayline SDK (wl.WlTask, data agent)
  transport_direct.DirectTransport     Slurm: node-local scratch, cross-machine inputs pulled
                                       from a per-machine data server
  transport_shared_fs.SharedFsTransport  Slurm: files staged in a shared work dir (NFS)

Env contract (FID_*; see docs/data-movement.md for the transport-specific ones):

  FID_MODE       sleep | cpu
  FID_SECONDS    runtime at speed 1.0, seconds (already scaled by sec_per_cost)
  FID_SPEED      "machine=speed,..."; sleep mode sleeps FID_SECONDS / speed[this machine]
  FID_HASH_RATE  cpu mode: sha256 iterations per second of a speed-1.0 task slot
                 (calibrated, testbed/machines.yaml calibration.cpu); the task does
                 FID_SECONDS * FID_HASH_RATE iterations, so the real machine sets the time
  FID_INPUTS     "producer.object,..."   named inputs to read (all are read, then dropped)
  FID_OUTPUTS    "object:bytes,..."      named outputs to write (random bytes)
  FID_TRANSPORT  wayline (default) | direct | shared_fs

Phase timing (same definitions on both backends, so results are comparable; the
field names are the Wayline SDK's, FileTransport._report_timings, and become
actual.json's input_read / compute / handoff). What each phase CONTAINS depends on
the transport (docs/data-movement.md):

  taskStartUnix     transport created (after interpreter start + imports)
  inputReadSeconds  first input read starts -> last input read ends
  computeSeconds    last input read ends (or task start) -> first output send starts
  handoffSeconds    first output send starts -> last output installed locally
  closeUnix         close(); actual.json "end"

Because outputs are generated just before each send (as in the original task.py),
generating the FIRST output's random bytes falls in compute and generating later
ones in handoff. Kept as is so Wayline results stay comparable with earlier runs.
"""
from __future__ import annotations

import hashlib
import os
import random
import socket
import time
from pathlib import Path

HASH_SEED = b"wl-fidelity"


def parse_map(s: str, sep: str) -> dict[str, str]:
    """"a=1,b=2" -> {"a": "1", "b": "2"} (sep is "=" or ":"). Empty -> {}."""
    result = {}
    for item in (s or "").split(","):
        if not item.strip():
            continue
        key, value = item.split(sep, 1)
        result[key.strip()] = value.strip()
    return result


def parse_list(s: str) -> list[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def parse_outputs(s: str) -> list[tuple[str, int]]:
    """FID_OUTPUTS "obj:bytes,..." -> [(obj, bytes)] in order."""
    return [(k, int(v)) for k, v in parse_map(s, ":").items()]


def node_speed(env, node: str) -> float:
    """Speed factor for this node from FID_SPEED; default 1.0."""
    speed = float(parse_map(env.get("FID_SPEED", ""), "=").get(node, 1.0))
    if speed <= 0:
        raise ValueError(f"FID_SPEED for {node} must be > 0, got {speed}")
    return speed


def hash_loop(iterations: int) -> bytes:
    """The cpu-mode work: chained sha256, one 32-byte digest per iteration
    (E11's spin()). Calibration measures exactly this loop."""
    d = HASH_SEED
    sha = hashlib.sha256
    for _ in range(iterations):
        d = sha(d).digest()
    return d


def do_work(mode: str, seconds: float, speed: float, hash_rate: float | None) -> None:
    """Do `seconds` of work as measured at speed 1.0.

    sleep: time.sleep(seconds / speed). Uses no CPU, so speed is emulated.
    cpu:   seconds * hash_rate iterations of hash_loop. Speed is not applied:
           the machine's real speed (and contention) sets the time.
    """
    if mode == "sleep":
        time.sleep(seconds / speed)
    elif mode == "cpu":
        if not hash_rate or hash_rate <= 0:
            raise ValueError("FID_HASH_RATE must be set (> 0) in cpu mode")
        hash_loop(int(seconds * hash_rate))
    else:
        raise ValueError(f"Unknown FID_MODE {mode!r}: use sleep or cpu")


def payload(nbytes: int) -> bytes:
    """Random (incompressible) bytes. Wayline's agent can gzip pushes
    (--push-compress gzip, off by default), and zeros would then be ~free."""
    return random.randbytes(nbytes)


def where() -> dict:
    """Host, Linux CPUs this process may use, and the physical core of the first one."""
    out = {"host": socket.gethostname()}
    try:
        cpus = sorted(os.sched_getaffinity(0))
        out["linux_cpus"] = ",".join(map(str, cpus))
        out["core"] = Path(f"/sys/devices/system/cpu/cpu{cpus[0]}/topology/core_id").read_text().strip()
    except (OSError, AttributeError, IndexError):
        pass
    return out


class PhaseClock:
    """The Wayline SDK's phase instrumentation (wl/transport/file.py), reproduced
    so a non-Wayline transport reports the same boundaries in the same fields."""

    def __init__(self) -> None:
        self.task_start = time.time()
        self.recv_first: float | None = None
        self.recv_last: float | None = None
        self.send_start: float | None = None
        self.send_installed: float | None = None
        self.bytes_in = 0
        self.bytes_out = 0

    def recv_begin(self) -> None:
        if self.recv_first is None:
            self.recv_first = time.time()

    def recv_end(self, nbytes: int) -> None:
        self.recv_last = time.time()
        self.bytes_in += nbytes

    def send_begin(self) -> None:
        if self.send_start is None:
            self.send_start = time.time()

    def send_end(self, nbytes: int) -> None:
        self.send_installed = time.time()
        self.bytes_out += nbytes

    def record(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        compute_start = self.recv_last if self.recv_last is not None else self.task_start
        compute_end = self.send_start if self.send_start is not None else now
        return {
            "taskStartUnix": self.task_start,
            "computeStartUnix": compute_start,
            "computeEndUnix": compute_end,
            "closeUnix": now,
            "computeSeconds": max(compute_end - compute_start, 0.0),
            "inputReadSeconds": (max(self.recv_last - self.recv_first, 0.0)
                                 if self.recv_first is not None and self.recv_last is not None else 0.0),
            "handoffSeconds": (max(self.send_installed - self.send_start, 0.0)
                               if self.send_start is not None and self.send_installed is not None else 0.0),
            "bytesIn": self.bytes_in,
            "bytesOut": self.bytes_out,
        }


class Transport:
    """What the core needs from a backend. `name` and `node` identify the task
    and the machine it runs on; recv/send are timed by the transport itself
    (PhaseClock or the Wayline SDK), so the boundaries match the SDK exactly."""
    name: str
    node: str

    def recv(self, peer: str) -> bytes:             # peer = "producer.object"
        raise NotImplementedError

    def send(self, output: str | None, data: bytes) -> None:   # None = default output
        raise NotImplementedError

    def close(self) -> dict | None:                 # timing record, or None if reported elsewhere
        raise NotImplementedError


def read_inputs(transport: Transport, env) -> int:
    """Read every input in FID_INPUTS; return total bytes. Reading is part of the
    measured cost, so inputs are read even though they are not used."""
    total = 0
    for peer in parse_list(env.get("FID_INPUTS", "")):
        data = transport.recv(peer)
        total += len(data)
        del data
    return total


def send_outputs(transport: Transport, env) -> None:
    """Send each output in FID_OUTPUTS. A leaf (no outputs) sends b"done" to its
    default output, as E11 and the original task.py did."""
    outputs = parse_outputs(env.get("FID_OUTPUTS", ""))
    if not outputs:
        transport.send(None, b"done")
        return
    for name, size in outputs:
        data = payload(size)
        transport.send(name, data)
        del data


def run(transport: Transport, env=None) -> dict | None:
    """One task: inputs -> work -> outputs -> close. Returns the transport's timing record."""
    env = os.environ if env is None else env
    mode = env.get("FID_MODE", "sleep")
    base = float(env["FID_SECONDS"])
    hash_rate = float(env.get("FID_HASH_RATE") or 0) or None

    in_bytes = read_inputs(transport, env)
    speed = node_speed(env, transport.node)

    t = time.perf_counter()
    do_work(mode, base, speed, hash_rate)
    elapsed = time.perf_counter() - t

    send_outputs(transport, env)
    print(f"[fidelity] task={transport.name} node={transport.node} mode={mode} "
          f"planned={base / speed:.3f} measured={elapsed:.3f} in_bytes={in_bytes}", flush=True)
    return transport.close()
