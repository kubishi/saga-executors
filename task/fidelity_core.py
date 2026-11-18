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

Exact timeline (measurement only; docs/formats.md "actual.json"). run() also takes
timestamps (time.time(), this machine's clock) immediately around each step it
already performs, without changing what it does or in which order:

  runStartUnix                  run() entered
  inputs[]                      per input, in read order: peer, producer, object, bytes,
                                readStartUnix / readEndUnix around transport.recv,
                                and what the transport knows (source machine, waits)
  workStartUnix / workEndUnix   immediately around do_work(): the pure work
  outputs[]                     per output, in send order: object, bytes,
                                generateStartUnix / generateEndUnix around payload(),
                                sendStartUnix / sendEndUnix around transport.send
  closeStartUnix / closeEndUnix around transport.close()

It is returned as record["timeline"] (Slurm: in the FID_RESULT line), or, when
the transport reports its timings elsewhere (Wayline), printed after close() as
one "FID_TIMELINE {json}" line on stdout (the pod log). Analysis uses
workStartUnix / workEndUnix for compute; computeSeconds above stays as it was.
"""
from __future__ import annotations

import hashlib
import json
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
    """Incompressible bytes, produced at memory speed.

    Real tasks produce outputs as part of their compute; generating fresh random
    bytes (~350 MB/s) would add a large artifact of this dummy task. So one random
    block of PAYLOAD_CHUNK bytes is generated once per process and outputs are
    that block repeated. Still incompressible for gzip (32 KB window), which is
    what Wayline's agent can use for pushes (--push-compress, off by default);
    zeros would be ~free there. Generation stays timed separately in the task's
    timeline."""
    block = _random_block()
    if nbytes <= len(block):
        return block[:nbytes]
    reps, rest = divmod(nbytes, len(block))
    return block * reps + block[:rest]


PAYLOAD_CHUNK = 64 * 1024 * 1024
_BLOCK: bytes | None = None


def _random_block() -> bytes:
    """One random block per process (random.randbytes overflows past ~268 MB)."""
    global _BLOCK
    if _BLOCK is None or len(_BLOCK) != PAYLOAD_CHUNK:
        _BLOCK = random.randbytes(PAYLOAD_CHUNK)
    return _BLOCK


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


class Timeline:
    """Timestamps taken around the steps run() already performs (see the module
    docstring). Measurement only: it never calls the transport or changes data."""

    def __init__(self) -> None:
        self.run_start = time.time()
        self.inputs: list[dict] = []
        self.outputs: list[dict] = []
        self.work_start: float | None = None
        self.work_end: float | None = None
        self.close_start: float | None = None
        self.close_end: float | None = None

    def to_dict(self) -> dict:
        return {
            "runStartUnix": self.run_start,
            "inputs": self.inputs,
            "workStartUnix": self.work_start,
            "workEndUnix": self.work_end,
            "workSeconds": (self.work_end - self.work_start
                            if self.work_start is not None and self.work_end is not None else None),
            "outputs": self.outputs,
            "closeStartUnix": self.close_start,
            "closeEndUnix": self.close_end,
        }


def _transport_detail(transport, attr: str) -> dict:
    """What a transport recorded about its last recv/send (e.g. the source machine,
    latency waits); {} if it records nothing. Read only."""
    info = getattr(transport, attr, None)
    return dict(info) if isinstance(info, dict) else {}


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


def read_inputs(transport: Transport, env, timeline: Timeline | None = None) -> int:
    """Read every input in FID_INPUTS; return total bytes. Reading is part of the
    measured cost, so inputs are read even though they are not used."""
    total = 0
    for peer in parse_list(env.get("FID_INPUTS", "")):
        start = time.time()
        data = transport.recv(peer)
        end = time.time()
        total += len(data)
        if timeline is not None:
            producer, _, obj = peer.partition(".")
            timeline.inputs.append({"peer": peer, "producer": producer, "object": obj, "bytes": len(data),
                                    "readStartUnix": start, "readEndUnix": end,
                                    **_transport_detail(transport, "last_recv_info")})
        del data
    return total


def send_outputs(transport: Transport, env, timeline: Timeline | None = None) -> None:
    """Send each output in FID_OUTPUTS. A leaf (no outputs) sends b"done" to its
    default output, as E11 and the original task.py did."""
    outputs = parse_outputs(env.get("FID_OUTPUTS", ""))
    if not outputs:
        start = time.time()
        transport.send(None, b"done")
        end = time.time()
        if timeline is not None:
            timeline.outputs.append({"object": None, "bytes": 4, "generateStartUnix": None,
                                     "generateEndUnix": None, "sendStartUnix": start, "sendEndUnix": end,
                                     **_transport_detail(transport, "last_send_info")})
        return
    for name, size in outputs:
        gen_start = time.time()
        data = payload(size)
        gen_end = time.time()
        transport.send(name, data)
        send_end = time.time()
        if timeline is not None:
            timeline.outputs.append({"object": name, "bytes": size, "generateStartUnix": gen_start,
                                     "generateEndUnix": gen_end, "sendStartUnix": gen_end,
                                     "sendEndUnix": send_end, **_transport_detail(transport, "last_send_info")})
        del data


def run(transport: Transport, env=None) -> dict | None:
    """One task: inputs -> work -> outputs -> close. Returns the transport's timing
    record with the exact timeline added (record["timeline"]); a transport that
    reports elsewhere (Wayline, close() -> None) returns None, and the timeline
    is printed as a "FID_TIMELINE {json}" line after close()."""
    env = os.environ if env is None else env
    timeline = Timeline()
    mode = env.get("FID_MODE", "sleep")
    base = float(env["FID_SECONDS"])
    hash_rate = float(env.get("FID_HASH_RATE") or 0) or None

    in_bytes = read_inputs(transport, env, timeline)
    speed = node_speed(env, transport.node)

    t = time.perf_counter()
    timeline.work_start = time.time()
    do_work(mode, base, speed, hash_rate)
    timeline.work_end = time.time()
    elapsed = time.perf_counter() - t

    send_outputs(transport, env, timeline)
    print(f"[fidelity] task={transport.name} node={transport.node} mode={mode} "
          f"planned={base / speed:.3f} measured={elapsed:.3f} in_bytes={in_bytes}", flush=True)
    timeline.close_start = time.time()
    record = transport.close()
    timeline.close_end = time.time()
    tl = timeline.to_dict()
    if record is None:
        # Reported after close(), so printing cannot shift any measured instant.
        print("FID_TIMELINE " + json.dumps({"task": transport.name, "node": transport.node, **where(),
                                            "timeline": tl}), flush=True)
        return None
    record["timeline"] = tl
    return record
