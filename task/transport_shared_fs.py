"""Shared-FS transport (Slurm, "shared_fs"): stage files through a shared work directory.

This is how workflow systems run on Slurm (Snakemake, Nextflow, Pegasus): every
machine mounts the same work dir (NFS here, testbed/slurm/nfs.md), a producer
writes its outputs there before it exits, and the scheduler starts a consumer
once its producers have completed; the consumer then reads the files. There are
no data servers. Standard library only.

  send(object, data)  write <FID_WORKDIR>/<task>.<object> as a temp file, then
                      rename it into place (readers never see a partial file).
                      On NFS, close() flushes the data to the server, so the
                      network transfer from a client machine is part of
                      handoffSeconds.
  recv(peer)          read <FID_WORKDIR>/<peer> (inputReadSeconds). On NFS, a file
                      another client just created may not be visible yet
                      (directory attribute caching), so the read retries for up
                      to FID_LATENCY_WAIT seconds, like Snakemake's --latency-wait.
                      The wait counts as input_read.
  leaf                writes <FID_WORKDIR>/<task> = b"done" (a completion marker)

Env (besides the core's FID_*): FID_TRANSPORT=shared_fs, FID_WORKDIR (this run's data
dir in the shared work dir), FID_TASK, FID_NODE, FID_LATENCY_WAIT (default 5).
"""
from __future__ import annotations

import os
import secrets
import time
from pathlib import Path

from fidelity_core import PhaseClock, where, write_payload


class SharedFsTransport:
    def __init__(self, env=None) -> None:
        env = os.environ if env is None else env
        self.clock = PhaseClock()
        self.dir = Path(env["FID_WORKDIR"])
        self.name = env["FID_TASK"]
        self.node = env["FID_NODE"]
        self.latency_wait = float(env.get("FID_LATENCY_WAIT") or 5.0)
        self.waited = 0.0
        self.last_recv_info: dict = {}       # measurement only, read by the core's timeline
        self.last_send_info: dict = {}

    def key(self, output: str | None) -> str:
        return self.name if output is None else f"{self.name}.{output}"

    def recv(self, peer: str) -> bytes:
        self.clock.recv_begin()
        path = self.dir / peer
        deadline = time.time() + self.latency_wait
        attempts, waited_before = 0, self.waited
        while True:
            attempts += 1
            try:
                with open(path, "rb") as f:
                    opened = time.time()             # measurement only: the file was visible
                    data = f.read()
                break
            except FileNotFoundError:
                if time.time() >= deadline:
                    raise
                time.sleep(0.05)
                self.waited += 0.05
        self.clock.recv_end(len(data))
        self.last_recv_info = {"openedUnix": opened, "attempts": attempts,
                               "latencyWaited": round(self.waited - waited_before, 3)}
        return data

    def send(self, output: str | None, data: bytes) -> None:
        self.clock.send_begin()
        dest = self.dir / self.key(output)
        tmp = self.dir / f".{dest.name}.tmp.{secrets.token_hex(6)}"
        try:
            with open(tmp, "wb") as f:
                write_payload(f, data)
            written = time.time()                    # measurement only: written and closed (NFS: flushed)
            os.replace(tmp, dest)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        self.clock.send_end(len(data))
        self.last_send_info = {"writtenUnix": written, "renamedUnix": self.clock.send_installed}

    def close(self) -> dict:
        rec = self.clock.record()
        rec.update(where())
        rec.update({"task": self.name, "node": self.node, "latency_waited": round(self.waited, 3)})
        return rec
