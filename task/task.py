#!/usr/bin/env python3
"""Fidelity dummy task: read inputs, do work, send named outputs.

One program serves every task on both backends; everything task-specific comes
from env vars (fidelity_core.py has the FID_* contract). The work and the
timing are in fidelity_core (stdlib only); the transport moves the data:

  FID_TRANSPORT=wayline (default)  Wayline SDK, in the task image (task/Dockerfile)
  FID_TRANSPORT=direct             Slurm (default there): node-local scratch, consumers on
                                   other machines pull from a per-machine data server
                                   (transport_direct.py)
  FID_TRANSPORT=shared_fs          Slurm: files staged in a shared work dir, e.g. NFS
                                   (transport_shared_fs.py)

Commands (the Slurm backend runs these from task.pyz, a zipapp of this folder):
  task.py                 run one task (prints "FID_RESULT {json}" on Slurm)
  task.py serve ...       the direct transport's data server for one machine
  task.py calibrate ...   sha256 hash rate of the cpu-mode loop (saga_exec.calibrate.cpu)
"""
import json
import os
import sys

import fidelity_core


def make_transport(env=None):
    env = os.environ if env is None else env
    kind = env.get("FID_TRANSPORT", "wayline")
    if kind == "wayline":
        from transport_wayline import WaylineTransport
        return WaylineTransport()
    if kind == "direct":
        from transport_direct import DirectTransport
        return DirectTransport(env)
    if kind == "shared_fs":
        from transport_shared_fs import SharedFsTransport
        return SharedFsTransport(env)
    raise ValueError(f"Unknown FID_TRANSPORT {kind!r}: use wayline, direct or shared_fs")


def main(argv=None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "serve":
        from transport_direct import serve_main
        return serve_main(argv[1:])
    if argv and argv[0] == "calibrate":
        from calibrate_cpu import main as calibrate_main
        return calibrate_main(argv[1:])
    if argv and argv[0] not in ("task",):
        sys.exit(f"unknown command {argv[0]!r}: use task, serve or calibrate")
    record = fidelity_core.run(make_transport())
    if record is not None:                    # Wayline reports through its data agent instead
        print("FID_RESULT " + json.dumps(record), flush=True)


if __name__ == "__main__":
    main()
