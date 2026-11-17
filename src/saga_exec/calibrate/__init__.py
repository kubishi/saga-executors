"""Calibration: measuring node speeds and link bandwidths (NOT RUN YET).

  cpu.py    sha256 hash rate of the cpu-mode loop per task slot on each machine (Slurm job;
            single + all cores busy) -> machines.yaml calibration.cpu.slurm (+ speed).
            `uv run saga-calibrate-cpu`. k8s: outlined there.
  links.py  self / intra / inter transfer time through each backend's real data path,
            fit latency + size / bandwidth. Slurm runner implemented, Wayline outlined.

Older pieces:
  - Slurm: backends/slurm/profiling_test.py (zlib on 1 core / all Slurm CPUs per machine
    -> rough relative speeds; superseded by cpu.py).
  - Wayline: Config.hash_rate in backends/wayline/convert.py (cpu mode: sha256 hashes/s of
    the reference machine) -- measured by hand so far; no script in Nick's fork.
  - Bandwidth: iperf3 pod-to-pod, 2026-10-27 (testbed/machines.yaml); no script committed.
  - Self / intra-machine bandwidth (and per-transfer latency) through Wayline's data
    path: links.py (outline + offline template builder; not run yet).

Output should update testbed/machines.yaml (speed, network.links).
"""
