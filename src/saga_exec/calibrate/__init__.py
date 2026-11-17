"""Calibration: measuring node speeds and link bandwidths (DRAFT, empty).

Existing pieces to consolidate here:
  - Slurm: backends/slurm/profiling_test.py (zlib on 1 core / all Slurm CPUs per machine
    -> rough relative speeds for a run file's "nodes").
  - Wayline: Config.hash_rate in backends/wayline/convert.py (cpu mode: sha256 hashes/s of
    the reference machine) -- measured by hand so far; no script in Nick's fork.
  - Bandwidth: iperf3 pod-to-pod, 2026-10-27 (testbed/machines.yaml); no script committed.

Output should update testbed/machines.yaml (speed, network.links).
"""
