"""Tests for Sebastian's LEGACY build_network (saga_exec/legacy/slurm_saga.py).

How pytest works: it finds files named test_*.py, runs every function named
test_*, and a test fails if any `assert` in it is false (or it crashes).

Run from the repo root:
    uv run pytest tests/test_slurm_network.py        # add -v to see each test's name

(Moved from executor/test_network.py in Sebastian's slurm-cluster repo. The Slurm
backend now schedules through Wayline's bridge; see test_slurm.py.)
"""

from types import SimpleNamespace

import pytest

from saga_exec.legacy.slurm_saga import build_network


def test_one_node_per_core():
    # 2 cores per machine keeps the expected list short; isha is half speed.
    config = SimpleNamespace(nodes={"cse-isha": 0.5}, bandwidth=1e9, cores=2)
    net = build_network(config)

    # One saga node per core, each with its machine's speed (lambda left out, so 1.0).
    assert {n.name: n.speed for n in net.nodes} == {
        "cse-lambda1:c0": 1.0, "cse-lambda1:c1": 1.0,
        "cse-isha:c0": 0.5, "cse-isha:c1": 0.5,
    }
    # Cores on the same machine share memory, so moving data between them is free.
    assert net.get_edge("cse-lambda1:c0", "cse-lambda1:c1").speed == float("inf")
    # Cores on different machines use the run file's bandwidth.
    assert net.get_edge("cse-lambda1:c0", "cse-isha:c0").speed == 1e9


def test_unknown_machine_stops_the_run():
    # A typo in "nodes" should stop with an error (sys.exit), not be ignored.
    config = SimpleNamespace(nodes={"cse-typo": 1.0}, bandwidth=1e9, cores=2)
    with pytest.raises(SystemExit):
        build_network(config)
