"""Every module imports without a cluster (no kubectl / sbatch needed at import time)."""

import importlib

import pytest

MODULES = [
    "saga_exec",
    "saga_exec.instance",
    "saga_exec.testbed",
    "saga_exec.scheduling",
    "saga_exec.schedule",
    "saga_exec.compare",
    "saga_exec.calibrate",
    "saga_exec.calibrate.links",
    "saga_exec.backends.base",
    "saga_exec.backends.wayline.convert",
    "saga_exec.backends.wayline.run",
    "saga_exec.backends.wayline.analyze",
    "saga_exec.backends.slurm.saga_slurm",
    "saga_exec.backends.slurm.allocation",
    "saga_exec.backends.slurm.driver",
    "saga_exec.backends.slurm.per_task",
    "saga_exec.backends.slurm.slurm_common",
    "saga_exec.backends.slurm.profiling_test",
    "saga_exec.legacy",
    "saga_exec.legacy.wayline_convert",
    "saga_exec.legacy.slurm_saga",
    "saga_exec.legacy.core_network",
    "bridge",               # Wayline's saga-sidecar (dependency wl-saga-sidecar)
]


@pytest.mark.parametrize("name", MODULES)
def test_import(name):
    importlib.import_module(name)


def test_calibration_outline_builds_templates():
    from saga_exec.calibrate import links

    t = links.build_chain_template("cse-isha", 1_000_000, "intra")
    assert [x["name"] for x in t["spec"]["tasks"]] == ["p0", "c0", "p1", "c1"]
    assert links.build_chain_template("cse-isha", 0, "inter", "cse-lambda1")["spec"]["tasks"][1][
        "constraints"] == {"nodeNames": ["cse-lambda1"]}
