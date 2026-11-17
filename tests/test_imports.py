"""Every module imports without a cluster (no kubectl / sbatch needed at import time)."""

import importlib

import pytest

MODULES = [
    "saga_exec",
    "saga_exec.instance",
    "saga_exec.network",
    "saga_exec.schedule",
    "saga_exec.compare",
    "saga_exec.calibrate",
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
]


@pytest.mark.parametrize("name", MODULES)
def test_import(name):
    importlib.import_module(name)
