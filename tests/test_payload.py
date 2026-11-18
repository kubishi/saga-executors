"""payload() must handle sizes whose bit count exceeds a C int (> ~268 MB)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "task"))
import fidelity_core  # noqa: E402


def test_small_payload_size():
    assert len(fidelity_core.payload(0)) == 0
    assert len(fidelity_core.payload(1234)) == 1234


def test_chunked_payload_size(monkeypatch):
    monkeypatch.setattr(fidelity_core, "PAYLOAD_CHUNK", 1000)
    data = fidelity_core.payload(2500)
    assert len(data) == 2500 and isinstance(data, bytes)


def test_payload_over_c_int_bits():
    n = 300_000_000                        # 2.4e9 bits > 2**31
    assert len(fidelity_core.payload(n)) == n


def test_link_transfer_seconds_uses_timeline_and_skips_failures():
    from saga_exec.calibrate.links import transfer_seconds
    ok = {"state": "COMPLETED"}
    results = {
        "p0": {**ok, "timings": {"timeline": {"outputs": [{"sendStartUnix": 10.0}]}}},
        "c0": {**ok, "timings": {"timeline": {"workStartUnix": 12.5,
                                              "inputs": [{"readEndUnix": 12.4}]}}},
        "p1": {"state": "FAILED", "timings": {}},
        "c1": {"state": "SKIPPED"},
    }
    assert transfer_seconds(results) == [12.4 - 10.0]


def test_payload_is_incompressible_and_fast():
    import time
    import zlib
    data = fidelity_core.payload(8_000_000)
    assert len(zlib.compress(data, 6)) > 0.99 * len(data)
    fidelity_core.payload(1)                       # block generated once per process
    t = time.perf_counter()
    big = fidelity_core.payload(500_000_000)
    assert len(big) == 500_000_000
    assert time.perf_counter() - t < 1.0           # memory speed, not ~350 MB/s generation
