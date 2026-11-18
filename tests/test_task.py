"""The shared task program (task/): core with a fake transport, the Wayline transport
against a fake SDK, and both Slurm transports end to end on localhost (shared_fs
with a temp dir as the "shared FS", direct with data servers). Offline."""

import hashlib
import json
import os
import subprocess
import sys
import threading
import types

import pytest
from conftest import ROOT

sys.path.insert(0, str(ROOT / "task"))
import fidelity_core  # noqa: E402
import transport_direct  # noqa: E402
from transport_direct import DataServer, request  # noqa: E402

import task as task_main  # noqa: E402
from saga_exec.backends.slurm.allocation import build_task_pyz  # noqa: E402


class FakeTransport:
    """In-memory transport: records calls; timings via the same PhaseClock."""

    def __init__(self, objects=None, node="m1", name="t"):
        self.objects = dict(objects or {})
        self.node, self.name = node, name
        self.clock = fidelity_core.PhaseClock()
        self.sent = []

    def recv(self, peer):
        self.clock.recv_begin()
        data = self.objects[peer]
        self.clock.recv_end(len(data))
        return data

    def send(self, output, data):
        self.clock.send_begin()
        self.sent.append((output, data))
        self.clock.send_end(len(data))

    def close(self):
        return self.clock.record()


def env(**kw):
    base = {"FID_MODE": "sleep", "FID_SECONDS": "0.05", "FID_SPEED": "m1=1.0,m2=0.5"}
    return {**base, **{k: str(v) for k, v in kw.items()}}


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def test_core_reads_works_writes_and_times():
    t = FakeTransport({"a.to-t": b"x" * 1000, "b.to-t": b"y" * 10}, node="m2")
    rec = fidelity_core.run(t, env(FID_INPUTS="a.to-t,b.to-t", FID_OUTPUTS="to-c:2000,to-d:0"))
    assert [(o, len(d)) for o, d in t.sent] == [("to-c", 2000), ("to-d", 0)]
    assert len(set(bytes(t.sent[0][1]))) > 200               # random, not zeros
    assert rec["bytesIn"] == 1010 and rec["bytesOut"] == 2000
    # sleep mode on m2 (speed 0.5): 0.05 / 0.5 = 0.1 s of compute
    assert 0.09 < rec["computeSeconds"] < 0.5
    assert rec["inputReadSeconds"] >= 0 and rec["handoffSeconds"] >= 0
    assert rec["taskStartUnix"] <= rec["computeStartUnix"] <= rec["computeEndUnix"] <= rec["closeUnix"]


def test_core_leaf_sends_done():
    t = FakeTransport()
    rec = fidelity_core.run(t, env(FID_SECONDS="0"))
    assert t.sent == [(None, b"done")]
    assert rec["inputReadSeconds"] == 0.0


def test_core_cpu_mode_hashes(monkeypatch):
    calls = []
    monkeypatch.setattr(fidelity_core, "hash_loop", lambda n: calls.append(n))
    fidelity_core.run(FakeTransport(), env(FID_MODE="cpu", FID_SECONDS="2", FID_HASH_RATE="1000"))
    assert calls == [2000]                                    # seconds * hash rate, speed not applied
    with pytest.raises(ValueError, match="FID_HASH_RATE"):
        fidelity_core.run(FakeTransport(), env(FID_MODE="cpu"))
    with pytest.raises(ValueError, match="FID_MODE"):
        fidelity_core.run(FakeTransport(), env(FID_MODE="gpu"))


def test_hash_loop_is_chained_sha256():
    d = fidelity_core.HASH_SEED
    for _ in range(3):
        d = hashlib.sha256(d).digest()
    assert fidelity_core.hash_loop(3) == d


def test_parsers():
    assert fidelity_core.parse_map("a=1, b=2,", "=") == {"a": "1", "b": "2"}
    assert fidelity_core.parse_outputs("to-c:10,to-d:0") == [("to-c", 10), ("to-d", 0)]
    assert fidelity_core.node_speed({"FID_SPEED": "m1=2"}, "m1") == 2.0
    assert fidelity_core.node_speed({}, "m1") == 1.0
    with pytest.raises(ValueError):
        fidelity_core.node_speed({"FID_SPEED": "m1=0"}, "m1")


def test_phase_clock_matches_the_sdk_definitions():
    c = fidelity_core.PhaseClock()
    c.task_start = 100.0
    c.recv_first, c.recv_last = 101.0, 103.0
    c.send_start, c.send_installed = 110.0, 112.5
    rec = c.record(now=113.0)
    assert rec["inputReadSeconds"] == 2.0 and rec["computeSeconds"] == 7.0 and rec["handoffSeconds"] == 2.5
    assert rec["closeUnix"] == 113.0
    c2 = fidelity_core.PhaseClock()
    c2.task_start = 5.0
    assert c2.record(now=9.0)["computeSeconds"] == 4.0        # no inputs/outputs: start -> close


def test_wayline_transport_uses_the_sdk(monkeypatch):
    calls = []

    class WlTask:
        name, node = "b", "cse-isha"

        def recv_raw(self, peer=None):
            calls.append(("recv", peer))
            return b"abc"

        def send_raw(self, *args):
            calls.append(("send", args[0] if len(args) == 2 else None, len(args[-1])))

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(sys.modules, "wl", types.SimpleNamespace(WlTask=WlTask))
    sys.modules.pop("transport_wayline", None)
    t = task_main.make_transport({})                          # FID_TRANSPORT defaults to wayline
    assert fidelity_core.run(t, env(FID_INPUTS="a.to-b", FID_OUTPUTS="to-c:5")) is None
    assert calls == [("recv", "a.to-b"), ("send", "to-c", 5), ("close",)]
    calls.clear()
    fidelity_core.run(t, env())
    assert calls == [("send", None, 4), ("close",)]           # leaf: default output b"done"


# ---------------------------------------------------------------------------
# Slurm: shared_fs (a temp dir stands in for the NFS work dir)
# ---------------------------------------------------------------------------

def shared_env(workdir, node, task, **kw):
    return env(**{"FID_TRANSPORT": "shared_fs", "FID_WORKDIR": str(workdir), "FID_TASK": task, "FID_NODE": node,
                  "FID_LATENCY_WAIT": "0.3", **kw})


def test_shared_fs_end_to_end(tmp_path):
    e = shared_env(tmp_path, "m1", "p", FID_OUTPUTS="to-c:300000,to-d:10")
    rec = fidelity_core.run(task_main.make_transport(e), e)
    assert rec["bytesOut"] == 300_010 and rec["handoffSeconds"] > 0 and rec["host"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["p.to-c", "p.to-d"]      # no temp files left
    produced = (tmp_path / "p.to-c").read_bytes()
    e = shared_env(tmp_path, "m2", "c", FID_INPUTS="p.to-c")
    rec = fidelity_core.run(task_main.make_transport(e), e)
    assert rec["bytesIn"] == len(produced) == 300_000 and rec["latency_waited"] == 0
    assert (tmp_path / "c").read_bytes() == b"done"                              # leaf marker


def test_shared_fs_latency_wait(tmp_path):
    """An input that appears late (NFS attribute caching) is waited for, then read."""
    e = shared_env(tmp_path, "m2", "c", FID_INPUTS="p.to-c", FID_LATENCY_WAIT="2")
    threading.Timer(0.2, lambda: (tmp_path / "p.to-c").write_bytes(b"x" * 7)).start()
    rec = fidelity_core.run(task_main.make_transport(e), e)
    assert rec["bytesIn"] == 7 and rec["latency_waited"] > 0 and rec["inputReadSeconds"] >= 0.15
    e = shared_env(tmp_path, "m2", "c2", FID_INPUTS="p.missing")
    with pytest.raises(FileNotFoundError):
        fidelity_core.run(task_main.make_transport(e), e)


# ---------------------------------------------------------------------------
# Slurm: direct (node-local scratch + data servers on localhost)
# ---------------------------------------------------------------------------

TOKEN = "t0k"


@pytest.fixture
def servers(tmp_path):
    """Two "machines" m1, m2: a data server each, separate scratch roots, on 127.0.0.1."""
    out = {}
    for m in ("m1", "m2"):
        srv = DataServer("run1", m, tmp_path / m, TOKEN)
        threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
        out[m] = srv
    yield out
    for srv in out.values():
        srv.shutdown()
        srv.server_close()


def direct_env(servers, tmp_path, node, task, **kw):
    return env(FID_TRANSPORT="direct", FID_RUN="run1", FID_TASK=task, FID_NODE=node,
               FID_SCRATCH=str(tmp_path / node), FID_TOKEN=TOKEN,
               FID_SERVERS=",".join(f"{m}={s.server_address[0]}:{s.server_address[1]}" for m, s in servers.items()),
               **kw)


def test_direct_end_to_end(servers, tmp_path):
    # producer p on m1 writes locally; nothing moves yet
    e = direct_env(servers, tmp_path, "m1", "p", FID_OUTPUTS="to-c:3000000,to-d:1000")
    rec = fidelity_core.run(task_main.make_transport(e), e)
    assert rec["bytesOut"] == 3_001_000 and rec["handoffSeconds"] > 0
    produced = (tmp_path / "m1" / "run1" / "p.to-c").read_bytes()
    assert not (tmp_path / "m2" / "run1" / "p.to-c").exists()
    # c on m2 pulls from m1's server (in input_read); d on m1 reads the local file
    e = direct_env(servers, tmp_path, "m2", "c", FID_INPUTS="p.to-c", FID_INPUT_NODES="p.to-c=m1")
    rec = fidelity_core.run(task_main.make_transport(e), e)
    assert rec["bytesIn"] == rec["bytesPulled"] == len(produced) == 3_000_000 and rec["inputReadSeconds"] > 0
    [served] = servers["m1"].status()["served"]
    assert served["object"] == "p.to-c" and served["bytes"] == 3_000_000 and served["end"] >= served["start"]
    e = direct_env(servers, tmp_path, "m1", "d", FID_INPUTS="p.to-d", FID_INPUT_NODES="p.to-d=m1")
    rec = fidelity_core.run(task_main.make_transport(e), e)
    assert rec["bytesIn"] == 1000 and rec["bytesPulled"] == 0
    assert len(servers["m1"].status()["served"]) == 1


def test_direct_pull_gets_the_same_bytes(servers, tmp_path):
    blob = os.urandom(1_234_567)
    (tmp_path / "m1" / "run1" / "a.to-b").write_bytes(blob)
    t = task_main.make_transport(direct_env(servers, tmp_path, "m2", "b", FID_INPUT_NODES="a.to-b=m1"))
    assert hashlib.sha256(t.recv("a.to-b")).digest() == hashlib.sha256(blob).digest()


def test_server_concurrent_pulls(servers, tmp_path):
    root = tmp_path / "m1" / "run1"
    blobs = {f"t{i}.o": os.urandom(200_000 + i) for i in range(12)}
    for k, v in blobs.items():
        (root / k).write_bytes(v)
    addr, got = servers["m1"].server_address, {}

    def pull(k):
        got[k] = request(addr, "GET", f"/obj/run1/{k}", TOKEN)
    threads = [threading.Thread(target=pull, args=(k,)) for k in blobs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert {k: v for k, (status, v) in got.items() if status == 200} == blobs


def test_server_rejects_bad_requests(servers, tmp_path):
    addr = servers["m1"].server_address
    (tmp_path / "m1" / "run1" / "a.x").write_bytes(b"abc")
    assert request(addr, "GET", "/obj/run1/a.x", "wrong")[0] == 403
    assert request(addr, "GET", "/obj/other-run/a.x", TOKEN)[0] == 404
    assert request(addr, "GET", "/obj/run1/..evil", TOKEN)[0] == 400
    assert request(addr, "GET", "/obj/run1/nope", TOKEN)[0] == 404
    assert request(addr, "GET", "/obj/run1/a.x", TOKEN) == (200, b"abc")


def test_server_subprocess_from_pyz(tmp_path):
    """The data server as the driver starts it: task.pyz serve, hello line, a real
    task pulling from it, shutdown cleans up."""
    pyz = build_task_pyz(tmp_path / "task.pyz")
    extra = tmp_path / "copy.pyz"
    extra.write_bytes(pyz.read_bytes())
    proc = subprocess.Popen([sys.executable, str(pyz), "serve", "--run", "r9", "--node", "m1",
                             "--scratch", str(tmp_path / "m1"), "--cleanup", str(extra)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            env={**os.environ, "FID_TOKEN": TOKEN})
    try:
        hello = json.loads(proc.stdout.readline())
        assert hello["fid_server"] == "m1" and hello["host"] == "127.0.0.1" and hello["port"] > 0
        root = tmp_path / "m1" / "r9"
        assert root.is_dir()
        (root / "p.to-c").write_bytes(b"z" * 5000)
        task_env = {**os.environ, **env(FID_TRANSPORT="direct", FID_RUN="r9", FID_TASK="c", FID_NODE="m2",
                                        FID_SCRATCH=str(tmp_path / "m2"), FID_TOKEN=TOKEN,
                                        FID_SERVERS=f"m1={hello['host']}:{hello['port']}",
                                        FID_INPUTS="p.to-c", FID_INPUT_NODES="p.to-c=m1")}
        out = subprocess.run([sys.executable, str(pyz)], env=task_env, capture_output=True, text=True, timeout=60)
        assert out.returncode == 0, out.stderr
        rec = json.loads(out.stdout.split("FID_RESULT ", 1)[1])
        assert rec["bytesPulled"] == 5000
        addr = (hello["host"], hello["port"])
        assert request(addr, "POST", "/shutdown/r9", TOKEN, body=b"")[0] == 200
        assert proc.wait(timeout=10) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
    assert not root.exists() and not extra.exists()


def test_bind_address():
    assert transport_direct.bind_address("m1", []) == "127.0.0.1"
    assert transport_direct.bind_address("m1", ["m1"]) == "127.0.0.1"
    with pytest.raises(RuntimeError, match="--bind"):
        transport_direct.bind_address("m1", ["no-such-host.invalid"])
