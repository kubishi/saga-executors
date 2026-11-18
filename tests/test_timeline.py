"""Measurement-only instrumentation (docs/formats.md, actual.json v2): the task core's
exact timeline, per-input / per-output / per-edge records on every backend and
transport, clock records, and the SAGA-vs-actual table. Offline.

The first test is the guard for the PI's constraint: the instrumented task program
performs exactly the same operations, in the same order, as before."""

import importlib.util
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import ROOT

sys.path.insert(0, str(ROOT / "task"))
import fidelity_core  # noqa: E402
from transport_direct import DataServer  # noqa: E402

import task as task_main  # noqa: E402
from saga_exec.actual import FORMAT, UNOBSERVABLE, build_actual, edge_kind  # noqa: E402
from saga_exec.backends.slurm import driver, saga_slurm  # noqa: E402
from saga_exec.backends.slurm.allocation import allocation_files, build_task_pyz, measured_from_results  # noqa: E402
from saga_exec.backends.wayline import analyze  # noqa: E402
from saga_exec.compare import saga_link, side_by_side  # noqa: E402
from saga_exec.testbed import load_testbed  # noqa: E402

BEFORE = ROOT / "tests" / "data" / "fidelity_core_before.py"


def env(**kw):
    base = {"FID_MODE": "sleep", "FID_SECONDS": "0.05", "FID_SPEED": "m1=1.0,m2=0.5"}
    return {**base, **{k: str(v) for k, v in kw.items()}}


class Recorder:
    """A transport that records every call the task program makes, in order."""

    def __init__(self, events, objects, node="m1", name="t", record=True):
        self.events, self.objects, self.node, self.name, self.record = events, objects, node, name, record

    def recv(self, peer):
        self.events.append(("recv", peer))
        return self.objects[peer]

    def send(self, output, data):
        self.events.append(("send", output, len(data), bytes(data[:8])))

    def close(self):
        self.events.append(("close",))
        return {"closeUnix": time.time()} if self.record else None


def _load_before():
    spec = importlib.util.spec_from_file_location("fidelity_core_before", BEFORE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("record", [True, False], ids=["slurm-like", "wayline-like"])
@pytest.mark.parametrize("outputs", ["to-c:2000,to-d:0,to-e:17", ""], ids=["outputs", "leaf"])
def test_task_program_does_the_same_operations_in_the_same_order(monkeypatch, record, outputs):
    """Old core (tests/data/fidelity_core_before.py, frozen) vs the instrumented one:
    the same transport calls, payload generations and work, in the same order, with
    the same arguments."""
    seqs = []
    for mod in (_load_before(), fidelity_core):
        events = []
        counter = iter(range(1000))
        monkeypatch.setattr(mod, "payload", lambda n, ev=events, c=counter: (
            ev.append(("payload", n)), bytes([next(c) % 256]) * n)[1])
        monkeypatch.setattr(mod, "do_work", lambda *a, ev=events: ev.append(("do_work", *a)))
        t = Recorder(events, {"a.to-t": b"x" * 10, "b.to-t": b"y" * 3}, node="m2", record=record)
        mod.run(t, env(FID_INPUTS="a.to-t,b.to-t", FID_OUTPUTS=outputs, FID_MODE="cpu", FID_HASH_RATE="10"))
        seqs.append(events)
    assert seqs[0] == seqs[1]
    assert [e[0] for e in seqs[1]][:3] == ["recv", "recv", "do_work"] and seqs[1][-1] == ("close",)


class FakeTransport:
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
        self.sent.append(output)
        self.clock.send_end(len(data))

    def close(self):
        return self.clock.record()


def test_timeline_order_and_payload_generation_outside_work(monkeypatch):
    real_payload = fidelity_core.payload

    def slow_payload(n):                       # stands in for 351 MB of random bytes
        time.sleep(0.1)
        return real_payload(n)
    monkeypatch.setattr(fidelity_core, "payload", slow_payload)
    t = FakeTransport({"a.to-t": b"x" * 1000, "b.to-t": b"y" * 10}, node="m2")
    rec = fidelity_core.run(t, env(FID_INPUTS="a.to-t,b.to-t", FID_OUTPUTS="to-c:2000,to-d:5"))
    tl = rec["timeline"]
    ins, outs = tl["inputs"], tl["outputs"]
    # per input: producer, object, bytes, absolute read times, in read order
    assert [(i["producer"], i["object"], i["bytes"]) for i in ins] == [("a", "to-t", 1000), ("b", "to-t", 10)]
    stamps = [tl["runStartUnix"]]
    for i in ins:
        stamps += [i["readStartUnix"], i["readEndUnix"]]
    stamps += [tl["workStartUnix"], tl["workEndUnix"]]
    for o in outs:
        stamps += [o["generateStartUnix"], o["generateEndUnix"], o["sendStartUnix"], o["sendEndUnix"]]
    stamps += [tl["closeStartUnix"], tl["closeEndUnix"]]
    assert stamps == sorted(stamps)
    assert rec["taskStartUnix"] <= tl["runStartUnix"] and rec["closeUnix"] <= tl["closeEndUnix"]
    assert [(o["object"], o["bytes"]) for o in outs] == [("to-c", 2000), ("to-d", 5)]
    # work is the sleep alone (0.05 / speed 0.5 = 0.1 s); generating the first output is not in it ...
    assert 0.09 < tl["workSeconds"] < 0.18
    assert all(o["generateEndUnix"] - o["generateStartUnix"] >= 0.09 for o in outs)
    # ... while the legacy computeSeconds (last read -> first send) still includes it
    assert rec["computeSeconds"] >= tl["workSeconds"] + 0.09


def test_timeline_on_a_leaf_and_wayline_style_close(capsys):
    t = FakeTransport()
    rec = fidelity_core.run(t, env(FID_SECONDS="0"))
    assert rec["timeline"]["outputs"][0]["object"] is None and rec["timeline"]["inputs"] == []

    class Wl(FakeTransport):
        def close(self):
            return None                          # Wayline: the SDK reports its timings itself
    assert fidelity_core.run(Wl({"a.to-b": b"zz"}, name="b"), env(FID_INPUTS="a.to-b", FID_OUTPUTS="to-c:3")) is None
    line = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("FID_TIMELINE ")]
    assert len(line) == 1
    rec = json.loads(line[0][len("FID_TIMELINE "):])
    assert rec["task"] == "b" and rec["timeline"]["inputs"][0]["producer"] == "a"
    assert analyze.timelines_from_logs(f"[pod/x/b] {line[0]}\nother\n")["b"]["timeline"] == rec["timeline"]


# ---------------------------------------------------------------------------
# Transports' own details
# ---------------------------------------------------------------------------

def test_direct_records_source_and_server_side_times(tmp_path):
    servers = {}
    for m in ("m1", "m2"):
        srv = DataServer("run1", m, tmp_path / m, "tok")
        threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
        servers[m] = srv
    try:
        e = env(FID_TRANSPORT="direct", FID_RUN="run1", FID_TOKEN="tok",
                FID_SERVERS=",".join(f"{m}=127.0.0.1:{s.server_address[1]}" for m, s in servers.items()))
        pe = {**e, "FID_TASK": "p", "FID_NODE": "m1", "FID_SCRATCH": str(tmp_path / "m1"),
              "FID_OUTPUTS": "to-c:2000000,to-d:10"}
        fidelity_core.run(task_main.make_transport(pe), pe)
        ce = {**e, "FID_TASK": "c", "FID_NODE": "m2", "FID_SCRATCH": str(tmp_path / "m2"),
              "FID_INPUTS": "p.to-c", "FID_INPUT_NODES": "p.to-c=m1"}
        rec = fidelity_core.run(task_main.make_transport(ce), ce)
        [i] = rec["timeline"]["inputs"]
        assert i["sourceNode"] == "m1" and i["via"] == "pull" and i["bytes"] == 2_000_000
        [served] = servers["m1"].status()["served"]
        assert served["server_node"] == "m1" and served["object"] == "p.to-c"
        assert i["readStartUnix"] <= served["received"] <= served["start"] <= served["body_start"] \
            <= served["end"] <= i["readEndUnix"]
    finally:
        for srv in servers.values():
            srv.shutdown()
            srv.server_close()


def test_shared_fs_records_write_and_read_times(tmp_path):
    base = env(FID_TRANSPORT="shared_fs", FID_WORKDIR=str(tmp_path), FID_LATENCY_WAIT="2")
    ce = {**base, "FID_TASK": "c", "FID_NODE": "m2", "FID_INPUTS": "p.to-c"}
    pe = {**base, "FID_TASK": "p", "FID_NODE": "m1", "FID_OUTPUTS": "to-c:300000"}
    threading.Timer(0.2, lambda: fidelity_core.run(task_main.make_transport(pe), pe)).start()
    rec = fidelity_core.run(task_main.make_transport(ce), ce)       # waits for the late file
    [i] = rec["timeline"]["inputs"]
    assert i["attempts"] > 1 and i["latencyWaited"] > 0 and i["readStartUnix"] < i["openedUnix"] <= i["readEndUnix"]


# ---------------------------------------------------------------------------
# Slurm end to end on localhost (fake srun / sbcast), both transports
# ---------------------------------------------------------------------------

def _run_fake_job(tmp_path, transport):
    from test_slurm import FAKE_SBCAST, FAKE_SRUN
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, text in (("srun", FAKE_SRUN), ("sbcast", FAKE_SBCAST)):
        (bin_dir / name).write_text(text.format(python=sys.executable))
        (bin_dir / name).chmod(0o755)
    m1, m2 = "cse-lambda1", "cse-isha"
    plan = saga_slurm.manual_plan(
        {"a": (m1, 0.05), "b": (m2, 0.05), "c": (m1, 0.1), "d": (m1, 0.05)},
        [("a", "b", 2_000_000), ("a", "c", 1000), ("b", "d", 500_000), ("c", "d", 0)],
        enact_order={m1: ["a", "c", "d"]})
    config = SimpleNamespace(cpus=1, mem="1G", mode="sleep", hash_rate=None, machine_speed={m1: 1.0, m2: 1.0},
                             task_python=sys.executable, transport=transport, workdir=str(tmp_path / "nfs"),
                             latency_wait=1.0, keep_data=False, scratch=str(tmp_path / "@NODE@" / "scratch"),
                             server_port=0, server_bind={m1: "127.0.0.1", m2: "127.0.0.1"})
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    plan_json, _ = allocation_files("r1", run_dir, plan, config, load_testbed())
    plan_json["driver_node"] = socket.gethostname()
    build_task_pyz(Path(plan_json["pyz"]))
    (run_dir / "plan.json").write_text(json.dumps(plan_json))
    e = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_NODES": f"{m1},{m2}"}
    proc = subprocess.run([sys.executable, driver.__file__, str(run_dir / "plan.json")], env=e,
                          capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    res = json.loads((run_dir / "results.json").read_text())
    rows, actual = saga_slurm.summarize("r1", plan, measured_from_results(res["tasks"], plan, "1"),
                                        transfers=res["transfers"], clocks=res["clocks"], transport=transport)
    return plan, res, rows, actual


@pytest.mark.parametrize("transport", ["direct", "shared_fs"])
def test_slurm_actual_v2_end_to_end(tmp_path, transport):
    plan, res, rows, actual = _run_fake_job(tmp_path, transport)
    assert actual["format"] == FORMAT and actual["transport"] == transport
    assert actual["tasks"][actual["t0_task"]]["start"] == 0.0
    assert set(actual["edges"]) == {"a->b", "a->c", "b->d", "c->d"}
    for t in actual["tasks"].values():
        tm = t["times"]
        assert t["precise"] and 0 <= tm["task_start"] <= tm["work_start"] < tm["work_end"] <= tm["task_end"]
        assert tm["inputs_read_end"] is None or tm["inputs_read_end"] <= tm["work_start"]
        assert t["durations"]["work"] >= 0.05 * 0.9
    assert actual["tasks"]["c"]["durations"]["work"] >= 0.1 * 0.9
    ab = actual["edges"]["a->b"]
    assert ab["kind"] == "inter" and ab["bytes"] == 2_000_000 and ab["src_machine"] == "cse-lambda1"
    ac = actual["edges"]["a->c"]
    assert ac["kind"] in ("same-core", "intra")
    for e in actual["edges"].values():
        t = e["times"]
        assert t["producer_work_end"] <= t["producer_output_generated"] <= t["producer_output_written"] \
            <= t["consumer_read_start"] <= t["consumer_read_end"]
        assert e["available"]["source"] and t["consumer_input_available"] is not None
        d = e["decomposition"]
        parts = [d["producer_write"], d["wait_launch"], d["consumer_read"]]
        if d["network"] != UNOBSERVABLE:
            parts.append(d["network"])
        assert sum(parts) == pytest.approx(e["data_movement"], abs=1e-6)
    if transport == "direct":
        # server-side pull times, on the producer machine's clock, inside the consumer's read
        t = ab["times"]
        assert ab["transfer"]["clock"] == "cse-lambda1" and ab["transfer"]["within_consumer_read"]
        assert t["consumer_read_start"] <= ab["transfer"]["server_received"] <= t["transfer_start"] \
            <= t["transfer_end"] <= t["consumer_read_end"]
        assert ab["decomposition"]["network"] > 0
        assert ac["decomposition"]["network"] == 0.0                  # same machine: local file
        assert set(res["clocks"]["server_vs_driver"]) == {"cse-lambda1", "cse-isha"}
        assert abs(res["clocks"]["server_vs_driver"]["cse-isha"]["offset"]) < 1.0
    else:
        assert ab["decomposition"]["network"] == UNOBSERVABLE
        out = actual["tasks"]["a"]["outputs"][0]
        assert out["send_start"] <= out["written_rel"] <= out["renamed_rel"] <= out["send_end"]
    # clock probes at job start and end (best-effort; fake srun runs them here)
    host = socket.gethostname()
    assert res["clocks"]["start"]["hosts"][host]["unix"] and res["clocks"]["end"]["hosts"][host]["unix"]
    assert actual["clocks"]["end"]["hosts"][host]["source"] in ("chronyc", "timedatectl", "none")
    assert (tmp_path / "run" / "clock-end.log").exists()
    # unchanged v1 fields
    assert {"start", "end", "input_read", "compute", "handoff", "inputs_ready", "delay"} <= set(actual["tasks"]["d"])
    assert actual["makespan"] == max(r["end"] for r in rows)


def test_clock_probe_parser():
    text = ("FIDCLK n1 unix 100.5\n"
            "FIDCLK n1 chrony A9FEA97B,169.254.169.123,4,1700000000.1,-0.000001,0.000002,0.00001,-12.3,0.001,"
            "0.05,0.0003,0.0002,64.2,Normal\n"
            "FIDCLK n2 timesync        Offset: +1.2ms\n"
            "FIDCLK n1 unix 100.6\nnoise\n")
    out = driver.parse_clock_probe(text)
    assert out["n1"]["unix"] == [100.5, 100.6] and out["n1"]["source"] == "chronyc"
    assert out["n1"]["chrony"]["system_time_offset"] == -0.000001 and out["n1"]["chrony"]["leap_status"] == "Normal"
    assert out["n2"]["source"] == "timedatectl" and "Offset" in out["n2"]["raw"][0]
    assert out["n2"]["timesync_offset"] == pytest.approx(0.0012)
    plan = {"machines": ["a", "b"]}
    assert driver.clock_probe_command(plan)[:2] == ["srun", "--overlap"]


# ---------------------------------------------------------------------------
# Wayline: analyze a synthetic status.json (+ logs with FID_TIMELINE lines)
# ---------------------------------------------------------------------------

T = 1_800_000_000.0


def _iso(x):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(x, timezone.utc).isoformat().replace("+00:00", "Z")


def _timeline(task, node, start, inputs, work, outputs):
    """A task core FID_TIMELINE record: inputs [(peer, bytes, read_start, read_end)],
    work (start, end), outputs [(object, bytes, gen_start, gen_end, send_end)]."""
    return {"task": task, "node": node, "host": f"pod-{task}", "timeline": {
        "runStartUnix": start,
        "inputs": [{"peer": p, "producer": p.split(".")[0], "object": p.split(".")[1], "bytes": b,
                    "readStartUnix": s, "readEndUnix": e} for p, b, s, e in inputs],
        "workStartUnix": work[0], "workEndUnix": work[1], "workSeconds": work[1] - work[0],
        "outputs": [{"object": o, "bytes": b, "generateStartUnix": gs, "generateEndUnix": ge,
                     "sendStartUnix": ge, "sendEndUnix": se} for o, b, gs, ge, se in outputs],
        "closeStartUnix": None, "closeEndUnix": None}}


def synthetic_wayline():
    """a (isha) -> b (isha), a -> c (lambda1, pushed), b, c -> d (lambda1; b->d pushed)."""
    I, L = "cse-isha", "cse-lambda1"
    spec = [
        {"name": "a", "dependencies": [], "outputs": [{"name": "to-b", "dataSize": "1000"},
                                                      {"name": "to-c", "dataSize": "2000000"}]},
        {"name": "b", "dependencies": ["a"], "inputs": [{"producer": "a", "object": "to-b"}],
         "outputs": [{"name": "to-d", "dataSize": "500"}]},
        {"name": "c", "dependencies": ["a"], "inputs": [{"producer": "a", "object": "to-c"}],
         "outputs": [{"name": "to-d", "dataSize": "700"}]},
        {"name": "d", "dependencies": ["b", "c"], "inputs": [{"producer": "b", "object": "to-d"},
                                                             {"producer": "c", "object": "to-d"}]},
    ]
    times = {"a": (T + 0.0, T + 2.5, I), "b": (T + 3.0, T + 5.0, I), "c": (T + 4.0, T + 6.0, L),
             "d": (T + 7.0, T + 8.0, L)}

    def task(n, **kw):
        s, e, node = times[n]
        return {"name": n, "node": node, "phase": "Succeeded", "startTime": _iso(int(s)),
                "taskStartTime": _iso(s), "taskCloseTime": _iso(e), "computeSeconds": 1.0,
                "inputReadSeconds": 0.0, "handoffSeconds": 0.1, **kw}
    status = {
        "metadata": {"name": "synth-run-x"},
        "spec": {"tasks": spec},
        "status": {
            "phase": "Succeeded", "startTime": _iso(T - 2),
            "tasks": [
                task("a"),
                task("b", inputs=[{"object": "a.to-b", "producer": "a", "installedUnix": T + 2.2,
                                   "source": "local"}], inputsReadyTime=_iso(T + 2.2)),
                task("c", inputs=[{"object": "a.to-c", "producer": "a", "installedUnix": T + 3.5,
                                   "receiveStartUnix": T + 2.6, "source": "remote", "fromNode": I}],
                     inputsReadyTime=_iso(T + 3.5)),
                task("d", inputs=[{"object": "b.to-d", "producer": "b", "installedUnix": T + 5.6,
                                   "source": "remote", "fromNode": I},
                                  {"object": "c.to-d", "producer": "c", "installedUnix": T + 5.9,
                                   "source": "local"}], inputsReadyTime=_iso(T + 5.9)),
            ],
            "actualNetworkFlows": [
                {"fromTask": "a.to-c", "toTask": "c", "srcNode": I, "dstNode": L, "dataSize": 2000000,
                 "start": 2.6, "end": 3.4, "startUnix": T + 2.6, "endUnix": T + 3.4,
                 "requestUnix": T + 2.31, "hashedUnix": T + 2.4, "ok": True},
                # older controller: relative times only (origin: earliest whole-second pod startTime = T)
                {"fromTask": "b.to-d", "toTask": "d", "srcNode": I, "dstNode": L, "dataSize": 500,
                 "start": 5.2, "end": 5.5, "ok": True},
            ],
        },
    }
    logs = "\n".join("[pod/x/%s] FID_TIMELINE %s" % (r["task"], json.dumps(r)) for r in [
        _timeline("a", I, T + 0.01, [], (T + 0.02, T + 2.0), [("to-b", 1000, T + 2.0, T + 2.05, T + 2.2),
                                                               ("to-c", 2000000, T + 2.2, T + 2.25, T + 2.3)]),
        _timeline("b", I, T + 3.01, [("a.to-b", 1000, T + 3.05, T + 3.06)], (T + 3.06, T + 4.9),
                  [("to-d", 500, T + 4.9, T + 4.91, T + 4.95)]),
        _timeline("c", L, T + 4.01, [("a.to-c", 2000000, T + 4.05, T + 4.1)], (T + 4.1, T + 5.8),
                  [("to-d", 700, T + 5.8, T + 5.81, T + 5.9)]),
        _timeline("d", L, T + 7.01, [("b.to-d", 500, T + 7.02, T + 7.03), ("c.to-d", 700, T + 7.03, T + 7.04)],
                  (T + 7.05, T + 7.9), []),
    ])
    return status, logs


def test_wayline_actual_from_synthetic_status():
    status, logs = synthetic_wayline()
    a = analyze.actual_from_status(status, logs)
    assert a["format"] == FORMAT and a["backend"] == "wayline" and a["t0_unix"] == T and a["makespan"] == 8.0
    assert a["tasks"]["c"]["times"]["work_start"] == pytest.approx(4.1)
    assert a["tasks"]["c"]["durations"]["work"] == pytest.approx(1.7)
    assert a["makespan_work"] == pytest.approx(7.9)
    ac = a["edges"]["a->c"]
    assert ac["kind"] == "inter" and ac["bytes"] == 2_000_000 and ac["transfer"]["clock"] == "cse-isha"
    t = ac["times"]
    assert t["producer_output_written"] == pytest.approx(2.3) and t["transfer_start"] == pytest.approx(2.6)
    assert t["transfer_end"] == pytest.approx(3.4) and t["consumer_input_available"] == pytest.approx(3.5)
    assert ac["transfer"]["push_request"] == pytest.approx(2.31) and ac["transfer"]["receive_start"] == pytest.approx(2.6)
    d = ac["decomposition"]
    assert d["producer_write"] == pytest.approx(0.05) and d["network"] == pytest.approx(0.8)
    assert d["consumer_read"] == pytest.approx(0.05) and d["cross_clock"]
    assert d["producer_write"] + d["wait_launch"] + d["network"] + d["consumer_read"] == pytest.approx(ac["data_movement"])
    assert d["wait_launch_detail"]["send_end_to_transfer_start"] == pytest.approx(0.3)
    assert d["wait_launch_detail"]["available_to_consumer_start"] == pytest.approx(0.5)
    ab = a["edges"]["a->b"]
    assert ab["kind"] == "same-machine" and ab["decomposition"]["network"] == 0.0
    bd = a["edges"]["b->d"]                                   # relative-only flow: origin reconstructed
    assert bd["times"]["transfer_start"] == pytest.approx(5.2) and "origin_reconstructed" in bd["transfer"]
    assert a["tasks"]["d"]["inputs_ready"] == pytest.approx(5.9)
    # without logs (old images): still parses, edges marked as lacking timelines
    old = analyze.actual_from_status(status)
    assert old["tasks"]["c"]["times"]["work_start"] is None and "note" in old["edges"]["a->c"]


def test_existing_wayline_example_still_parses():
    ex = sorted((ROOT / "results" / "examples" / "wayline").glob("*/*/runs/*/status.json"))
    assert ex
    for p in ex:
        a = analyze.actual_from_status(json.loads(p.read_text()), (p.parent / "logs.txt").read_text())
        assert a["format"] == FORMAT and set(a["edges"]) == {"a->b", "a->c", "b->d", "c->d"}


# ---------------------------------------------------------------------------
# compare: SAGA vs actual side by side
# ---------------------------------------------------------------------------

def _prediction():
    I, L = "cse-isha", "cse-lambda1"
    request = {
        "model": {"selfBandwidth": {I: 1e12, L: 1e12}, "intraBandwidth": {I: 1e9, L: 1e9}},
        "dag": {"tasks": [
            {"name": "a", "dependencies": []},
            {"name": "b", "dependencies": ["a"], "inputs": [{"producer": "a", "object": "to-b", "bytes": 1000}]},
            {"name": "c", "dependencies": ["a"], "inputs": [{"producer": "a", "object": "to-c", "bytes": 2000000}]},
            {"name": "d", "dependencies": ["b", "c"], "inputs": [{"producer": "b", "object": "to-d", "bytes": 500},
                                                                 {"producer": "c", "object": "to-d", "bytes": 700}]}]},
        "clusterState": {"bandwidth": [{"from": I, "to": L, "bytesPerSec": 2e6},
                                       {"from": L, "to": I, "bytesPerSec": 1e6}]},
    }
    return {"makespan": 7.0, "request": request,
            "order": {I: ["a", "b"], L: ["c", "d"]},
            "tasks": {"a": {"node": f"{I}/c0", "machine": I, "start": 0.0, "end": 2.0},
                      "b": {"node": f"{I}/c0", "machine": I, "start": 2.0, "end": 4.0},
                      "c": {"node": f"{L}/c0", "machine": L, "start": 4.0, "end": 6.0},
                      "d": {"node": f"{L}/c1", "machine": L, "start": 6.0, "end": 7.0}}}


def test_saga_link_kinds():
    req = _prediction()["request"]
    assert saga_link(req, "cse-isha/c0", "cse-isha/c0") == ("same-core", 1e12)
    assert saga_link(req, "cse-isha/c0", "cse-isha/c3") == ("intra", 1e9)
    assert saga_link(req, "cse-isha/c0", "cse-lambda1/c1") == ("inter", 1e6)       # min of directions


def test_side_by_side():
    status, logs = synthetic_wayline()
    actual = analyze.actual_from_status(status, logs)
    side = side_by_side(_prediction(), actual)
    assert side["makespan"] == {"saga": 7.0, "actual_task": 8.0, "actual_work": pytest.approx(7.9),
                                "actual_first_work_start": pytest.approx(0.02),
                                "actual_task_minus_saga": 1.0, "actual_work_minus_saga": pytest.approx(0.9)}
    c = side["tasks"]["c"]
    assert c["saga"]["start"] == 4.0 and c["actual"]["work_start"] == pytest.approx(4.1)
    assert c["actual_minus_saga"]["work_start"] == pytest.approx(0.1)
    assert c["actual_minus_saga"]["task_start"] == pytest.approx(0.0) and c["same_machine"]
    ac = side["edges"]["a->c"]
    assert ac["saga"]["kind"] == "inter" and ac["saga"]["comm_time"] == pytest.approx(2.0)
    assert ac["actual"]["network"] == pytest.approx(0.8)
    assert ac["differences"]["network_minus_comm"] == pytest.approx(-1.2)
    assert side["edges"]["a->b"]["saga"]["kind"] == "same-core"
    assert side["order"]["actual_by_work_start"]["cse-lambda1"] == ["c", "d"]
    json.dumps(side)
    # a v1 actual.json (no work times / edges) still compares
    v1 = json.loads(next((ROOT / "results" / "examples" / "wayline").glob("*/heft-*/runs/*/actual.json")).read_text())
    old = side_by_side(_prediction(), v1)
    assert old["tasks"]["a"]["actual"]["work_start"] is None and old["makespan"]["actual_task"] == v1["makespan"]


def test_build_actual_kinds_and_unobservable():
    assert edge_kind("m1", "m2") == "inter" and edge_kind("m1", "m1", 3, 3) == "same-core"
    assert edge_kind("m1", "m1", 3, 4) == "intra" and edge_kind("m1", "m1") == "same-machine"
    tl = lambda ins, outs: {"runStartUnix": 0, "inputs": ins, "outputs": outs, "workStartUnix": 1, "workEndUnix": 2}
    tasks = {
        "p": {"machine": "m1", "task_start": 10.0, "task_end": 13.0, "timeline": {**tl([], [
            {"object": "to-c", "bytes": 5, "generateStartUnix": 11, "generateEndUnix": 11.5,
             "sendStartUnix": 11.5, "sendEndUnix": 12}]), "workStartUnix": 10.5, "workEndUnix": 11}},
        "c": {"machine": "m2", "task_start": 14.0, "task_end": 16.0, "timeline": {**tl([
            {"producer": "p", "object": "to-c", "bytes": 5, "readStartUnix": 14.5, "readEndUnix": 15}], []),
            "workStartUnix": 15, "workEndUnix": 15.5}},
    }
    a = build_actual(run="r", backend="slurm", transport="shared_fs", tasks=tasks)
    e = a["edges"]["p->c"]
    assert a["t0_unix"] == 10.0 and e["decomposition"]["network"] == UNOBSERVABLE
    assert e["decomposition"]["wait_launch"] == pytest.approx(2.5)           # read start - send end
    assert e["work_gap"] == pytest.approx(4.0) and e["data_movement"] == pytest.approx(3.5)


def test_slurm_dry_run_prints_the_clock_probe(tmp_path, capsys, monkeypatch):
    run = tmp_path / "run.json"
    run.write_text(json.dumps({"instance": "graphs/smoke.json", "dry_run": True}))
    monkeypatch.setattr(sys, "argv", ["saga-slurm", str(run)])
    monkeypatch.setattr(saga_slurm, "RUNS_DIR", tmp_path / "runs")
    saga_slurm.main()
    out = capsys.readouterr().out
    assert "clock-probe" in out and "chronyc" in out
    assert not (tmp_path / "runs").exists()

