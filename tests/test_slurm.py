"""Slurm backend, offline: no sbatch/srun (the end-to-end test fakes them locally). SAGA via the shared path."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import ROOT

from saga_exec.backends.slurm import driver, saga_slurm
from saga_exec.backends.slurm.allocation import allocation_files
from saga_exec.testbed import load_testbed

RUNS = ROOT / "experiments" / "slurm" / "runs"


@pytest.mark.parametrize("path", sorted(RUNS.rglob("*.json")), ids=lambda p: str(p.relative_to(RUNS)))
def test_run_files_load(path):
    config = saga_slurm.load_run_file(str(path))
    assert saga_slurm.instance_path(config).exists()
    assert config.enact_order == "order"


def _write(tmp_path, **spec) -> str:
    p = tmp_path / "run.json"
    p.write_text(json.dumps({"instance": "graphs/smoke.json", **spec}))
    return str(p)


def test_strict_order_is_deprecated(tmp_path):
    with pytest.warns(DeprecationWarning, match="strict_order"):
        assert saga_slurm.load_run_file(_write(tmp_path, strict_order=False)).enact_order == "none"
    with pytest.warns(DeprecationWarning):
        assert saga_slurm.load_run_file(_write(tmp_path, strict_order=True)).enact_order == "order"


@pytest.mark.parametrize("spec", [{"time_scale": 3}, {"bandwidth": 1e9}, {"cores": 8}, {"enact_order": "serial"},
                                  {"tpyo": 1}, {"instance": None}, {"model": {"links": "x"}},
                                  {"model": {"transfer_latency": 0.01}}, {"model": {"typo": 1}}])
def test_bad_run_files_stop(tmp_path, spec):
    with pytest.raises(SystemExit):
        saga_slurm.load_run_file(_write(tmp_path, **spec))


def test_prepare_and_plan(tmp_path):
    run_file = _write(tmp_path, scheduler="heft", speed={"cse-isha": 0.5}, sec_per_cost=2, _why="comment")
    p = saga_slurm.prepare(run_file)
    assert p.prediction["meta"]["backend"] == "slurm"
    assert p.prediction["request"]["clusterState"]["nodes"][0]["cpuMillis"] == 14000
    plan = p.plan
    assert list(plan) == sorted(plan, key=lambda n: (plan[n]["start"], n))
    for name, t in plan.items():
        speed = 0.5 if t["machine"] == "cse-isha" else 1.0
        assert t["duration"] == pytest.approx(p.instance.tasks[name] * 2 / speed)
        assert t["end"] - t["start"] == pytest.approx(t["duration"])      # bridge: exact runtimes
    assert plan["d"]["waits_for"] == ["b", "c"]
    # enact_order "order": each task starts after its predecessor on the machine
    for order in p.prediction["order"].values():
        assert [plan[t]["start_after"] for t in order] == [[]] + [[a] for a in order[:-1]]

    script_plan, script = allocation_files("rid", Path("/tmp/x"), plan, p.config, p.testbed)
    assert "#SBATCH --cpus-per-task=2" in script
    by_name = {t["name"]: t for t in script_plan["tasks"]}
    assert by_name["d"]["start_after"] == plan["d"]["start_after"]
    assert by_name["a"]["node"] in ("cse-lambda1", "cse-isha")


def test_transfer_seconds_follow_the_bridge(tmp_path):
    """SAGA's assumed transfer = bytes / its link speed: min of both directions across machines."""
    p = saga_slurm.prepare(str(RUNS / "transfer_test.json"))
    tb = load_testbed()
    for t in p.plan.values():
        for parent, seconds in t["predicted_transfer"].items():
            if p.plan[parent]["machine"] != t["machine"]:
                assert seconds == pytest.approx(3.0)        # 3 units * 117e6 B / 117e6 B/s
            else:
                assert seconds < 1e-3
    assert saga_slurm.link_speed(tb, None, "cse-isha/c0", "cse-lambda1/c3") == 117_000_000
    model = {"selfBandwidth": {"cse-isha": 5.0}, "intraBandwidth": 7.0}
    assert saga_slurm.link_speed(tb, model, "cse-isha/c1", "cse-isha/c1") == 5.0       # same core
    assert saga_slurm.link_speed(tb, model, "cse-isha/c1", "cse-isha/c2") == 7.0       # same machine
    assert saga_slurm.link_speed(tb, model, "cse-lambda1/c1", "cse-lambda1/c1") == 1e12  # unset -> free


def test_measured_links_reach_the_plan(tmp_path):
    p = saga_slurm.prepare(_write(tmp_path, model={"links": "measured", "cores": 2}))
    assert p.prediction["request"]["model"]["intraBandwidth"] == {"cse-isha": 1e9, "cse-lambda1": 1e9}
    sizes = {(s, d): z for s, d, z in p.instance.edges}
    for name, t in p.plan.items():
        assert t["core"].rpartition("/c")[0] == t["machine"]
        for parent, seconds in t["predicted_transfer"].items():
            if p.plan[parent]["machine"] == t["machine"]:       # self or intra: both 1e9 placeholders
                assert seconds == pytest.approx(sizes[(parent, name)] / 1e9)


def test_no_enacted_order(tmp_path):
    p = saga_slurm.prepare(_write(tmp_path, enact_order="none"))
    assert all(t["start_after"] == [] for t in p.plan.values())


def test_driver_start_after():
    t = {"start_after": ["a"]}
    assert not driver.started_after(t, {"a", "b"})
    assert driver.started_after(t, {"b"})
    assert driver.started_after({}, {"a"})                  # plans without start_after (old plan.json)


# ---------------------------------------------------------------------------
# Real work and real transfers: plan, commands, dry run, driver end to end (fake srun)
# ---------------------------------------------------------------------------

def test_plan_inputs_outputs_and_env(tmp_path):
    p = saga_slurm.prepare(_write(tmp_path, bytes_per_size=0.001, sec_per_cost=0.5, mode="cpu", hash_rate=1e6))
    plan = p.plan
    assert p.config.transport == "direct"                                  # default: needs no shared FS
    for name, t in plan.items():
        assert t["seconds"] == pytest.approx(p.instance.tasks[name] * 0.5)
        for i in t["inputs"]:
            [out] = [o for o in plan[i["producer"]]["outputs"] if o["consumer"] == name]
            assert out["object"] == i["object"] == f"to-{name}" and out["bytes"] == i["bytes"]
            assert out["consumer_node"] == t["machine"]
    plan_json, script = allocation_files("rid", tmp_path, plan, p.config, p.testbed)
    assert plan_json["mode"] == "cpu" and plan_json["hash_rate"] == 1e6 and plan_json["transport"] == "direct"
    assert plan_json["pyz"] == str(tmp_path / "task.pyz")
    assert "#SBATCH --mem=" in script
    tasks = {t["name"]: t for t in plan_json["tasks"]}
    d = tasks["d"]
    servers = {m: {"host": f"10.0.0.{i}", "port": 4000 + i} for i, m in enumerate(plan_json["machines"])}
    env = driver.task_env(d, plan_json, tasks, servers, "tok")
    assert env["FID_TRANSPORT"] == "direct" and env["FID_TOKEN"] == "tok" and env["FID_NODE"] == d["node"]
    assert env["FID_INPUTS"] == "b.to-d,c.to-d" and env["FID_OUTPUTS"] == ""
    assert env["FID_INPUT_NODES"] == f"b.to-d={tasks['b']['node']},c.to-d={tasks['c']['node']}"
    assert env["FID_HASH_RATE"] == "1000000.0" and float(env["FID_SECONDS"]) == d["seconds"]
    assert set(env["FID_SERVERS"].split(",")) == {f"{m}={a['host']}:{a['port']}" for m, a in servers.items()}
    cmd = driver.step_command(d, plan_json)
    assert cmd[:2] == ["srun", "--nodes=1"] and f"--nodelist={d['node']}" in cmd
    assert cmd[-1].startswith("exec $HOME/.local/bin/python3.12 /tmp/saga-exec-") and cmd[-1].endswith(" task")
    scmd = driver.server_command(plan_json, "cse-isha")
    assert "--overlap" in scmd and f"--cpus-per-task={plan_json['cpus_per_node']}" in scmd
    assert " serve --run rid --node cse-isha " in scmd[-1] and "--cleanup" in scmd[-1]
    assert driver.sbcast_command(plan_json)[:2] == ["sbcast", "--force"]


def test_shared_fs_plan(tmp_path):
    p = saga_slurm.prepare(_write(tmp_path, transport="shared_fs"))
    assert p.config.workdir == "/srv/saga-exec"                            # machines.yaml default
    plan_json, script = allocation_files("rid", tmp_path, p.plan, p.config, p.testbed)
    assert plan_json["shared_dir"].startswith("/srv/saga-exec/") and plan_json["shared_dir"].endswith("/rid")
    tasks = {t["name"]: t for t in plan_json["tasks"]}
    env = driver.task_env(tasks["d"], plan_json, tasks)
    assert env["FID_TRANSPORT"] == "shared_fs" and env["FID_WORKDIR"] == plan_json["shared_dir"] + "/data"
    assert "FID_TOKEN" not in env and "FID_SERVERS" not in env
    assert driver.step_command(tasks["d"], plan_json)[-1].startswith(
        f"exec $HOME/.local/bin/python3.12 {plan_json['shared_dir']}/task.pyz")
    assert "test -w" in driver.visibility_check_command(plan_json)[-1]
    p = saga_slurm.prepare(_write(tmp_path, transport="shared_fs", workdir="/nfs/x"))
    assert allocation_files("rid", tmp_path, p.plan, p.config, p.testbed)[0]["shared_dir"].startswith("/nfs/x/")
    with pytest.raises(SystemExit):
        saga_slurm.load_run_file(_write(tmp_path, transport="nfs"))


def test_cpu_mode_needs_a_hash_rate(tmp_path):
    with pytest.raises(SystemExit, match="hash_rate"):
        saga_slurm.prepare(_write(tmp_path, mode="cpu"))
    with pytest.raises(SystemExit):
        saga_slurm.load_run_file(_write(tmp_path, mode="gpu"))


def test_bad_task_names_stop(tmp_path):
    inst = tmp_path / "g.json"
    inst.write_text(json.dumps({"name": "bad", "tasks": {"a/b": 1.0, "c": 1.0}, "edges": [["a/b", "c", 1]]}))
    with pytest.raises(ValueError, match="task names"):
        saga_slurm.prepare(_write(tmp_path, instance=str(inst)))


@pytest.mark.parametrize("transport", ["direct", "shared_fs"])
def test_dry_run_prints_the_steps(tmp_path, capsys, monkeypatch, transport):
    monkeypatch.setattr(sys, "argv", ["saga-slurm", _write(tmp_path, dry_run=True, transport=transport)])
    saga_slurm.main()
    out = capsys.readouterr().out
    assert "#SBATCH --job-name=saga-" in out and f"FID_TRANSPORT={transport}" in out
    if transport == "direct":
        assert "sbcast --force" in out and " serve --run " in out
    else:
        assert "check-workdir" in out and "sbcast" not in out
    assert "FID_TOKEN" not in out and "simulated" not in out


def test_driver_gates_on_completed_producers():
    task = {"inputs": [{"producer": "a", "object": "to-b"}, {"producer": "c", "object": "to-b"}]}
    results = {"a": {"state": "COMPLETED", "end": 5.0}}
    assert not driver.parents_completed(task, results) and driver.parents_end(task, results) is None
    results["c"] = {"state": "COMPLETED", "end": 7.0}
    assert driver.parents_completed(task, results) and driver.parents_end(task, results) == 7.0
    results["c"]["state"] = "FAILED"
    assert not driver.parents_completed(task, results)
    assert driver.parents_completed({"inputs": []}, {})                   # sources start at once


FAKE_SRUN = """#!{python}
# Test stand-in for srun: run the command here; "@NODE@" in args/env -> --nodelist
# (or the first node, for a multi-node step).
import os, subprocess, sys
args, i, nodes = sys.argv[1:], 0, ["?"]
while i < len(args) and args[i].startswith("--"):
    if args[i].startswith("--nodelist="):
        nodes = args[i].split("=", 1)[1].split(",")
    i += 1
for node in nodes:
    cmd = [a.replace("@NODE@", node) for a in args[i:]]
    env = {{k: v.replace("@NODE@", node) for k, v in os.environ.items()}}
    if len(nodes) == 1:
        os.execvpe(cmd[0], cmd, env)
    rc = subprocess.run(cmd, env=env).returncode
    if rc:
        sys.exit(rc)
"""

FAKE_SBCAST = """#!{python}
# Test stand-in for sbcast: copy to every fake node's path.
import os, shutil, sys
src, dst = sys.argv[-2], sys.argv[-1]
for node in os.environ["FAKE_NODES"].split(","):
    d = dst.replace("@NODE@", node)
    os.makedirs(os.path.dirname(d), exist_ok=True)
    shutil.copy(src, d)
"""


@pytest.mark.parametrize("transport", ["direct", "shared_fs"])
def test_driver_end_to_end_with_fake_srun(tmp_path, transport):
    """The whole in-job path on localhost with fake srun/sbcast: delivery of task.pyz,
    data servers (direct) or a temp "shared FS" (shared_fs), real tasks, a
    cross-machine edge, start order, results, cleanup."""
    import socket
    from types import SimpleNamespace

    from saga_exec.backends.slurm.allocation import build_task_pyz, measured_from_results

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

    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_NODES": f"{m1},{m2}"}
    proc = subprocess.run([sys.executable, driver.__file__, str(run_dir / "plan.json")], env=env,
                          capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    res = json.loads((run_dir / "results.json").read_text())
    t = res["tasks"]
    assert res["transport"] == transport
    assert {n: r["state"] for n, r in t.items()} == dict.fromkeys("abcd", "COMPLETED")
    if transport == "direct":
        served = {f["object"]: f for f in res["transfers"]}             # a -> b and b -> d cross machines
        assert set(served) == {"a.to-b", "b.to-d"}
        assert served["a.to-b"]["src_node"] == m1 and served["a.to-b"]["bytes"] == 2_000_000
        assert t["b"]["timings"]["bytesPulled"] == 2_000_000 and t["c"]["timings"]["bytesPulled"] == 0
        for m in (m1, m2):                                               # servers cleaned up
            assert not (tmp_path / m / "scratch" / "r1").exists()
            assert not (tmp_path / m / "scratch-r1.pyz").exists()
    else:
        assert res["transfers"] == [] and not Path(plan_json["shared_dir"]).exists()
    assert t["b"]["start"] >= t["a"]["end"] and t["b"]["inputs_ready"] == t["a"]["end"]
    assert t["d"]["start"] >= max(t["b"]["end"], t["c"]["end"])
    assert t["c"]["launch"] >= t["a"]["launch"]                          # enacted order on m1
    assert t["b"]["timings"]["bytesIn"] == 2_000_000 and t["a"]["timings"]["bytesOut"] == 2_001_000
    assert t["c"]["timings"]["computeSeconds"] >= 0.1
    rows, actual = saga_slurm.summarize("r1", plan, measured_from_results(t, plan, "1"))
    assert actual["tasks"]["b"]["delay"] >= 0 and actual["tasks"]["a"]["delay"] is None
    assert {"input_read", "compute", "handoff", "inputs_ready"} <= set(actual["tasks"]["d"])
    assert actual["makespan"] == max(r["end"] for r in rows)
