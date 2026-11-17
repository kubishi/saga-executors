#!/usr/bin/env python3
"""Run a SAGA schedule inside one Slurm job, with real work and real data transfers.

allocation.py writes plan.json, task.pyz and job.sbatch into the run folder and
submits job.sbatch once. Slurm runs the batch script on cse-isha (first node in
its order), which only starts this driver as a step on cse-lambda1, where the
run folder is. Every task is an `srun` step running the shared task program
(task/: read inputs, sleep or hash, write random-byte outputs). The run's
transport (docs/data-movement.md) decides how data moves:

direct (default; needs nothing shared)
  setup  sbcast task.pyz to node-local /tmp on every machine; start one data
         server per machine as its own step (--overlap) and read its address;
  tasks  write outputs to node-local scratch; a consumer reads same-machine
         inputs from local disk and pulls the others from the producer
         machine's data server;
  end    collect what the servers served, shut them down (they delete their
         scratch and task.pyz).

shared_fs (needs a shared work dir, e.g. NFS: testbed/slurm/nfs.md)
  setup  copy task.pyz into this run's folder in the work dir
         (<workdir>/<user>/<run_id>/), create data/ there, and check with one
         step per machine that both are visible everywhere;
  tasks  write outputs to data/ and read inputs from there;
  end    delete the run's folder in the work dir (unless keep_data).

Either way the loop is the same, as a workflow system on Slurm would do it:
    start every task whose producers' steps have all COMPLETED and whose
    start_after tasks (machine predecessor in SAGA's order, with enact_order
    "order") have been started                         (srun -w <node> ...)
    when a step exits, record its FID_RESULT line (phase timings, core)
until nothing is pending or running; then write results.json.

Uses only the Python standard library.

Usage (normally called by job.sbatch):
    python3 driver.py <run folder>/plan.json
"""

import json
import os
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

POLL = 0.005                # seconds between checks for finished steps
SERVER_START_TIMEOUT = 120  # seconds for every data server to report its address
SERVER_STOP_TIMEOUT = 30
TOKEN_HEADER = "X-Fid-Token"
TRANSPORTS = ("direct", "shared_fs")


def log(t0: float, message: str) -> None:
    print(f"[{time.time() - t0:8.3f}s] {message}", flush=True)


# ---------------------------------------------------------------------------
# Commands (pure; tested offline and printed by dry runs)
# ---------------------------------------------------------------------------

def is_direct(plan: dict) -> bool:
    return plan.get("transport", "direct") == "direct"


def remote_pyz(plan: dict) -> str:
    """task.pyz as every machine sees it: sbcast into node-local /tmp next to the
    scratch folder (direct), or in the run's shared folder (shared_fs)."""
    if is_direct(plan):
        return f"{plan['scratch']}-{plan['run_id']}.pyz"
    return f"{plan['shared_dir']}/task.pyz"


def shared_data_dir(plan: dict) -> str:
    return f"{plan['shared_dir']}/data"


def python_cmd(plan: dict, *args: str) -> List[str]:
    """`<task_python> task.pyz args` through bash, so $HOME in task_python is
    expanded on the node the step runs on (home folders differ per machine)."""
    script = f"exec {plan['task_python']} {shlex.quote(remote_pyz(plan))} " + " ".join(shlex.quote(a) for a in args)
    return ["bash", "-c", script.strip()]


def sbcast_command(plan: dict) -> List[str]:
    return ["sbcast", "--force", plan["pyz"], remote_pyz(plan)]


def visibility_check_command(plan: dict) -> List[str]:
    """One tiny step per machine: is the run's shared folder (task.pyz, data/) there?"""
    pyz, data = shlex.quote(remote_pyz(plan)), shlex.quote(shared_data_dir(plan))
    n = len(plan["machines"])
    return ["srun", "--overlap", f"--nodes={n}", f"--ntasks={n}", "--ntasks-per-node=1",
            f"--nodelist={','.join(plan['machines'])}", "--mem=64M", "--job-name=check-workdir", "bash", "-c",
            f'test -r {pyz} && test -w {data} || {{ echo "$(hostname): {data} not shared here"; exit 1; }}']


def server_command(plan: dict, machine: str) -> List[str]:
    s = plan["server"]
    return [
        "srun", "--overlap",                          # shares the job's CPUs, takes no task slot
        "--nodes=1", "--ntasks=1", f"--nodelist={machine}",
        f"--cpus-per-task={plan['cpus_per_node']}",   # floats over all of the job's CPUs there
        "--cpu-bind=none", f"--mem={s['mem']}", f"--job-name=data-{machine}",
        *python_cmd(plan, "serve", "--run", plan["run_id"], "--node", machine,
                    "--scratch", plan["scratch"], "--peers", ",".join(plan["machines"]),
                    "--port", str(s.get("port", 0)), "--cleanup", remote_pyz(plan),
                    *(["--bind", s["bind"][machine]] if s.get("bind", {}).get(machine) else [])),
    ]


def step_command(task: dict, plan: dict) -> List[str]:
    """One srun job step running the shared task program on the task's machine."""
    return [
        "srun",
        "--nodes=1", "--ntasks=1",
        "--exact",                                  # use only the CPUs/memory asked for, so steps can share a node
        "--cpu-bind=none",                          # Slurm's pinning fails for a 2nd step on a node; cgroups still
                                                    # keep each step on its own core
        f"--nodelist={task['node']}",
        f"--cpus-per-task={task['cpus']}",
        f"--mem={task['mem']}",
        f"--job-name={task['name']}",
        *python_cmd(plan, "task"),
    ]


def task_env(task: dict, plan: dict, tasks: Dict[str, dict], servers: Optional[Dict[str, dict]] = None,
             token: str = "") -> Dict[str, str]:
    """The FID_* contract for one task (task/fidelity_core.py, task/transport_*.py)."""
    env = {
        "FID_MODE": plan["mode"],
        "FID_SECONDS": repr(float(task["seconds"])),
        "FID_SPEED": ",".join(f"{m}={s}" for m, s in plan["speed"].items()),
        "FID_HASH_RATE": repr(float(plan.get("hash_rate") or 0.0)),
        "FID_INPUTS": ",".join(f"{i['producer']}.{i['object']}" for i in task["inputs"]),
        "FID_OUTPUTS": ",".join(f"{o['object']}:{o['bytes']}" for o in task["outputs"]),
        "FID_TASK": task["name"],
        "FID_NODE": task["node"],
    }
    if not is_direct(plan):
        return {**env, "FID_TRANSPORT": "shared_fs", "FID_WORKDIR": shared_data_dir(plan),
                "FID_LATENCY_WAIT": repr(float(plan.get("latency_wait", 5.0)))}
    return {
        **env,
        "FID_TRANSPORT": "direct",
        "FID_RUN": plan["run_id"],
        "FID_SCRATCH": plan["scratch"],
        "FID_SERVERS": ",".join(f"{m}={s['host']}:{s['port']}" for m, s in (servers or {}).items()),
        "FID_INPUT_NODES": ",".join(f"{i['producer']}.{i['object']}={tasks[i['producer']]['node']}"
                                    for i in task["inputs"]),
        "FID_TOKEN": token,
    }


def parents_completed(task: dict, results: Dict[str, dict]) -> bool:
    """Every producer's step COMPLETED, so its outputs are written."""
    return all(results.get(i["producer"], {}).get("state") == "COMPLETED" for i in task["inputs"])


def parents_end(task: dict, results: Dict[str, dict]) -> Optional[float]:
    """When the last input was written: the producers' closeUnix (their clocks)."""
    ends = [results.get(i["producer"], {}).get("end") for i in task["inputs"]]
    return max(ends) if ends and None not in ends else None


def started_after(task: dict, pending: set) -> bool:
    """Enacted start order: the tasks before this one on its machine have been started
    (as Wayline's enactOrder "order"; they need not have finished). Started within this
    loop iteration counts: they leave `pending` as soon as they are launched."""
    return not any(prev in pending for prev in task.get("start_after", []))


def parse_step_output(path: Path) -> dict:
    """The task's last 'FID_RESULT {json}' line (timings, host, core)."""
    result: dict = {}
    if not path.exists():
        return result
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("FID_RESULT "):
            try:
                result = json.loads(line[len("FID_RESULT "):])
            except json.JSONDecodeError:
                continue
    return result


# ---------------------------------------------------------------------------
# Data servers (direct transport)
# ---------------------------------------------------------------------------

def server_call(server: dict, method: str, path: str, token: str, timeout: float = 5.0) -> dict:
    req = urllib.request.Request(f"http://{server['host']}:{server['port']}{path}", method=method,
                                 data=b"" if method == "POST" else None, headers={TOKEN_HEADER: token})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body) if body.startswith(b"{") else {}


def read_hello(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("{") and '"fid_server"' in line:
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                pass
    return None


def start_servers(plan: dict, run_dir: Path, env: dict, t0: float) -> tuple:
    procs, servers = {}, {}
    for m in plan["machines"]:
        out = run_dir / f"server-{m}.log"
        procs[m] = subprocess.Popen(server_command(plan, m), stdout=out.open("w"), stderr=subprocess.STDOUT, env=env)
    deadline = time.time() + SERVER_START_TIMEOUT
    while len(servers) < len(procs):
        for m, proc in procs.items():
            if m in servers:
                continue
            hello = read_hello(run_dir / f"server-{m}.log")
            if hello:
                servers[m] = hello
                log(t0, f"data server on {m}: {hello['host']}:{hello['port']} (python {hello['python']})")
            elif proc.poll() is not None:
                raise RuntimeError(f"data server on {m} exited ({proc.returncode}); see server-{m}.log")
        if time.time() > deadline:
            raise RuntimeError(f"data servers did not start within {SERVER_START_TIMEOUT}s: "
                               f"{sorted(set(procs) - set(servers))}")
        time.sleep(0.05)
    versions = {s["python"] for s in servers.values()}
    if len(versions) > 1:
        log(t0, f"WARNING: task Python differs between machines: {versions}")
    return procs, servers


def stop_servers(procs: dict, servers: dict, token: str, run_id: str) -> Dict[str, dict]:
    """Collect what each server served, then shut it down (it cleans its scratch)."""
    status = {}
    for m, s in servers.items():
        try:
            status[m] = server_call(s, "GET", f"/status/{run_id}", token)
            server_call(s, "POST", f"/shutdown/{run_id}", token)
        except Exception as e:
            print(f"could not stop data server on {m}: {e}", flush=True)
    deadline = time.time() + SERVER_STOP_TIMEOUT
    for proc in procs.values():
        try:
            proc.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            proc.terminate()
    return status


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def setup(plan: dict, run_dir: Path, env: dict, t0: float) -> tuple:
    """Deliver task.pyz (and start the data servers). Returns (server procs, servers)."""
    if is_direct(plan):
        rc = subprocess.run(sbcast_command(plan), capture_output=True, text=True)
        if rc.returncode != 0:
            raise RuntimeError(f"sbcast failed: {rc.stderr.strip()}")
        log(t0, f"sbcast {plan['pyz']} -> {remote_pyz(plan)} on {','.join(plan['machines'])}")
        return start_servers(plan, run_dir, env, t0)
    Path(shared_data_dir(plan)).mkdir(parents=True, exist_ok=True)
    shutil.copy(plan["pyz"], remote_pyz(plan))
    rc = subprocess.run(visibility_check_command(plan), capture_output=True, text=True)
    if rc.returncode != 0:
        raise RuntimeError(f"shared work dir {plan['shared_dir']} is not visible on every machine "
                           f"(testbed/slurm/nfs.md): {(rc.stdout + rc.stderr).strip()}")
    log(t0, f"shared work dir {plan['shared_dir']} visible on {','.join(plan['machines'])}")
    return {}, {}


def main() -> None:
    plan_path = Path(sys.argv[1])
    plan = json.loads(plan_path.read_text())
    run_dir = plan_path.parent
    run_id = plan["run_id"]
    t0 = time.time()

    host = socket.gethostname()
    if host != plan["driver_node"]:
        sys.exit(f"driver is running on {host}, expected {plan['driver_node']}")

    tasks: Dict[str, dict] = {t["name"]: t for t in plan["tasks"]}
    pending = set(tasks)
    running: Dict[str, tuple] = {}       # name -> (process, launch time, output path)
    results: Dict[str, dict] = {}
    token = secrets.token_hex(16)
    env = {**os.environ, "FID_TOKEN": token}
    server_procs: dict = {}
    servers: dict = {}
    log(t0, f"driver on {host}: {len(tasks)} tasks, mode {plan['mode']}, transport {plan['transport']}, "
            f"Slurm job {os.environ.get('SLURM_JOB_ID', '?')}")

    try:
        server_procs, servers = setup(plan, run_dir, env, t0)
        while pending or running:
            # 1. Collect steps that have exited.
            for name, (proc, launched, out_path) in list(running.items()):
                rc = proc.poll()
                if rc is None:
                    continue
                finished = time.time()
                del running[name]
                rec = parse_step_output(out_path)
                state = "COMPLETED" if rc == 0 and rec else "FAILED"
                results[name] = {
                    "node": rec.get("host", tasks[name]["node"]),
                    "state": state, "exit_code": rc,
                    "launch": launched, "finish": finished,                      # driver's clock (lambda)
                    "start": rec.get("taskStartUnix"), "end": rec.get("closeUnix"),   # the task's own clock
                    "core": rec.get("core"), "linux_cpus": rec.get("linux_cpus"),
                    "timings": rec,
                }
                log(t0, f"{name:<8} {state:<9} on {results[name]['node']} (exit {rc})")

            # 2. Never start tasks downstream of a failure.
            for name in sorted(pending):
                bad = [i["producer"] for i in tasks[name]["inputs"]
                       if results.get(i["producer"], {}).get("state") in ("FAILED", "SKIPPED")]
                if bad:
                    pending.discard(name)
                    results[name] = {"node": tasks[name]["node"], "state": "SKIPPED"}
                    log(t0, f"{name:<8} SKIPPED (failed {', '.join(bad)})")

            # 3. Start every task whose producers have completed, earliest-planned first.
            ready = [n for n in pending if parents_completed(tasks[n], results) and started_after(tasks[n], pending)]
            for name in sorted(ready, key=lambda n: (tasks[n]["predicted_start"], n)):
                out_path = run_dir / f"{name}.out"
                proc = subprocess.Popen(step_command(tasks[name], plan), stdout=out_path.open("w"),
                                        stderr=subprocess.STDOUT,
                                        env={**env, **task_env(tasks[name], plan, tasks, servers, token)})
                running[name] = (proc, time.time(), out_path)
                pending.discard(name)
                log(t0, f"{name:<8} started   on {tasks[name]['node']}")

            if pending and not running and not ready:
                # Nothing can make progress (should not happen with a valid plan: a task
                # waiting only for start_after would be an order/dependency cycle).
                for name in sorted(pending):
                    results[name] = {"node": tasks[name]["node"], "state": "SKIPPED"}
                log(t0, f"stuck: {sorted(pending)} can never start")
                break

            time.sleep(POLL)
    except Exception as e:
        log(t0, f"ERROR: {e}")
        for name in sorted(pending):
            results.setdefault(name, {"node": tasks[name]["node"], "state": "SKIPPED"})
    finally:
        for proc, _, _ in running.values():   # e.g. the job was cancelled
            proc.terminate()
        for name, r in results.items():
            r["inputs_ready"] = parents_end(tasks[name], results)
        served = stop_servers(server_procs, servers, token, run_id)
        if not is_direct(plan) and not plan.get("keep_data") and plan.get("shared_dir"):
            shutil.rmtree(plan["shared_dir"], ignore_errors=True)
        (run_dir / "results.json").write_text(json.dumps({
            "driver_node": host, "driver_start": t0, "transport": plan["transport"], "tasks": results,
            "servers": servers,
            "transfers": [dict(t, src_node=m) for m, st in served.items() for t in st.get("served", [])],
        }, indent=2))

    failed = [n for n, r in results.items() if r["state"] != "COMPLETED"]
    log(t0, f"done: {len(results) - len(failed)}/{len(tasks)} completed")
    sys.exit(1 if failed or len(results) < len(tasks) else 0)


if __name__ == "__main__":
    main()
