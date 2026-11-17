# Task environment on the Slurm machines (for Sebastian)

Every Slurm task now runs the shared task program (`task/`, the same one Wayline
runs), so it does real work and really moves its data (docs/data-movement.md).
The program uses only the Python standard library. Each machine needs only **the
same uv-managed Python**: no venv, no packages, no copy of this repo. Home
folders are not shared, so the driver delivers the program at job start.

## 1. Python, once per machine (as the user who submits)

On **both** cse-lambda1 and cse-isha:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh        # if uv is missing (isha)
uv python install 3.12.8                               # pin the same patch version on both
~/.local/bin/python3.12 -VV                            # same output on both machines
```

`uv python install` puts `python3.12` in `~/.local/bin`. That is the run
setting `"task_python": "$HOME/.local/bin/python3.12"` (the default; `$HOME` is
expanded on each machine). A different interpreter changes sha256 speed, so
re-run the CPU calibration after any change. The driver warns when the
machines report different Python versions.

## 2. How the program reaches each machine

`allocation.py` packs `task/*.py` into `<run folder>/task.pyz` (a zipapp) on lambda.

- `"transport": "direct"` (default): the driver runs `sbcast` to copy it to
  `/tmp/saga-exec-<user>-<run>.pyz` on every machine of the job. The data
  servers delete it at the end.
- `"transport": "shared_fs"`: the driver copies it into the run's folder in the
  shared work dir (`<workdir>/<user>/<run>/task.pyz`) and checks that every
  machine sees it. This needs the NFS setup in [nfs.md](nfs.md).

## 3. Checks before the first run (from lambda)

```bash
srun -w cse-isha bash -c '~/.local/bin/python3.12 -VV'      # Python is there
srun -w cse-isha getent hosts cse-lambda1                    # names resolve to the
srun -w cse-lambda1 getent hosts cse-isha                    #   cluster NIC (eno1np0), not 127.x
srun -w cse-isha df -h /tmp                                  # disk, not tmpfs; enough space
```

**Firewall**: with `direct`, a task on one machine pulls from the data server on
the other machine, on a random high port by default. If a host firewall (ufw)
is on, either allow TCP between the two machines, or fix a port with the run
setting `"server_port": 47000` and allow that port. If a name resolves to the
wrong interface, set `"server_bind": {"cse-lambda1": "<ip>", "cse-isha": "<ip>"}`.

## 4. First runs

```bash
# 1. look at what would run (nothing is submitted)
#    copy a run file and add "dry_run": true
uv run saga-slurm /tmp/diamond-dry.json

# 2. small real run, sleep mode, default direct transport
uv run saga-slurm experiments/slurm/runs/diamond.json
#    -> ~/slurm-logs/saga/<run_id>/: driver.log, server-<machine>.log, <task>.out,
#       results.json (incl. "transfers"), actual.json (input_read/compute/handoff),
#       summary.json, gantt.png

# 3. a run with real cross-machine data
uv run saga-slurm experiments/slurm/runs/transfer_test.json

# 4. CPU calibration (holds Slurm's share of both machines for ~2 min),
#    then cpu-mode runs: add "mode": "cpu" to a run file
uv run saga-calibrate-cpu

# 5. once NFS is set up (nfs.md): the same runs with "transport": "shared_fs"
```

If a run hangs at start, read `driver.log` and `server-*.log`. A data server that
never prints its address usually means Python is missing on that machine or the
name lookup failed.
