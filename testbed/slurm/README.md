<!-- Imported verbatim from Sebastian Lange's README, github.com/MasterSebas04/saga-slurm
     @ e4f6de2b5546fe54060c4f19bfc3d417fcdd1312. Paths in "Usage" refer to that repo:
     executor/saga_slurm.py is now `uv run saga-slurm` (saga_exec/backends/slurm),
     runs/ is now experiments/slurm/runs/. slurm.conf and cgroup.conf are also
     extracted next to this file. -->

# saga-slurm

Runs saga task-graph schedules on a two-node Slurm cluster (`cse-lambda1` and `cse-isha`), then compares the actual run times with saga's predictions. Tasks are placeholder `sleep`s for now, each lasting as long as saga predicted.

This is the Slurm counterpart to Nick Laus's Kubernetes setup, which uses [wayline](https://github.com/ANRGUSC/wayline).

Maintainer: Sebastian Lange

## Usage

Run on lambda, in a Python environment with `saga`, `numpy` and `matplotlib`:

```bash
python executor/saga_slurm.py runs/diamond.json
```

Each run is described by a JSON file in `runs/`. To make a new one, copy an example and edit it:

```json
{
  "scheduler": "heft",
  "time_scale": 1,
  "nodes": {"cse-lambda1": 1.0, "cse-isha": 0.5},
  "tasks": {"A": 5, "B": 3, "C": 4, "D": 2},
  "dependencies": [["A", "B"], ["A", "C"], ["B", "D"], ["C", "D"]]
}
```

A task sleeps for `cost / node speed × time_scale` seconds. Here, task A would sleep 5 s on lambda or 10 s on isha.

| Setting | Default | Meaning |
|---|---|---|
| `tasks` | | Task names and costs |
| `dependencies` | none | `[parent, child]`, or `[parent, child, data size]` |
| `graph` | | Random graph instead of `tasks`: `shape` (`diamond`, `chain`, `fork` or `branching`), `seed`, `min_cost`, `max_cost` |
| `nodes` | all `1.0` | Node speeds |
| `scheduler` | `heft` | saga scheduler, such as `heft` or `cpop` |
| `strict_order` | `true` | Each task also waits for the previous task on its machine |
| `time_scale` | `10` | Seconds of sleep per unit of saga time |
| `bandwidth` | `1e9` | Link speed between the machines. A task waits `data size / bandwidth × time_scale` for data from a parent on the other machine (simulated, no data moves) |
| `cpus` | `1` | Whole cores per task |
| `mem` | `1G` | Memory per task |
| `poll` | `1` | Seconds between status checks |
| `dry_run` | `false` | Print the job without submitting it |

`allocation.py` submits one `sbatch` for the whole schedule, and `driver.py` starts each task as an `srun` step on its assigned machine. `per_task.py` (one `sbatch` per task) is still in the folder, but nothing uses it for now.

Results go to `~/slurm-logs/saga/<run_id>/` on lambda, including `summary.json`, `gantt.png` (predicted vs. actual) and each task's log.

## Cluster

| | cse-lambda1 | cse-isha |
|---|---|---|
| Role | Controller and worker | Worker |
| OS | Ubuntu 24.04 | Ubuntu 22.04 |
| Slurm | 23.11.4 (apt) | 23.11.10 (built from source) |
| CPU | Threadripper PRO 7975WX | EPYC 9354 |
| Cores / threads | 32 / 64 | 32 / 64 |

Slurm gets cores 4–17 (Linux CPUs `4-17,36-49`) and 64 GB on each machine. Cores 0–3 are for the OS, and cores 18–31 are set aside for Kubernetes.

## Configuration

Both files must be identical on both machines:

```ini
# /etc/slurm/slurm.conf
ClusterName=saga
SlurmctldHost=cse-lambda1
SlurmUser=slurm
StateSaveLocation=/var/spool/slurmctld
SlurmdSpoolDir=/var/spool/slurmd
ProctrackType=proctrack/cgroup
TaskPlugin=task/cgroup,task/affinity
SelectType=select/cons_tres
SelectTypeParameters=CR_Core_Memory
ReturnToService=2

NodeName=cse-lambda1 CPUs=64 Boards=1 SocketsPerBoard=1 CoresPerSocket=32 ThreadsPerCore=2 RealMemory=65536 CpuSpecList=0-7,36-63
NodeName=cse-isha CPUs=64 Boards=1 SocketsPerBoard=1 CoresPerSocket=32 ThreadsPerCore=2 RealMemory=65536 CpuSpecList=0-7,36-63

PartitionName=saga Nodes=ALL Default=YES MaxTime=INFINITE State=UP
```

```ini
# /etc/slurm/cgroup.conf
ConstrainCores=yes
ConstrainRAMSpace=yes
```

`CpuSpecList` lists the CPUs jobs can't use, in Slurm's numbering (core *k* is Slurm CPUs 2*k* and 2*k*+1), which differs from Linux's. `cgroup.conf` makes Linux enforce the CPU and memory limits.

After changing a config, copy it to the other machine, restart Slurm (`slurmctld` and `slurmd` on lambda, `slurmd` on isha), and rerun the checks below.

## Setup

Both machines need synced clocks, the same `slurm` user ID (64030) and the same `/etc/munge/munge.key` (lambda's is the master copy). Copy the config files above to `/etc/slurm/` on both.

**lambda:**

```bash
sudo apt install munge slurm-wlm
sudo mkdir -p /var/spool/slurmctld /var/spool/slurmd
sudo chown slurm:slurm /var/spool/slurmctld
sudo systemctl enable --now munge slurmctld slurmd
```

**isha:** Ubuntu 22.04's apt only has Slurm 21.08, which is too old for lambda's controller, so build it from source. `libdbus-1-dev` is required for cgroup support. Keep the source folder, because `sudo make uninstall` needs it.

```bash
sudo apt install munge build-essential libmunge-dev libdbus-1-dev libhwloc-dev libnuma-dev libpam0g-dev
mkdir -p ~/slurm-build && cd ~/slurm-build
wget https://download.schedmd.com/slurm/slurm-23.11.10.tar.bz2
tar -xaf slurm-23.11.10.tar.bz2 && cd slurm-23.11.10
./configure --prefix=/usr --sysconfdir=/etc/slurm --libdir=/usr/lib/x86_64-linux-gnu
make -j$(nproc) && sudo make install && sudo ldconfig
sudo cp etc/slurmd.service /etc/systemd/system/ && sudo systemctl daemon-reload
sudo mkdir -p /var/spool/slurmd
sudo systemctl enable --now munge slurmd
```

**Check it from lambda:**

```bash
sinfo                                                                # both nodes "idle"
srun -w cse-isha hostname                                            # cse-isha
srun -w cse-lambda1 -c 28 grep Cpus_allowed_list /proc/self/status   # 4-17,36-49
srun -w cse-isha -c 28 grep Cpus_allowed_list /proc/self/status      # 4-17,36-49
```

