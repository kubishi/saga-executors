# Kubernetes / Wayline testbed

Two-node kubeadm cluster: **cse-lambda1** (control plane + worker, local image
registry `cse-lambda1:5000`) and **cse-isha** (worker). Set up by Nick Laus.

## What the Wayline backend needs (checked in run.py / convert.py / task.py)

| Piece | Where it runs | Needs |
|---|---|---|
| `convert.py` (+ `experiments/wayline/generate.py`) | anywhere | this package only (incl. Wayline's bridge, `wl-saga-sidecar`, for the offline prediction). No cluster. |
| `run.py` | cse-lambda1 | **`kubectl`** on PATH with a kubeconfig for the cluster, Wayline's CRDs (`ODAGTemplate`, `ODAG`, group `wl.io/v1`) and controller deployed **from Wayline branch `saga-executor`** (records SAGA's prediction in `status.scheduling`) **with a working `saga-sidecar` container** (built from the same SAGA as `uv.lock`; the `cse-lambda1:5000` registry has no `wl-saga-sidecar` image as of 2026-11-17), namespace `wl-runs`. Applies each folder's `network-profile.yml` (ConfigMap `wl-system/wl-network-profile`, from `machines.yaml`). It does **not** use the `wl`/`wayline` CLI binary or the Wayline Python SDK: it reimplements `wayline run` by creating an `ODAG` from the template with `kubectl create`. |
| `analyze.py` | anywhere | this package (matplotlib, `saga.utils.draw`). Reads saved `status.json`: SAGA's prediction from `status.scheduling`, measured times from the SDK. |
| `task/task.py` | inside pods | Wayline Python SDK `wl` (`sdk/python/wl` in the Wayline repo, needs `pyzmq`), baked into the image `cse-lambda1:5000/wl-fidelity-task:latest` (see `task/Dockerfile`). |

So the Python SDK is **not** a dependency of `saga-executors`; it is only
copied into the task image from a Wayline checkout. Because of that no uv git
source was added. If host-side code ever needs the SDK, add e.g.

```toml
[tool.uv.sources]
wayline-sdk = { git = "https://github.com/ANRGUSC/wayline", subdirectory = "sdk/python", rev = "<commit>" }
```

## Deployed Wayline version

The smoke results (2026-11-05) were produced with Nick's fork:

- repo: `github.com/nick23447/wayline` (fork of `github.com/ANRGUSC/wayline`)
- branch `lab` @ `aff6fa8a698c1ca2cda6ecd6bb10948ef54df5a8`
- which is upstream/origin `main` @ `4168029feeaac5629e1456b72d29ec110c3a531d`
  (2026-09-28) + the lab-specific changes below + the fidelity experiment code
  (now in this repo). No changes to the controller, SDK or CRDs on `lab`.

**Assumption to verify:** that the images in `cse-lambda1:5000` (wl-odag-controller,
wl-data-agent, wl-saga-sidecar, wl-ui-server) were built from that commit. Not
checked (that would require querying the cluster).

## Lab-specific files (copied here, read-only reference)

- `wayline-deployments/wl-runs-namespace.yml` — namespace `wl-runs` (privileged PSA) used by the templates.
- `wayline-deployments/network-profile-configmap.yml` — `wl-network-profile` with the iperf3
  bandwidths measured 2026-10-27 (lambda1->isha 117000000 B/s, isha->lambda1 117625000 B/s).
  Now generated from `testbed/machines.yaml` as `network-profile.yml` in each converted
  folder (and applied by `run.py`); this copy is the lab's original.
- `wayline-lab.diff` — full `git diff origin/main...lab -- deployments examples` from the fork:
  the two files above plus image registry `192.168.1.163:5000` -> `cse-lambda1:5000` and
  nodeSelector `anrg-2` -> `cse-lambda1` for odag-controller, ui-server, data-agent,
  and the dag-pipeline example retargeted to the two machines.
- `kubeadm/kubeadm-config.yaml`, `kubeadm/patches/kubeletconfiguration.yaml`,
  `calico/custom-resources.yaml` — from Nick's `~/k8s-cluster/` (not a git repo;
  files dated 2026-09-15). Kubernetes v1.37.0, pod CIDR 10.244.0.0/16, Calico without
  encapsulation on `eno1np0`. Kubelet reserves 48 CPUs for the system + 4 for kube
  (which, together with Slurm's share, is how cores are split; see `testbed/machines.yaml`).
  Note: `kubeadm-config.yaml` points `patches.directory` at Nick's home dir.

Not copied: `~/k8s-cluster/logs/` (kubeadm init output may contain join tokens),
kubeconfigs, anything under `~/.kube` or `~/.ssh`.

## Recommended change: give each task a whole core (for Nick, NOT APPLIED)

**Mismatch.** A Slurm task gets `--cpus-per-task=2`, which is one physical core
(both hyperthreads, pinned by cgroups to Slurm's cores 4–17). A Wayline task
gets `cpu: "1"`, which is one hyperthread's worth of CFS quota anywhere in the
shared pool. Its sibling thread may run another task, and nothing pins it to the
k8s cores 18–31. So the "same" task does different work per second on the two
backends, and cpu-mode hash rates are not comparable (docs/formats.md open
question 6). Calibrating each backend separately (`calibration.cpu.<backend>`)
only measures the gap; it does not close it.

**Fix (kubelet static CPU manager + whole-core requests):**

1. Kubelet config on both nodes (`testbed/k8s/kubeadm/patches/kubeletconfiguration.yaml`):

   ```yaml
   cpuManagerPolicy: static
   cpuManagerPolicyOptions:
     full-pcpus-only: "true"        # allocate whole physical cores (both hyperthreads)
   # Replace systemReserved/kubeReserved cpu with an explicit CPU set: OS cores 0-3
   # and Slurm's 4-17 (+ their siblings), so k8s owns exactly cores 18-31.
   reservedSystemCPUs: "0-17,32-49"
   systemReserved: {memory: "210Gi"}   # keep the memory reservation; drop its cpu
   ```

   Changing the policy needs, per node: drain, stop kubelet, delete
   `/var/lib/kubelet/cpu_manager_state`, start kubelet, uncordon.
2. Tasks request integer CPUs with limits = requests (Guaranteed QoS). The
   controller already sets limits = requests (`parseResources`). With
   `full-pcpus-only` a task must ask for `cpu: "2"` (one core); `cpu: "1"` would
   be rejected (SMTAlignmentError). So set `Config.cpu = "2"` in
   `experiments/wayline/generate.py`. The SAGA cores model is unaffected, since
   `model.cores` is explicit.
3. Then `partitions.k8s` in machines.yaml becomes 14 cores (28 threads), with
   `task_unit: 1 physical core`, the same as Slurm. Re-run the CPU calibration
   for k8s.

Check afterwards: `kubectl exec <task pod> -- grep Cpus_allowed_list /proc/self/status`
should show two sibling CPUs (e.g. `18,50`).
