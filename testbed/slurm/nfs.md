# Shared work dir over NFS (recommended setup, NOT DONE YET)

The `shared_fs` transport stages task data as files in a work dir that every
machine mounts at the **same path**. This is how Snakemake, Nextflow and Pegasus
run on Slurm. The default `direct` transport does not need it. The path is
`machines.yaml` `partitions.slurm.shared_workdir` (`/srv/saga-exec`), and the run
setting `"workdir"` overrides it.

Proposal: export a directory on cse-lambda1's local disk and mount it at the
same path on both machines. Needs sudo on both; check paths and the subnet
first.

**cse-lambda1 (server):**

```bash
sudo apt install nfs-kernel-server
sudo mkdir -p /export/saga-exec /srv/saga-exec
sudo chmod 1777 /export/saga-exec              # every user makes <user>/ inside (sticky)
# /etc/exports: only the cluster subnet (the address range of eno1np0)
echo '/export/saga-exec <cluster-subnet>/24(rw,sync,no_subtree_check,root_squash)' | sudo tee -a /etc/exports
sudo exportfs -ra
# Same path on lambda too. Either a bind mount (local disk; fastest, but lambda's
# "transfers" then never touch NFS) ...
echo '/export/saga-exec /srv/saga-exec none bind 0 0' | sudo tee -a /etc/fstab
# ... or a loopback NFS mount (lambda goes through the NFS client like isha;
# symmetric, slower). Pick one and record which in machines.yaml.
sudo mount /srv/saga-exec
```

**cse-isha (client):**

```bash
sudo apt install nfs-common
sudo mkdir -p /srv/saga-exec
echo 'cse-lambda1:/export/saga-exec /srv/saga-exec nfs4 rw,hard,noatime,_netdev 0 0' | sudo tee -a /etc/fstab
sudo mount /srv/saga-exec
```

**Check from lambda:**

```bash
srun -w cse-isha bash -c 'mkdir -p /srv/saga-exec/$USER && touch /srv/saga-exec/$USER/x && ls -l /srv/saga-exec/$USER'
ls -l /srv/saga-exec/$USER/x                    # visible on lambda as well
```

Notes:

- The driver checks at job start that the run's folder is visible and writable
  on every machine and stops with an error if it is not.
- With NFS, a "same-machine" transfer on isha goes to lambda and back, while
  lambda→isha crosses the network once. SAGA's link model cannot express this.
  It is an open question in docs/formats.md, and `saga-calibrate-links
  --transport shared_fs` measures it.
- NFS client caching: a file that another machine just created can be invisible
  for a moment. Tasks retry for `latency_wait` seconds (run setting, default 5).
- Do not use the NFS export for `/tmp` scratch or for the `direct` transport.
