# docs/

- `formats.md` — DRAFT proposal for shared formats (instance, machines.yaml,
  prediction/actual) and the full list of current divergences between the two
  backends (open questions).
- Backend docs: `src/saga_exec/backends/wayline/README.md` (Nick's original fidelity README),
  `testbed/k8s/README.md`, `testbed/slurm/README.md` (Sebastian's original README).

## Missing: the fidelity plan

Nick's README (`eval/experiments/fidelity/README.md` in his Wayline fork) says
"Plan: `../../../../fidelity-plan/main.tex` (outside the repo)" and refers to it
for the execution model ("see plan"). That resolves to
`~/src/fidelity-plan/main.tex in Nick's home directory`, which **does not exist on
this machine** (cse-lambda1, checked 2026-11-17), and no `fidelity-plan` directory
was found elsewhere. Ask Nick for it; it should probably live here as
`docs/fidelity-plan/`.
