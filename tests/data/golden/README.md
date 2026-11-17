# Golden bridge requests

`*/controller-request.json` is the exact body Wayline's odag-controller POSTs to
the saga-sidecar for `*/template.yml` (with `*/network-profile.yml` loaded and
12000 millicores per node), produced by running the controller's own Go code:
`golden_request_test.go.txt`, copied temporarily into
`wayline/cmd/odag-controller/` (branch `saga-executor` @ 721d478) and run with

```bash
GOLDEN_TEMPLATE=.../template.yml GOLDEN_CONFIGMAP=.../network-profile.yml \
GOLDEN_OUT=.../controller-request.json go test -run TestZZGoldenRequest .
```

It uses `extractTasks`, `extractProfilingConfig`, the runtime/data-size/bandwidth
resolvers (with a profiler DB, as deployed) and `sagaAssignTasksWithModel` (forwarding `schedulerConfig.model`, as main.go does) against a stub
server. `tests/test_scheduling.py` checks that `saga_exec.scheduling.build_request`
gives the same JSON, and that `convert.to_template` still produces these
templates. `machines.yaml` is the testbed file at the time. Cases: cores model with free links (default), cores model with measured links + a cores override + speed/cpu/options, and the bridge default capacity model (no `model`).
Regenerate all of it if the controller's request or the template format changes.
