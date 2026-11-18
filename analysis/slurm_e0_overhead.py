import json, sys, statistics as st, collections
from pathlib import Path
runs=[Path.home()/'slurm-logs/saga'/l.strip() for l in open(sys.argv[1])]
mk=collections.defaultdict(list); work=[]; first=[]; edges=collections.defaultdict(lambda: collections.defaultdict(list)); offs=collections.defaultdict(list)
for r in runs:
    a=json.load(open(r/'actual.json')); p=json.load(open(r/'prediction.json')); s=json.load(open(r/'summary.json'))
    name=Path(s.get('run_file','?')).stem
    mk[name].append((p['makespan'], a['makespan'], a['makespan_work'], a['first_work_start']))
    for t,v in a['tasks'].items():
        pd=p['tasks'][t]; work.append(v['durations']['work']-(pd['end']-pd['start']))
        if v['times']['launched'] is not None and v['times']['task_start'] is not None: first.append(v['times']['task_start']-v['times']['launched'])
    for e in (a['edges'].values() if isinstance(a['edges'],dict) else a['edges']):
        d=e['decomposition']; k=e['kind']
        edges[k]['work_gap'].append(e['work_gap'])
        for f in ('producer_write','wait_launch','consumer_read'): edges[k][f].append(d[f])
        w=d.get('wait_launch_detail',{})
        for f in ('producer_end_to_consumer_start','available_to_consumer_start'):
            if w.get(f) is not None: edges[k][f].append(w[f])
    for when in ('start','end'):
        for h,v in ((a.get('clocks') or {}).get(when) or {}).get('hosts',{}).items():
            for line in v.get('raw',[]):
                if 'Offset' in line or 'System time' in line: offs[h].append(line.split(None,1)[1].strip())
f=lambda xs: f"mean {st.mean(xs)*1000:7.1f} ms  (min {min(xs)*1000:6.1f}, max {max(xs)*1000:6.1f}, n={len(xs)})"
print("MAKESPAN  predicted | actual (task start->end) | actual work-only | first work start")
for n,v in mk.items():
    for x in v: print(f"  {n:13s} {x[0]:6.2f} | {x[1]:6.3f} (+{(x[1]-x[0])*1000:5.0f}ms) | {x[2]:6.3f} | {x[3]*1000:5.2f}ms")
print("TASK work duration - SAGA duration:", f(work))
print("TASK srun launch -> task program start:", f(first))
for k,v in edges.items():
    print(f"EDGE kind={k}")
    for fld,xs in v.items(): print(f"   {fld:32s}", f(xs))
for h,v in offs.items(): print("CLOCK",h, sorted(set(v))[:6])
