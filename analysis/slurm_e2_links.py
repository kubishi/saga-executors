import json,glob,re,collections,statistics as st,sys
import numpy as np
from pathlib import Path
rows=collections.defaultdict(lambda: collections.defaultdict(list))
for d in glob.glob(str(Path.home()/'slurm-logs/saga/calib-direct-*')):
    m=re.match(r'calib-direct-(self|intra|inter)-(cse-\w+)-(\d+)-\d+-\d+',Path(d).name)
    if not m: continue
    case,mach,size=m.group(1),m.group(2),int(m.group(3))
    try: r=json.load(open(Path(d)/'results.json'))['tasks']
    except Exception: continue
    for p in [k for k in r if k.startswith('p')]:
        c=r.get('c'+p[1:],{})
        if r[p].get('state')!='COMPLETED' or c.get('state')!='COMPLETED': continue
        pt,ct=r[p]['timings']['timeline'],c['timings']['timeline']
        o=pt['outputs'][0]; i=ct['inputs'][0]
        key=(case,mach,c['node'])
        rows[key][size].append(dict(write=o['sendEndUnix']-o['sendStartUnix'], wait=i['readStartUnix']-o['sendEndUnix'],
            read=i['readEndUnix']-i['readStartUnix'], total=i['readEndUnix']-o['sendStartUnix']))
for key in sorted(rows):
    print(f"\n{key[0]:5s} {key[1]} -> {key[2]}")
    print(f"   {'size':>7s} {'n':>2s} {'total':>8s} {'write':>8s} {'wait/launch':>11s} {'read':>8s}")
    xs,ys,rs=[],[],[]
    for s in sorted(rows[key]):
        v=rows[key][s]; g=lambda f: st.mean(x[f] for x in v)
        print(f"   {s/1e6:6g}M {len(v):2d} {g('total')*1000:7.1f}ms {g('write')*1000:7.1f}ms {g('wait')*1000:10.1f}ms {g('read')*1000:7.1f}ms")
        for x in v: xs.append(s); ys.append(x['total']); rs.append(x['write']+x['read'])
    if len(set(xs))>1:
        b,a=np.polyfit(xs,ys,1); b2,a2=np.polyfit(xs,rs,1)
        print(f"   fit total      = {a*1000:6.1f}ms + size/{1/b/1e6:7.0f} MB/s")
        print(f"   fit write+read = {a2*1000:6.1f}ms + size/{1/b2/1e6:7.0f} MB/s   (excluding wait/launch)")
