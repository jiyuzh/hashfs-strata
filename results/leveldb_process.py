#!/bin/env python3
import json
import re
idx = ['ext', 'hash' ]
layout=[str(i) for i in [70]]
num = [str(i) for i in [4000000]]
value = [512]
rep = [str(i) for i in range(1,2)]

interests_tp = [ 'fillseq', 'readseq', 'fillrandom', 'overwrite']
interests = ['readrandom']

#r'readrandom\s*:\s*([0-9.]*)'
for i in idx:
    for l in layout:
        for n in num:
            for v in value:
                dst_fn = "db_bench_{}_{}_{}_{}.json".format(i, l,n,v)
                ds = []
                for r in rep:
                    d = {}
                    fn = "db_bench_{}_{}_{}_{}_{}".format(i, l,n,v,r)
                    with open(fn, "r") as f:
                        fs = f.read()
                        for kt in interests_tp:
                            m = re.findall(r'{}[^0-9.]*([0-9.]*)[^0-9.]*([0-9.]*)'.format(kt), fs)
                            d[kt] = m
                        for k in interests:
                            m = re.findall('{}\s*:\s*([0-9.]*)'.format(k), fs)
                            d[k] = m
                    ds += [d]
                with open(dst_fn, "w") as f:
                    json.dump(ds, f, indent=4);
