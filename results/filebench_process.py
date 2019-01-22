#!/bin/env python3
import json
import re
idx = ['ext', 'hash']
type = ['seq','rand']
layout = ['layout{}'.format(i) for i in [60, 70, 80, 90, 100]]
seq = [str(i) for i in range(1,6)]
for i in idx:
    for t in type:
        for l in layout:
            objs = []
            for s in seq:
                fn = "{}_filebench_{}_4k_{}_{}.json".format(i,t,l,s)
                with open(fn, "r") as f:
                    jsobj = json.load(f)
                stdoutfn = "{}_filebench_{}_4k_{}_{}.stdout".format(i,t,l,s)
                with open(stdoutfn, "r") as f:
                    m = re.search(r'([0-9.]+)mb', f.read())
                    tp = m.group(1)
                jsobj["tp"] = tp
                objs += [jsobj]
            outfn = "{}_filebench_{}_4k_{}.json".format(i,t,l)
            with open(outfn, "w") as f:
                json.dump(objs, f, indent=4)
