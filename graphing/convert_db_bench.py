#! /usr/bin/env python3

import json
from pathlib import Path
import sys

tests = ['fillseq', 'fillrandom', 'overwrite', 'readseq', 'readrandom']

def main():
    infile = Path(sys.argv[1])
    outfile = Path(sys.argv[2])
    assert infile != outfile

    outdata = []
    with infile.open() as f:
        data = json.load(f)

        for obj in data:
            bench = obj['bench']
            layout = obj['layout']
            struct = obj['struct']
            for test in tests:
                for res in obj[test]:
                    total_time = res['lat']['mean']
                    outdata += [{
                                'workload': 'db_bench_%s' % (test),
                                'layout': layout,
                                'total_time': total_time,
                                'struct': struct
                               }]

    with outfile.open('w') as f:
        json.dump(outdata, f, indent=4)

if __name__ == '__main__':
    main()
