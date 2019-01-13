#! /usr/bin/env python3
import json
from argparse import ArgumentParser
from pathlib import Path

def parse(outfile):
    objs = []
    for fname in Path('.').iterdir():
        if fname == Path(__file__):
            continue
        if fname.is_file():
            with fname.open() as f:
                data = f.read()
                count = 1
                index = data.find('{')
                if index < 0:
                    continue
                index += 1
                while count > 0:
                    openb = data.find('{', index)
                    closeb = data.find('}', index)
                    if not openb > 0 and not closeb > 0:
                        print(data)
                        assert False
                    if openb < closeb and openb > 0:
                        count += 1
                        index = openb + 1
                    else:
                        count -= 1
                        index = closeb + 1

                raw_obj = data[0:index]
                obj = json.loads(raw_obj)
                if 'sigusr1' in obj['title']:
                    objs += [obj]

    with outfile.open('w') as f:
        json.dump(objs, f, indent=4)

def main():
    parser = ArgumentParser()
    parser.add_argument('outfile')

    args = parser.parse_args()

    parse(Path(args.outfile))

if __name__ == '__main__':
    main()
