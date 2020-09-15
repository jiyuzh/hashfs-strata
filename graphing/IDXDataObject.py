from argparse import ArgumentParser
from collections import defaultdict
import copy
from IPython import embed
import itertools
import json
from enum import Enum
from math import sqrt
import pandas as pd
import numpy as np
from pathlib import Path
from pprint import pprint
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
import matplotlib.pyplot as plt
import matplotlib.patheffects as PathEffects
from matplotlib.patches import Patch
import matplotlib.ticker as ticker
import re
import yaml
import inflect
from scipy import stats

from Graph import Grapher

import pandas as pd

class IDXDataObject:

    @staticmethod
    def _parse_db_bench(data_obj):
        parsed = {}
        tests = ['fillseq', 'fillrandom', 'overwrite', 'readseq', 'readrandom']
        for test in tests:
            test_data = []
            for res in data_obj[test]:
                test_data += [res['lat']['mean']]
            test_series = pd.Series(test_data)
            parsed[test] = test_series.mean()

        return parsed

    @staticmethod
    def _parse_filebench(data_obj):
        parsed = {}
        keys = ['workload', 'struct', 'layout']
        data = ['throughput', 'trial num']

        parsed = { i: data_obj[i] for i in keys + data }

        parsed['struct'] = parsed['struct'].upper()

        # embed()
        return keys, parsed

    def _parse_relevant_fields(self, data_obj):
        parsed = {}
        if data_obj is not None and not data_obj:
            return None

        if 'bench' in data_obj and data_obj['bench'] == 'db_bench':
            return self._parse_db_bench(data_obj)

        if 'bench' in data_obj and data_obj['bench'] == 'filebench':
            return self._parse_filebench(data_obj)

        labels = ['struct', 'layout', 'start size', 'io size', 'repetitions',
                  'num files', 'test', 'trial num']
        parsed = {l: data_obj[l] for l in labels if l in data_obj}

        if len(parsed) != len(labels) and 'ycsb_workload' not in data_obj \
            and 'Concurrency' not in parsed['test']:
            return None, None

        if 'start size' in parsed:
            if parsed['start size'] / 1024 ** 3 > 1:
                parsed['display start size'] = f'{parsed["start size"] // 1024 ** 3}GB'
            elif parsed['start size'] / 1024 ** 2 > 1:
                parsed['display start size'] = f'{parsed["start size"] // 1024 ** 2}MB'
            elif parsed['start size'] / 1024  > 1:
                parsed['display start size'] = f'{parsed["start size"] // 1024}KB'
            else:
                parsed['display start size'] = f'{int(parsed["start size"])}'

        okeys = list(parsed.keys())
        if 'trial num' in okeys:
            okeys.remove('trial num')

        if 'repetitions' in parsed:
            parsed['repetitions'] = str(int(parsed['repetitions']))
            parsed['reps'] = int(parsed['repetitions'])
            parsed['hot_or_cold'] = 'hot' if parsed['reps'] > 1000 else 'cold'
            okeys += ['hot_or_cold']

        parsed['layout'] = float(parsed['layout']) / 100.0
        parsed['layout_str'] = str(round(parsed['layout'], 2))
        okeys += ['layout_str']
        assert str(parsed['layout']) == parsed['layout_str'], 'wat'
        # if parsed['struct'] == 'HASHFS' and parsed['layout'] < 1.0 and parsed['hot_or_cold'] == 'hot':
        #     embed() 

        if 'log' in data_obj:
            parsed['log_per_op'] = data_obj['log']['commit']['tsc'] / max(data_obj['log']['commit']['nr'], 1.0)

        if 'read_end_to_end' in data_obj:
            parsed['read_end_to_end_per_op'] = data_obj['read_end_to_end']['tsc'] / max(data_obj['read_end_to_end']['nr'], 1.0)
            parsed['n_aligned'] = data_obj['read_end_to_end']['n_aligned']
            parsed['n_unaligned'] = data_obj['read_end_to_end']['n_unaligned']
        if 'bh_meta' in data_obj:
            parsed['bh_meta'] = data_obj['bh_meta']['tsc'] / max(data_obj['bh_meta']['nr'], 1.0)
        if 'ua_fcache' in data_obj:
            parsed['ua_fcache'] = data_obj['ua_fcache']['tsc'] / max(data_obj['ua_fcache']['nr'], 1.0)

        if 'wait_digest' in data_obj:
            parsed['wait_digest_per_op'] = data_obj['wait_digest']['tsc'] / max(data_obj['wait_digest']['nr'], 1.0)

        if 'read_data' in data_obj:
            parsed['read_data_bytes_per_cycle'] = data_obj['read_data']['bytes'] / max(data_obj['read_data']['tsc'], 1.0)

        if 'threads' in data_obj and data_obj['threads'] != 'T1':
            return None, None
        if 'num threads' in data_obj:
            parsed['threads'] = int(data_obj['num threads'])
            if parsed['threads'] > 32:
                return None, None
        if 'TP' in data_obj:
            parsed['throughput'] = data_obj['TP']
        if 'fragmentation' in data_obj:
            frag_obj = data_obj['fragmentation']
            parsed['nfiles'] = frag_obj['nfiles']
            parsed['nblocks'] = frag_obj['nblocks']
            parsed['nfragments'] = frag_obj['nfragments']
            parsed['layout_derived'] = frag_obj['layout_derived']

        if 'throughput' in data_obj:
            parsed['throughput'] = data_obj['throughput']
        if 'total_time' in data_obj:
            parsed['total_time'] = data_obj['total_time']
        if 'idx_stats' in data_obj:
            parsed['idx_compute_per_op'] = data_obj['idx_stats']['compute_tsc'] / max(data_obj['idx_stats']['compute_nr'], 1.0)

            if 'avg_depth' in data_obj['idx_stats']:
                parsed['avg_tree_depth'] = data_obj['idx_stats']['avg_depth']
            if 'memo_hit_ratio' in data_obj['idx_stats']:
                parsed['memo_hit_ratio'] = data_obj['idx_stats']['memo_hit_ratio']
                parsed['ref_per_lookup'] = data_obj['idx_stats']['ref_per_lookup']
            if 'nbuckets_checked' in data_obj['idx_stats']:
                parsed['nbuckets_checked'] = data_obj['idx_stats']['nbuckets_checked']

        if 'lsm' in data_obj:
            if data_obj['lsm']['nr']:
                parsed['indexing'] = data_obj['lsm']['tsc']
                parsed['read_data'] = data_obj['l0']['tsc'] + data_obj['read_data']['tsc']
                parsed['nops'] = data_obj['lsm']['nr'] 
                # if 'read_end_to_end' in data_obj:
                #     parsed['indexing'] = data_obj['read_end_to_end']['tsc'] - parsed['read_data']
            else:
                kernfs_obj = data_obj['kernfs']
                parsed['indexing'] = kernfs_obj['search']['total_time']
                parsed['read_data'] = kernfs_obj['digest'] - kernfs_obj['search']['total_time']
                #parsed['read_data'] = kernfs_obj['storage']['rtsc'] + kernfs_obj['storage']['wtsc']
                #parsed['indexing'] = kernfs_obj['digest'] - parsed['read_data']
                parsed['nops'] = kernfs_obj['search']['nr_search']

            total = parsed['indexing'] + parsed['read_data']
            parsed['total_breakdown'] = total

        if 'hashfs_lookups' in data_obj:
            hl = data_obj['hashfs_lookups']
            parsed['hash_entries_per_op'] = float(hl['nentries'] / max(hl['nops'], 1.0)) 

        if 'total_time' in data_obj and 'io size' in data_obj:
            total_bytes = data_obj['io size'] * \
                (data_obj['l0']['nr'] + data_obj['log']['write']['nr'])
            parsed['throughput'] = total_bytes / data_obj['total_time']

            read_bytes = data_obj['io size'] * data_obj['l0']['nr']
            parsed['read_throughput'] = read_bytes / data_obj['total_time']

            read_bytes = data_obj['io size'] * data_obj['log']['write']['nr']
            parsed['write_throughput'] = read_bytes / data_obj['total_time']
            parsed['write_throughput_mb'] = parsed['write_throughput'] / (1024 ** 2)


        if 'cache' in data_obj:
            cache_obj = data_obj['cache']
            if 'l1' in cache_obj:
                parsed['l1_accesses'] = cache_obj['l1']['accesses']
                parsed['l1_misses'] = cache_obj['l1']['misses']
                parsed['l2_accesses'] = cache_obj['l2']['accesses']
                parsed['l2_misses'] = cache_obj['l2']['misses']
                parsed['llc_misses'] = cache_obj['l3']['misses']
                parsed['llc_accesses'] = cache_obj['l3']['accesses']

            nvdimm_stats = {k: v for k, v in cache_obj.items() if 'nvdimm' in k}
            parsed.update(nvdimm_stats)

            if 'kernfs' in cache_obj:
                kern_cache = cache_obj['kernfs']
                if 'l1' in kern_cache:
                    parsed['l1_accesses'] += kern_cache['l1']['accesses']
                    parsed['l1_misses'] += kern_cache['l1']['misses']
                    parsed['l2_accesses'] += kern_cache['l2']['accesses']
                    parsed['l2_misses'] += kern_cache['l2']['misses']
                    parsed['llc_misses'] += kern_cache['l3']['misses']
                    parsed['llc_accesses'] += kern_cache['l3']['accesses']

            cache_keys = ['l1_accesses', 'l1_misses', 'l2_accesses', 
                          'l2_misses', 'llc_accesses', 'llc_misses']
           
            for k in cache_keys:
                if k in parsed:
                    parsed[k] /= parsed['nops']
    
            if 'l1_accesses' in parsed:
                parsed['l1_hits'] = (parsed['l1_accesses'] - parsed['l1_misses']) / max(parsed['l1_accesses'], 1.0)
                parsed['l2_hits'] = (parsed['l2_accesses'] - parsed['l2_misses']) / max(parsed['l2_accesses'], 1.0)
                parsed['llc_hits'] = (parsed['llc_accesses'] - parsed['llc_misses']) / max(parsed['llc_accesses'], 1.0)

        if 'ycsb_workload' in data_obj:
            parsed['test'] = \
                f'{data_obj["ycsb_workload"].split(".")[0].replace("workload", "").upper()}'
            # embed()
            # okeys['test'] = parsed['test']
            parsed['workload'] = parsed['test'].split('_')[0]
            parsed['throughput'] = float(data_obj['KTPS'])
            ops = ["READ", "UPDATE", "SCAN", "INSERT", "READMODIFYWRITE"]
            keys = [f'{k.lower()}_latency' for k in ops]
            okeys += keys
            okeys += ['workload']
            parsed['op_cycles'] = 0
            parsed['op_cnt'] = 0

            all_zero = True

            for op, k in zip(ops, keys):
                if op in data_obj:
                    parsed[k] = int(data_obj[op]['cycles']) / max(int(data_obj[op]['cnt']), 1.0)
                    if parsed[k] != 0.0:
                        all_zero = False

                    parsed['op_cycles'] += int(data_obj[op]['cycles'])
                    parsed['op_cnt'] += int(data_obj[op]['cnt'])
            
            if all_zero:
                return None, None

            parsed['op_latency'] = parsed['op_cycles'] / max(parsed['op_cnt'], 1.0)

        if 'throughput' in parsed:
            parsed['throughput_mbs'] = parsed['throughput'] / (1024 ** 2)
            parsed['throughput_kbs'] = parsed['throughput'] / (1024)

        # embed()
        return okeys, parsed

    def _normalize_fields(self, dfs):
        #fields = ['cache_accesses', 'llc_misses', 'llc_accesses',
        #        'kernfs_cache_accesses', 'kernfs_llc_misses',
        #        'kernfs_llc_accesses', 'tlb_misses']
        #fields = ['cache_accesses']
        fields = []
        minimums = {}
        for config, config_data in dfs.items():
            for layout, layout_data in config_data.items():
                for field in fields:
                    if field not in layout_data:
                        continue
                    minimum = layout_data[field].min()
                    if minimum == 0:
                        continue
                    if field not in minimums:
                        minimums[field] = minimum
                    else:
                        cur = minimums[field]
                        minimums[field] = min(minimum, cur)

        for config, config_data in dfs.items():
            for layout, layout_data in config_data.items():
                for field, m in minimums.items():
                    dfs[config][layout][field] /= m

        pprint(minimums)

        return dfs

    def _parse_results(self, results_dir):
        from scipy.stats.mstats import gmean
        data = defaultdict(list)
        seen = defaultdict(lambda: 1)
        files = [f for f in results_dir.iterdir() if f.is_file() and 'summary' not in f.name]
        groupby_list = None
        for fp in files:
            with fp.open() as f:
                objs = json.load(f)
                for obj in objs:
                    keys, parsed_data = self._parse_relevant_fields(obj)
                    key_str = json.dumps(keys)
                    # embed()
                    if groupby_list is None:
                        groupby_list = keys
                    if parsed_data is not None:
                        data[parsed_data['trial num']] += [parsed_data]
                        seen[key_str] += 1

        df_list = []
        for _, d in data.items():
            # embed()
            # df_list += [pd.DataFrame(d).reset_index(drop=True)]
            df_list += [pd.DataFrame(d)]

        df_combined = pd.concat(df_list).reset_index(drop=True)
        
        df_list = []
        # Trim outliers:
        # embed()
        if 'indexing' in df_combined.columns:
            for g, idx in df_combined.groupby(groupby_list).groups.items():
                gdf = df_combined.iloc[idx]
                z_scores = np.nan_to_num(stats.zscore(gdf['indexing']))
                abs_z_scores = np.abs(z_scores)
                filtered_df = gdf.loc[abs_z_scores < 2.5]
                # embed()
                # print(g)
                # if 'RADIX_TREES' in g and '1' in g and 'Insert' in g:
                #     print('STAHP')
                #     embed()
                # if len(filtered_df) != len(gdf):
                #     print('hello!')
                #     embed()
                #     exit(-1)
                df_list += [filtered_df]
            df_combined = pd.concat(df_list).reset_index(drop=True)

        # dfg = df_combined.groupby(df_combined.index)
        dfg = df_combined.groupby(groupby_list)
        df_mean = dfg.mean()
        ntrials = len(data)
        df_ci = ((1.96 * dfg.std()) / np.sqrt(ntrials))
        # print('passed!')
        # embed()
        # df_ci = ((1.645 * dfg.std(ddof=0)) / np.sqrt(ntrials))
        # df_ci = ((1.96 * dfg.std(ddof=0)) / (dfg.count() ** (1/2)))

        pprint(df_ci)
        pprint(df_mean)
        pprint(df_ci / df_mean)
        
        if 'indexing' in df_mean.columns:
            df_mean = df_mean[df_mean.indexing != 0]
            df_mean = df_mean[df_mean.indexing.notna()]

            df_ci = df_ci[df_ci.indexing != 0]
            df_ci = df_ci[df_ci.indexing.notna()]

        ci_columns = { c: f'{c}_ci' for c in df_ci.columns }

        df_ci = df_ci.rename(columns=ci_columns)

        # df_mean['indexing'] /= df_mean['indexing'].min()
        # df_mean['read_data'] /= df_mean['read_data'].min()
        # df_mean['total_breakdown'] /= df_mean['total_breakdown'].min()

        if 'total_breakdown' in df_mean.columns:
            df_mean['total_raw_tsc'] = df_mean['total_breakdown']

        if 'read_data' in df_mean.columns:
            df_mean['read_data_raw'] = df_mean['read_data']
            df_ci['read_data_raw_ci'] = df_ci['read_data_ci']

            df_mean['read_data_per_op'] = df_mean['read_data'] / df_mean['nops']
            df_ci['read_data_per_op_ci'] = df_ci['read_data_ci'] / df_mean['nops']

            df_mean['indexing_per_op'] = df_mean['indexing'] / df_mean['nops']
            df_ci['indexing_per_op_ci'] = df_ci['indexing_ci'] / df_mean['nops']

        if 'reps' in df_mean:
            df_mean['read_data_per_rep'] = df_mean['read_data'] / df_mean['reps']
            df_ci['read_data_per_rep_ci'] = df_ci['read_data_ci'] / df_mean['reps']

            df_mean['indexing_per_rep'] = df_mean['indexing'] / df_mean['reps']
            df_ci['indexing_per_rep_ci'] = df_ci['indexing_ci'] / df_mean['reps']

        if 'indexing' in df_mean:
            df_mean['indexing_raw'] = df_mean['indexing'] / df_mean['indexing'].min()
            df_ci['indexing_raw_ci'] = df_ci['indexing_ci'] / df_mean['indexing'].min()

        if 'op_latency' in df_mean:
            df_mean['io_path_indexing_per_op'] = df_mean['indexing'] / df_mean['op_cnt']

        if 'indexing' in df_mean:
            df_mean['indexing'] /= df_mean['total_breakdown']
            df_mean['read_data'] /= df_mean['total_breakdown']
            df_ci['indexing_ci'] /= df_mean['total_breakdown']
            df_ci['read_data_ci'] /= df_mean['total_breakdown']

            df_mean['indexing_throughput'] = df_mean['indexing'] * df_mean['throughput']
            df_mean['read_data_throughput'] = df_mean['read_data'] * df_mean['throughput']

            df_ci['io_cycles_ci'] = df_ci['total_breakdown_ci'] / df_mean['nops']
            df_mean['io_cycles'] = df_mean['total_breakdown'] / df_mean['nops']

        if 'reps' in df_mean:
            df_ci['io_cycles_rep_ci'] = df_ci['total_breakdown_ci'] / df_mean['reps']
            df_mean['io_cycles_rep'] = df_mean['total_breakdown'] / df_mean['reps']

        if 'op_latency' in df_mean:
            df_mean['app_logic_per_op'] = ((df_mean['op_latency'] * df_mean['op_cnt']) - df_mean['total_breakdown']) / df_mean['op_cnt']
            df_mean['io_path_per_op'] = df_mean['total_breakdown'] / df_mean['op_cnt']

        if 'reps' in df_mean:
            df_ci['io_cycles_rep_ci'] = df_ci['total_breakdown_ci'] / df_mean['reps']
            df_mean['io_cycles_rep'] = df_mean['total_breakdown'] / df_mean['reps']

        if 'op_latency' in df_mean:
            df_mean['io_path'] = df_mean['io_cycles'] / df_mean['op_latency']
            df_ci['io_path_ci'] = df_ci['io_cycles_ci'] / df_ci['op_latency_ci']
        
        if 'total_breakdown' in df_mean:
            df_ci['total_breakdown_ci'] /= df_mean['total_breakdown']
            df_mean['total_breakdown'] /= df_mean['total_breakdown']

        # self.df = df_list[0].reindex(df_mean.index) # to preserve string fields
        # self.df[df_mean.columns] = df_mean
        # self.df = pd.concat([self.df, df_ci], axis=1)
        self.df = df_mean.reset_index()
        df_ci = df_ci.reset_index()
        # embed()q
        self.df = pd.concat([self.df, df_ci], axis=1)
        # embed()

        # hashfs = self.df[self.df.struct == 'HASHFS'].copy()
        # if not hashfs.empty:
        #     for layout in self.df.layout.unique().tolist():
        #         if layout == 1.0:
        #             continue
        #         if not hashfs[hashfs.layout == layout].empty:
        #             hashfs = hashfs[~(hashfs.layout == layout)]
                
        #         l1 = hashfs[hashfs.layout == 1.0]
        #         if l1.empty:
        #             break
        #         assert not l1.empty
        #         l2 = l1.copy()
        #         l2.layout = layout
        #         self.df = self.df.append(l2)

        # Reset index to reset the row numbers or whatnot
        # self.df = self.df.reset_index(drop=True)

    def _load_results_file(self, file_path):
        with file_path.open() as f:
            self.df = pd.DataFrame.from_dict(yaml.load(f))

    def __init__(self, results_dir=None, file_path=None):
        assert results_dir is not None or file_path is not None

        if file_path is not None:
            self._load_results_file(file_path)
        elif results_dir is not None:
            self._parse_results(Path(results_dir))

    def save_to_file(self, file_path):
        with file_path.open('w') as f:
            yaml.dump(self.df.to_dict(), f)

    def interact(self):
        df = self.df
        print('DataFrame in df (or self.df)')
        embed()

    def _reorder_data_frames(self):
        new_dict = defaultdict(dict)
        for x, x_data in self.dfs.items():
            for y, y_df in x_data.items():
                new_dict[y][x] = y_df

        return new_dict

    def reorder_data_frames(self, dfs):
        new_dict = defaultdict(dict)
        for x, x_data in dfs.items():
            for y, y_df in x_data.items():
                new_dict[y][x] = y_df

        return new_dict

    def get_dataframe(self):
        return self.df

    def data_by_benchmark(self):
        return self.df.groupby('test')

    def data_by_config(self):
        return self._reorder_data_frames()

    def filter_benchmarks(self, benchmark_filter, dfs):
        new_df_data = {}
        #new_df_data = []
        for config, config_df in dfs.items():
            if isinstance(config_df, pd.DataFrame):
                for c in config_df.columns:
                    if c not in benchmark_filter:
                        config_df.drop(c, axis=1, inplace=True)

                config_df.index = [config]
                new_df_data[config] = config_df
                #new_df_data += [config_df]
            else:
                for c in [c for c in config_df]:
                    if c not in benchmark_filter:
                        config_df.pop(c, None)
                    else:
                        df = config_df[c]
                        df.index = [config]
                        new_df_data[config] = df
                        #new_df_data += [df]

        new_df = None


        new_df_list = list(new_df_data.values())
        if new_df_list:
            new_df = new_df_list[0]
        if len(new_df_list) > 1:
            for df in new_df_list[1:]:
                new_df = new_df.append(df)

        return new_df.T

    def filter_layout_score(self, layout_score, dfs):
        new_dfs = copy.deepcopy(dfs)
        for idx, idx_data in dfs.items():
            for bench, bench_data in idx_data.items():
                print(bench_data)
                if str(layout_score) not in bench_data:
                    new_dfs[idx].pop(bench, None)
                else:
                    new_dfs[idx][bench] = bench_data[str(layout_score)]
            new_dfs[idx] = pd.DataFrame(new_dfs[idx])
        return new_dfs

    def filter_configs(self, config_filter, dfs):
        assert config_filter is not None
        new_dfs = copy.deepcopy(dfs)
        for bench in [k for k in dfs]:
            for config in [x for x in dfs[bench]]:
                matches_any = False
                for c in config_filter:
                    if c == config:
                        matches_any = True
                        break
                if not matches_any:
                    new_dfs[bench].pop(config, None)

        return new_dfs


    def filter_stats(self, stats, dfs):
        if isinstance(stats, str):
            stats = [stats]

        new_dfs = {}
        for bench, bench_data in dfs.items():
            new_dfs[bench] = {}
            compress = False
            for config, config_data in bench_data.items():
                new_df = {}
                for layout, layout_data in config_data.items():
                    for stat in stats:
                        if stat not in layout_data:
                            continue
                    filtered = [layout_data[s] for s in stats if s in layout_data]
                    if not filtered:
                        continue
                    new_df[layout] = filtered

                if len(new_df):
                    try:
                        new_dfs[bench][config] = pd.DataFrame(new_df)
                        new_dfs[bench][config].index = stats
                    except:
                        compress = True
                        new_dfs[bench][config] = pd.Series(new_df)

            if compress:
                new_dfs[bench] = pd.DataFrame(new_dfs[bench])

        return new_dfs

    def filter_stat_field(self, field, dfs):
        new_dfs = {}
        for x, x_df in dfs.items():
            if isinstance(x_df, pd.Series):
                return pd.DataFrame(dfs)
            per_x = {}
            for y, y_data in x_df.T.items():
                if field not in y_data:
                    raise Exception('Not available!')
                per_x[y] = y_data[field]

            new_df = pd.Series(per_x)
            new_dfs[x] = new_df

        return pd.DataFrame(new_dfs)

    def average_stats(self, dfs):
        averages = {}
        for x, df in dfs.items():
            averages[x] = df.mean()

        return pd.DataFrame(averages)

    # ------------------------
    # Text functions
    # ------------------------

    @staticmethod
    def make_bench_name_latex_compat(bench):
        from itertools import chain
        if isinstance(bench, float):
            bench = str(int(bench * 100))

        engine = inflect.engine()
        pieces = chain(*[p.split(' ') for p in bench.split('_')])
        new_name = ''
        for piece in pieces:
            new_piece = ''
            if piece.isdigit():
                number = engine.number_to_words(piece)
                number_pieces = chain(*[s.split('-') for s in number.split()])
                for np in number_pieces:
                    new_piece += np.capitalize()
            elif piece[:-1].isdigit():
                # Common for things with size letter at end
                number = engine.number_to_words(piece[:-1])
                number_pieces = chain(*[s.split('-') for s in number.split()])
                for np in number_pieces:
                    new_piece += np.capitalize()
                new_piece += piece[-1].capitalize()
            else:
                new_piece = piece.capitalize()

            new_name += new_piece

        # Now try numbers
        import re
        nums = re.findall(r'\d+', new_name)
        if nums:
            new_name = new_name.upper()
            for num in nums:
                new_name = new_name.replace(num, engine.number_to_words(num).capitalize())

        return new_name

    def _mtcc_average(self, agg, output, struct):
        dfs = copy.deepcopy(self.dfs)
        for bench in self.dfs:
            if '_threads' not in bench:
                dfs.pop(bench, None)

        if not dfs:
            return

        layout_data = {}
        for n_threads, idx_res in dfs.items():
            idx_data = {}
            for idx_struct, series in idx_res.items():
                idx_data[idx_struct] = series.T.loc['0.8']['total_time']

            layout_data[n_threads] = pd.Series(idx_data)

        dfs = pd.DataFrame(layout_data)

        baseline = 'extent_trees'
        # MTCC average improvement
        base_res = dfs.T[baseline]
        improvement = (dfs.sub(base_res).div(base_res) * -1).mean(axis=1)

        agg['MTCCReadfile']['MaxTimeImprovement'] = improvement.max()
        output['MTCCReadfile']['MaxTimeImprovement'] = '{:.0%}'.format(improvement.max())
        struct['MTCCReadfile']['MaxTimeImprovement'] = improvement.idxmax()

    def summary(self, baseline, output_file_path, command_prefix, is_final):
        outfile = None
        if output_file_path is not None:
            outfile = Path(output_file_path)

        agg = {}
        agg_output = {}
        agg_struct = {}

        agg_bench = defaultdict(dict)
        agg_bench_output = defaultdict(dict)
        agg_bench_struct = defaultdict(dict)


        def update_max(key, val, val_str, reason):
            if key not in agg or val > agg[key]:
                agg[key] = val
                agg_output[key] = val_str
                agg_struct[key] = reason
                return True
            return False

        def update_max_bench(bench, key, val, val_str, reason):
            if key not in agg_bench[bench] or val > agg_bench[bench][key]:
                agg_bench[bench][key] = val
                agg_bench_output[bench][key] = val_str
                agg_bench_struct[bench][key] = reason
                return True
            return False

        def do_cache_thing(l, col, val, baseline):
            nice_val = self.make_bench_name_latex_compat(val)
            nl = self.make_bench_name_latex_compat(l)
            
            val_df = self.df[self.df[col] == val]
            
            if f'l{l}_hits' in val_df:
                l1_hit_ratio = val_df[f'l{l}_hits'].mean()
                update_max(f'AvgL{nl}Hits{nice_val}', l1_hit_ratio,
                        '{:.0%}'.format(l1_hit_ratio), val)

                if baseline is not None:
                    baseline_df = self.df[self.df[col] == baseline.upper()]
                    rel_l1_accesses = val_df[f'l{l}_accesses'].mean() / baseline_df[f'l{l}_accesses'].mean()
                    update_max(f'L{nl}Ratio{nice_val}', rel_l1_accesses,
                        '{:.0%}'.format(rel_l1_accesses), val)

        def do_l1_thing(col, val, baseline=None):
            do_cache_thing('1', col, val, baseline)

        def do_llc_thing(col, val, baseline=None):
            do_cache_thing('lc', col, val, baseline)

        for idx in self.df.struct.unique():
            do_l1_thing('struct', idx, baseline=baseline)



        # Avg indexing time per op by rep
        if 'repetitions' in self.df.columns:
            idx_df = self.df.groupby('repetitions').mean().indexing_per_op
            idx_norm = idx_df / idx_df.min()
            update_max('IndexingRatioCold', idx_norm[0], 
                    f'{idx_norm[0]:.1f}\\myx', '')


        for layout in self.df.layout.unique():
            do_llc_thing('layout', layout)

        for bench in self.df.test.unique():
            bench_data = self.df[self.df.test == bench]
            baseline_df = bench_data[bench_data.struct == baseline.upper()]
            
            # MaxFragmentationOverhead
            # frag_max, frag_min = None, None
            # if 'throughput' in baseline_df:
            #     frag_min = baseline_df['throughput'].min()
            #     frag_max = baseline_df['throughput'].max()
            # else:
            #     frag_min = 1.0 / baseline_df['total_time'].max()
            #     frag_max = 1.0 / baseline_df['total_time'].min()

            # frag_diff = (frag_max - frag_min) / frag_max
            # update_max('MaxFragmentationOverhead', frag_diff,
            #         '{:.0%}'.format(frag_diff), '%s %s' % (baseline, bench))
            # update_max_bench(bench, 'MaxFragmentationOverhead', frag_diff,
            #         '{:.0%}'.format(frag_diff), '%s %s' % (baseline, bench))

            for idx in bench_data.struct.unique():
                df = bench_data[bench_data.struct == idx]
                nice_idx = self.make_bench_name_latex_compat(idx)
                # MaxIndexingOverhead
                if 'indexing' in df:
                    indexing = df['indexing'].max()
                    update_max('MaxIndexingOverhead', indexing,
                            '{:.0%}'.format(indexing), '%s %s' % (idx, bench))
                    update_max_bench(bench, 'MaxIndexingOverhead', indexing,
                            '{:.0%}'.format(indexing), '%s %s' % (idx, bench))

                    # AvgIndexingOverhead (per idx)
                    avg_indexing = df['indexing'].mean()
                    update_max_bench(bench, f'AvgIndexingOverhead{nice_idx}', avg_indexing,
                            '{:.0%}'.format(avg_indexing), '%s %s' % (idx, bench))

                    # Min
                    min_indexing = df['indexing'].min()
                    update_max(f'MinIndexingOverhead{nice_idx}', min_indexing,
                            '{:.0%}'.format(min_indexing), '%s %s' % (idx, bench))
                    update_max_bench(bench, f'MinIndexingOverhead{nice_idx}', min_indexing,
                            '{:.0%}'.format(min_indexing), '%s %s' % (idx, bench))
                
                if 'l1_hits' in df:
                    l1_hit_ratio = df['l1_hits'].mean()
                    rel_l1_accesses = df['l1_accesses'].mean() / baseline_df['l1_accesses'].mean()

                    update_max_bench(bench, f'AvgLOneHits{nice_idx}', l1_hit_ratio,
                            '{:.0%}'.format(l1_hit_ratio), '%s %s' % (idx, bench))

                    update_max_bench(bench, f'LOneRatio{nice_idx}', rel_l1_accesses,
                            '{:.0%}'.format(rel_l1_accesses), '%s %s' % (idx, bench))

                # MaxIndexingOverheadReduction
                for layout in df.layout.unique():
                    explanation = '%s %s @ %s' % (idx, bench, layout)

                    key = 'MaxThroughputImprovement{}LS{}'.format(
                            self.make_bench_name_latex_compat(idx),
                            self.make_bench_name_latex_compat(
                                '%.0f' % (float(layout) * 100) ))
                    df_perf, base_perf = None, None
                    if 'throughput' in df[df.layout == layout]:
                        df_perf = df[df.layout == layout]['throughput']
                        base_perf = baseline_df[baseline_df.layout == layout]['throughput']
                    else:
                        df_perf = 1.0 / df[df.layout == layout]['total_time']
                        base_perf = 1.0 / baseline_df[baseline_df.layout == layout]['total_time']

                    df_perf = df_perf.reset_index(drop=True)
                    base_perf = base_perf.reset_index(drop=True)

                    agg_bench[bench][key] = ((df_perf - base_perf) / base_perf).max()
                    agg_bench_output[bench][key] = '{:.0%}'.format(agg_bench[bench][key])
                    agg_bench_struct[bench][key] = explanation

                    if 'indexing' not in df[df.layout == layout]:
                        continue

                    base = baseline_df[baseline_df.layout == layout]['indexing'].reset_index(drop=True)
                    curr = df[df.layout == layout]['indexing'].reset_index(drop=True)
                    reduction = (base - curr)
                    diff = float((reduction / base).max())

                    if update_max('MaxIndexingOverheadReduction', diff,
                            '{:.0%}'.format(diff), explanation):
                        key = 'MaxThroughputImprovement'
                        try:
                            df_perf, base_perf = None, None
                            if 'throughput' in df[df.layout == layout]:
                                df_perf = df[df.layout == layout]['throughput']
                                base_perf = baseline_df[baseline_df.layout == layout]['throughput']
                            else:
                                df_perf = 1.0 / df[df.layout == layout]['total_time']
                                base_perf = 1.0 / baseline_df[baseline_df.layout == layout]['total_time']

                            df_perf = df_perf.reset_index(drop=True)
                            base_perf = base_perf.reset_index(drop=True)

                            agg[key] = ((df_perf - base_perf) / base_perf).max()
                            agg_output[key] = '{:.0%}'.format(agg[key])
                            agg_struct[key] = explanation
                        except:
                            pass

                if 'io_cycles' in df:
                    base = baseline_df['io_cycles'].mean()
                    curr = df['io_cycles'].mean()
                    reduction = (base - curr)
                    diff = float(reduction / base)
                    explanation = '%s %s' % (idx, bench)

                    nice_idx = self.make_bench_name_latex_compat(idx)

                    update_max_bench(bench, f'AvgIOCycleReduction{nice_idx}',
                            diff, '{:.0%}'.format(diff), explanation)
                    print(diff)

                # AvgIndexingOverheadReduction (per-benchmark only!)
                if 'indexing' in df:
                    base = baseline_df['indexing'].mean()
                    curr = df['indexing'].mean()
                    reduction = (base - curr)
                    diff = float(reduction / base)
                    explanation = '%s %s' % (idx, bench)

                    nice_idx = self.make_bench_name_latex_compat(idx)

                    key = 'AvgThroughputImprovement{}'.format(nice_idx)
                    df_perf, base_perf = None, None
                    if 'throughput' in df:
                        df_perf = df['throughput']
                        base_perf = baseline_df['throughput']
                    else:
                        df_perf = 1.0 / df['total_time']
                        base_perf = 1.0 / baseline_df['total_time']

                    df_perf = df_perf.reset_index(drop=True)
                    base_perf = base_perf.reset_index(drop=True)

                    agg_bench[bench][key] = ((df_perf - base_perf) / base_perf).mean()
                    agg_bench_output[bench][key] = '{:.0%}'.format(agg_bench[bench][key])
                    agg_bench_struct[bench][key] = explanation

                    key = 'AvgThroughputImprovement'
                    update_max_bench(bench, f'AvgIndexingOverheadReduction{nice_idx}',
                            diff, '{:.0%}'.format(diff), explanation)
                    if update_max_bench(bench, 'AvgIndexingOverheadReduction',
                            diff, '{:.0%}'.format(diff), explanation):
                        try:
                            df_perf, base_perf = None, None
                            if 'throughput' in df:
                                df_perf = df['throughput']
                                base_perf = baseline_df['throughput']
                            else:
                                df_perf = 1.0 / df['total_time']
                                base_perf = 1.0 / baseline_df['total_time']

                            df_perf = df_perf.reset_index(drop=True)
                            base_perf = base_perf.reset_index(drop=True)

                            agg_bench[bench][key] = ((df_perf - base_perf) / base_perf).mean()
                            agg_bench_output[bench][key] = '{:.0%}'.format(agg_bench[bench][key])
                            agg_bench_struct[bench][key] = explanation
                        except:
                            pass

            # Diff across layout scores
            # AvgRangeFragmentation (per benchmark)
            # all_range_fr = {}
            # all_range_bt = {}
            # for idx in bench_data.struct.unique():
            #     df = bench_data[bench_data.struct == idx]
            #     if 'total_time' in df.T.iloc[0]:
            #         all_range_fr[idx] = df.T.iloc[0]['total_time'] - df.T.iloc[-1]['total_time']
            #         all_range_fr[idx] /= df.T.iloc[-1]['total_time']
            #     else:
            #         all_range_fr[idx] = df.T.iloc[-1]['throughput'] - df.T.iloc[0]['throughput']
            #         all_range_fr[idx] /= df.T.iloc[0]['throughput']

            #     k = 'read_data_bytes_per_cycle'
            #     if k in df.T.iloc[0]:
            #         all_range_bt[idx] = df.T.iloc[-1][k] - df.T.iloc[0][k]
            #         all_range_bt[idx] /= df.T.iloc[-1][k]

            # fr_series = pd.Series(all_range_fr)
            # bt_series = pd.Series(all_range_bt)

            # agg_bench[bench]['AvgRangeFragmentation'] = fr_series.mean()
            # agg_bench_output[bench]['AvgRangeFragmentation'] = '{:.0%}'.format(fr_series.mean())
            # agg_bench_struct[bench]['AvgRangeFragmentation'] = ''

            # if all_range_bt:
            #     agg_bench[bench]['AvgRangeBytesPerTime'] = bt_series.mean()
            #     agg_bench_output[bench]['AvgRangeBytesPerTime'] = '{:.0%}'.format(bt_series.mean())
            #     agg_bench_struct[bench]['AvgRangeBytesPerTime'] = ''

        # self._mtcc_average(agg_bench, agg_bench_output, agg_bench_struct)

        # Output time

        pprint(agg)
        pprint(agg_struct)

        pprint(agg_bench)
        pprint(agg_bench_struct)

        if outfile is not None:
            with outfile.open('w') as f:
                cmd_str = '\\newcommand{{\\{0}{1}}}{{\\tentative{{{2}}}}}'
                bench_cmd_str = '\\newcommand{{\\{0}{1}{2}}}{{\\tentative{{{3}}}}}'
                if is_final:
                    cmd_str = '\\newcommand{{\\{0}{1}}}{{{2}}}'
                    bench_cmd_str = '\\newcommand{{\\{0}{1}{2}}}{{{3}}}'

                for cmd in sorted(agg_output.keys()):
                    data = agg_output[cmd]

                    cmd_output = cmd_str.format(command_prefix, cmd, data)
                    cmd_output = cmd_output.replace('%', '\\%')
                    f.write(cmd_output)
                    f.write('\n')

                for bench in sorted(agg_bench_output.keys()):
                    bench_data = agg_bench_output[bench]
                    bname = self.make_bench_name_latex_compat(bench)

                    for cmd in sorted(bench_data.keys()):
                        data = bench_data[cmd]

                        cmd_output = bench_cmd_str.format(command_prefix, cmd, bname, data)
                        cmd_output = cmd_output.replace('%', '\\%')
                        f.write(cmd_output)
                        f.write('% {}\n'.format(agg_bench_struct[bench][cmd]))

    def page_cache_test_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        df = self.df[self.df['repetitions'] != '1']

        # Overall IO portion indexing
        x = df.groupby(['struct', 'test']).mean()

        avg_improvement = x.indexing_per_op / x.io_cycles

        for test, ratio in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            res[f'PCAvg{struct_name}{test_name}IndexingRatio'] = f'{ratio:.0%}'
       
        # Overall IO cycle reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x.loc['EXTENT_TREES'] / x.loc['NONE']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            test_name = self.make_bench_name_latex_compat(test)
            res[f'PCAvg{test_name}IOCycleDecrease'] = f'{improvement:.0%}'

        # Overall difference in repetitions
        x = df[df['test'] != 'Insert'].groupby(['struct', 'repetitions']).mean()

        avg_io_cycle_improvement = 1.0 - (x.loc['EXTENT_TREES'] / x.loc['NONE']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            test_name = self.make_bench_name_latex_compat(test)
            res[f'PCAvg{test_name}ForReadsIOCycleDecrease'] = f'{improvement:.0%}'

        x = df[df['test'] == 'Insert'].groupby(['struct', 'repetitions']).mean()

        avg_io_cycle_improvement = 1.0 - (x.loc['EXTENT_TREES'] / x.loc['NONE']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            test_name = self.make_bench_name_latex_compat(test)
            res[f'PCAvg{test_name}ForWritesIOCycleDecrease'] = f'{improvement:.0%}'

        # Overall difference in bytes read and written

        x = df.groupby(['struct']).mean()
        nvdimm = 1.0 - (x.loc['EXTENT_TREES'] / x.loc['NONE'])
        res[f'PCAvgBytesReadDecrease'] = f'{nvdimm.nvdimm_bytes_read:.0%}'
        res[f'PCAvgBytesWrittenDecrease'] = f'{nvdimm.nvdimm_bytes_written:.0%}'

        pprint(res)

        return res

    def ycsb_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        x = self.df.groupby(['struct']).mean()

        avg_improvement = 1.0 - (x / x.loc['NONE']).op_latency

        for struct, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'YCSBAvg{struct_name}Latency{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)
        print()

        avg_improvement = 1.0 - (x / x.loc['NONE']).throughput

        for struct, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'YCSBAvg{struct_name}Throughput{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)
        print()

        # Max improvement
        x = self.df.groupby(['struct', 'test']).mean()

        avg_improvement = 1.0 - (x / x.loc['NONE']).op_latency
        for struct in x.index.get_level_values(0).unique().to_list():
            struct_name = self.make_bench_name_latex_compat(struct)

            struct_improvement = avg_improvement[struct]
            improvement = struct_improvement.max()

            verb = 'Decrease' if improvement > 0 else 'Increase'

            key = f'YCSBMax{struct_name}Latency{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmax())
            else:
                print('\t', key, val, struct_improvement.idxmax())


            improvement = struct_improvement.min()

            verb = 'Decrease' if improvement > 0 else 'Increase'

            key = f'YCSBMin{struct_name}Latency{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmin())
            else:
                print('\t', key, val, struct_improvement.idxmin())

            print()

        # Max IO path stats
        avg_improvement = (x.io_path_per_op / x.op_latency)
        for struct in x.index.get_level_values(0).unique().to_list():
            struct_name = self.make_bench_name_latex_compat(struct)

            struct_improvement = avg_improvement[struct]
            improvement = struct_improvement.max()

            key = f'YCSBMax{struct_name}IOPathPortion' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmax())
            else:
                print('\t', key, val, struct_improvement.idxmax())

        print()

        avg_improvement = (x.io_path_indexing_per_op / x.op_latency)
        for struct in x.index.get_level_values(0).unique().to_list():
            struct_name = self.make_bench_name_latex_compat(struct)

            struct_improvement = avg_improvement[struct]
            improvement = struct_improvement.max()

            key = f'YCSBMax{struct_name}IndexingPortion' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmax())
            else:
                print('\t', key, val, struct_improvement.idxmax())

        print()

        avg_improvement = (x.io_path_indexing_per_op / x.io_path_per_op)
        for struct in x.index.get_level_values(0).unique().to_list():
            struct_name = self.make_bench_name_latex_compat(struct)

            struct_improvement = avg_improvement[struct]
            improvement = struct_improvement.max()

            key = f'YCSBMax{struct_name}IndexingOfIOPortion' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmax())
            else:
                print('\t', key, val, struct_improvement.idxmax())

        print()

        # Throughput stats
        avg_improvement = (x / x.loc['NONE']).throughput - 1.0
        for struct in x.index.get_level_values(0).unique().to_list():
            struct_name = self.make_bench_name_latex_compat(struct)

            struct_improvement = avg_improvement[struct]
            improvement = struct_improvement.max()

            verb = 'Increase' if improvement > 0 else 'Decrease'

            key = f'YCSBMax{struct_name}Throughput{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmax())
            else:
                print('\t', key, val, struct_improvement.idxmax())


            improvement = struct_improvement.min()

            verb = 'Increase' if improvement > 0 else 'Decrease'

            key = f'YCSBMin{struct_name}Throughput{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val, struct_improvement.idxmin())
            else:
                print('\t', key, val, struct_improvement.idxmin())

            print()

        #


        for io_size in ['SmallIO', 'LargeIO']:

            df = self.df[self.df['test'].str.contains(io_size.upper())]

            # Overall improvment
            x = df.groupby(['struct']).mean()

            avg_improvement = 1.0 - (x / x.loc['NONE']).op_latency

            for struct, improvement in avg_improvement.to_dict().items():
                struct_name = self.make_bench_name_latex_compat(struct)
                verb = 'Decrease' if improvement > 0 else 'Increase'
                key = f'YCSBAvg{struct_name}{io_size}Latency{verb}' 
                val = f'{abs(improvement):.0%}'
                res[key] = val
                if improvement > 0:
                    print(key, val)
                else:
                    print('\t', key, val)

            # Max improvement
            x = df.groupby(['struct', 'test']).mean()

            avg_improvement = 1.0 - (x / x.loc['NONE']).op_latency
            for struct in x.index.get_level_values(0).unique().to_list():
                struct_name = self.make_bench_name_latex_compat(struct)

                struct_improvement = avg_improvement[struct]
                improvement = struct_improvement.max()

                verb = 'Decrease' if improvement > 0 else 'Increase'

                key = f'YCSBMax{struct_name}{io_size}Latency{verb}' 
                val = f'{abs(improvement):.0%}'
                res[key] = val
                if improvement > 0:
                    print(key, val, struct_improvement.idxmax())
                else:
                    print('\t', key, val, struct_improvement.idxmax())

                improvement = struct_improvement.min()

                verb = 'Decrease' if improvement > 0 else 'Increase'

                key = f'YCSBMin{struct_name}{io_size}Latency{verb}' 
                val = f'{abs(improvement):.0%}'
                res[key] = val
                if improvement > 0:
                    print(key, val, struct_improvement.idxmin())
                else:
                    print('\t', key, val, struct_improvement.idxmin())

            print()


        pprint(res)

        return res

    def concurrency_test_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        df = self.df
       
        '''
        # Overall IO cycle reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'CCAvg{struct_name}{test_name}IOCycle{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by layout 
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'CCAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall IO cycle reduction by size
        x = df.groupby(['struct', 'display start size', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            size_name = self.make_bench_name_latex_compat(test[1])
            test_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'CCAvg{struct_name}{test_name}{size_name}IOCycle{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()
        '''

        x = df.groupby(['threads']).mean()

        avg_improvement = 1.0 - (x / x.loc[2.0]).write_throughput_mb

        for thread, improvement in avg_improvement.to_dict().items():
            thread_num = self.make_bench_name_latex_compat(str(round(thread)))
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'CCAvg{thread_num}ThreadWriteThroughput{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()


        pprint(res)

        return res

    def large_io_test_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        df = self.df[self.df['repetitions'] != '1']
       
        # Overall IO cycle reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles_rep

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvg{struct_name}{test_name}IOCycle{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by layout 
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles_rep

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'LIAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Increase in read data time
        x = df.groupby(['struct']).mean()

        avg_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).read_data_per_rep

        for struct, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvg{struct_name}ReadData{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val 
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        x = df[df['struct'] != 'HASHFS'].groupby(['layout']).mean()

        avg_improvement = 1.0 - (x / x.loc[1.0]).read_data_per_rep

        for layout, improvement in avg_improvement.to_dict().items():
            layout_name = self.make_bench_name_latex_compat(layout)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgLS{layout_name}ReadData{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val 
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by size
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles_rep

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'LIAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall difference in bytes read and written

        x = df[df['test'] != 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_read.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesRead{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        for struct, improvement in nvdimm.nvdimm_read_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesReadRatio{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        x = df[df['test'] == 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_written.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesWritten{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        for struct, improvement in nvdimm.nvdimm_write_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesWrittenRatio{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        pprint(res)

        return res

    def large_io_test_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        df = self.df[self.df['repetitions'] != '1']
       
        # Overall IO cycle reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles_rep

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvg{struct_name}{test_name}IOCycle{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by layout 
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles_rep

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'LIAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Increase in read data time
        x = df.groupby(['struct']).mean()

        avg_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).read_data_per_rep

        for struct, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvg{struct_name}ReadData{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val 
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        x = df[df['struct'] != 'HASHFS'].groupby(['layout']).mean()

        avg_improvement = 1.0 - (x / x.loc[1.0]).read_data_per_rep

        for layout, improvement in avg_improvement.to_dict().items():
            layout_name = self.make_bench_name_latex_compat(layout)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgLS{layout_name}ReadData{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val 
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by size
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles_rep

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'LIAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall difference in bytes read and written

        x = df[df['test'] != 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_read.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesRead{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        for struct, improvement in nvdimm.nvdimm_read_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesReadRatio{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        x = df[df['test'] == 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_written.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesWritten{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        for struct, improvement in nvdimm.nvdimm_write_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'LIAvgBytesWrittenRatio{struct_name}{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        pprint(res)

        return res

    def multi_file_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        df = self.df[self.df['repetitions'] != '1']
       
        # Overall IO cycle reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'MFAvg{struct_name}{test_name}IOCycle{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Cache reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).l1_accesses

        for test, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'MFAvg{struct_name}{test_name}LOneAccesses{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        avg_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).l1_misses

        for test, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'MFAvg{struct_name}{test_name}LOneMisses{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by layout 
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'MFAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall IO cycle reduction by size
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'MFAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall difference in bytes read and written

        x = df[df['test'] != 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_read.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'MFAvgBytesRead{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        for struct, improvement in nvdimm.nvdimm_read_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'MFAvgBytesReadRatio{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        x = df[df['test'] == 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_written.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'MFAvgBytesWritten{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        for struct, improvement in nvdimm.nvdimm_write_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'MFAvgBytesWrittenRatio{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        pprint(res)

        return res


    def single_block_summary(self):
        ''' Returns <latex command name> -> <value> '''

        res = {}

        df = self.df[self.df['repetitions'] != '1']
       
        # Overall IO cycle reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'SBAvg{struct_name}{test_name}IOCycle{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction relative to level hashing
        x = df.groupby(['struct', 'test']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['LEVEL_HASH_TABLES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'SBAvg{struct_name}{test_name}IOCycleOverLevel{verb}' 
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Cache reduction
        x = df.groupby(['struct', 'test']).mean()

        avg_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).l1_accesses

        for test, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'SBAvg{struct_name}{test_name}LOneAccesses{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        avg_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).l1_misses

        for test, improvement in avg_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            key = f'SBAvg{struct_name}{test_name}LOneMisses{verb}'
            val = f'{abs(improvement):.0%}'
            res[key] = val
            if improvement > 0:
                print(key, val)
            else:
                print('\t', key, val)

        print()

        # Overall IO cycle reduction by layout 
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'SBAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall IO cycle reduction by size
        x = df.groupby(['struct', 'test', 'layout']).mean()

        avg_io_cycle_improvement = 1.0 - (x / x.loc['EXTENT_TREES']).io_cycles

        for test, improvement in avg_io_cycle_improvement.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(test[0])
            test_name = self.make_bench_name_latex_compat(test[1])
            layout_name = self.make_bench_name_latex_compat(test[2])
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'SBAvg{struct_name}{test_name}{layout_name}IOCycle{verb}'] = f'{abs(improvement):.0%}'

        # Overall difference in bytes read and written

        x = df[df['test'] != 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_read.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'SBAvgBytesRead{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        for struct, improvement in nvdimm.nvdimm_read_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'SBAvgBytesReadRatio{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        x = df[df['test'] == 'Insert'].groupby(['struct']).mean()
        nvdimm = 1.0 - (x / x.loc['EXTENT_TREES'])
        for struct, improvement in nvdimm.nvdimm_bytes_written.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'SBAvgBytesWritten{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        for struct, improvement in nvdimm.nvdimm_write_hit_ratio.to_dict().items():
            struct_name = self.make_bench_name_latex_compat(struct)
            verb = 'Decrease' if improvement > 0 else 'Increase'
            res[f'SBAvgBytesWrittenRatio{struct_name}{verb}'] = f'{abs(improvement):.0%}'

        pprint(res)

        return res

    def summarize(self, func_name, output_file_path, is_final):
        outfile = None
        if output_file_path is not None:
            outfile = Path(output_file_path)

        res = getattr(self, func_name)()

        if outfile is not None:
            with outfile.open('w') as f:
                cmd_str = '\\newcommand{{\\{0}}}{{\\tentative{{{1}}}}}'
                if is_final:
                    cmd_str = '\\newcommand{{\\{0}}}{{{1}}}'

                for cmd in sorted(res.keys()):
                    data = res[cmd]

                    cmd_output = cmd_str.format(cmd, data)
                    cmd_output = cmd_output.replace('%', '\\%')
                    f.write(cmd_output)
                    f.write('\n')

