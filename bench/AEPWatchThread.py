import csv
import io
import pandas as pd
import os
import shlex
import sys
import time
import numpy
from pathlib import Path
from tempfile import NamedTemporaryFile
from subprocess import Popen, DEVNULL, PIPE, STDOUT, TimeoutExpired

class AEPWatchThread:

    AEPWATCH_PATH = Path('/opt/intel/aepwatch/bin/AEPWatch')

    def __init__(self, sample_period, timeout=(10*10), dimm_filter=[0, 1, 2, 3]):
        assert self.AEPWATCH_PATH.exists()
        self.sample_period = sample_period
        self.timeout = timeout
        self.dimm_filter = [f'DIMM{x}' for x in dimm_filter]

    def start(self):
        self.tmpfile = NamedTemporaryFile(suffix='.csv')
        self.tmpfile_path = Path(self.tmpfile.name)

        command_str = (f'{str(self.AEPWATCH_PATH)} {self.sample_period} '
                       f'{self.timeout / self.sample_period} '
                       f'-f {self.tmpfile.name}')

        args = shlex.split(command_str)

        self.proc = Popen(args, stdout=DEVNULL, stderr=DEVNULL)


    def _parse_stats(self):
        csv_rows = []

        with self.tmpfile_path.open() as f:
            for l in f.readlines():
                if '#' in l:
                    continue
                csv_rows += [l.strip()]

        header = csv_rows[0].split(';')
        new_header = []
        for f in header:
            if f:
                new_header += [f]
            else:
                new_header += [new_header[-1]]

        csv_rows[0] = ';'.join(new_header)
        csv_dat = '\n'.join(csv_rows)
        
        return pd.read_csv(io.StringIO(csv_dat), sep=';', header=[0, 1])


    def _average_stats(self, df):
        '''
            Multiple DIMMX columns. Need to average hit ratios by number of
            bytes across all DIMMS.
        '''
        dimms = {}
        for c in numpy.unique(df.columns.droplevel(1).to_list()):
            if c not in self.dimm_filter:
                continue

            df_dimm = df[c]
            assert not df_dimm.empty
            rd_hit_total = (df_dimm['bytes_read (derived)'] * df_dimm['read_hit_ratio (derived)']).sum()
            wr_hit_total = (df_dimm['bytes_written (derived)'] * df_dimm['write_hit_ratio (derived)']).sum()

            rd_bytes_total = df_dimm['bytes_read (derived)'].sum()
            wr_bytes_total = df_dimm['bytes_written (derived)'].sum()

            dimms[c] = {
                'bytes_read': rd_bytes_total,
                'bytes_written': wr_bytes_total,
                'read_hit_total': rd_hit_total,
                'write_hit_total': wr_hit_total,
            }

        combo_df = pd.DataFrame(dimms).T.sum()
        combo_df['read_hit_ratio'] = combo_df['read_hit_total'] / combo_df['bytes_read']
        combo_df['write_hit_ratio'] = combo_df['write_hit_total'] / combo_df['bytes_written']

        # relabel for convenience
        new_index = {l: f'nvdimm_{l}' for l in combo_df.index.to_list()}

        return combo_df.rename(index=new_index)

    def stop(self):
        # This way we capture everything.
        time.sleep(2*self.sample_period)
        self.proc.terminate()
        self.proc.wait()
        assert self.proc.returncode is not None
        # Not sure why this is sometimes negative, but it works...
        
        df = self._parse_stats()
        cdf = self._average_stats(df)
        self.stats_series = cdf
        
        self.tmpfile.close()
        assert not self.tmpfile_path.exists()
        return self.stats()


    def stats(self):
        return self.stats_series

    def stats_dict(self):
        return self.stats_series.to_dict()


if __name__ == '__main__':
    import time
    aep = AEPWatchThread(1, 1)
    aep.start()
    time.sleep(2)
    aep.stop()
    df = aep.stats()
    import IPython
    IPython.embed()
