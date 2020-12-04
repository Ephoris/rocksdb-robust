import copy
import logging
import os
import subprocess
import sys

import pandas as pd

from experiments.database import RocksDBWrapper

EMPTY_READS = 0
VALID_READS = 0
WRITES = 100

class SizeRatioCost(object):

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('exp_logger')

    def name(self):
        return "Size Ratio Cost"

    def run(self, tiering=False):

        local_cfg = copy.deepcopy(self.config)
        size_ratios = [2, 4, 6, 8, 10, 12, 16, 24, 32, 50, 64, 100]
        # size_ratios = list(range(2, 101))

        time_results = []
        for T in size_ratios:
            self.log.info('Running workloads with T = %d', T)
            local_cfg['T'] = T
            local_cfg['K'] = T - 1 if tiering else 1
            local_cfg['Z'] = T - 1 if tiering else 1

            db = RocksDBWrapper(**local_cfg)
            (write_time, valid_read_time, empty_read_time) = db.run_workload(VALID_READS, EMPTY_READS, WRITES)

            time_results.append(
                {
                    'T' : T,
                    'K' : local_cfg['K'],
                    'Z' : local_cfg['Z'],
                    "B" : local_cfg['B'],
                    "E" : local_cfg['E'],
                    "bpe" : local_cfg['bpe'],
                    "N" : local_cfg['N'],
                    'write_time' : write_time,
                    'num_writes' : WRITES,
                    'non_empty_read_time' : valid_read_time,
                    'num_non_empty_reads' : VALID_READS,
                    'empty_read_time' : empty_read_time,
                    'num_empty_reads' : EMPTY_READS,
                }
            )
            self.log.info('Time (w, z1, z0) : (%d, %d, %d)', write_time, valid_read_time, empty_read_time)
            del db

        df = pd.DataFrame(time_results)
        df.to_csv('size_ratio_cost.csv', index=False)

