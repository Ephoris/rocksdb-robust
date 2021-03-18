import copy
import logging
import os
import subprocess
import sys

import numpy as np
import pandas as pd

from experiments.database import RocksDBWrapper

EMPTY_READS = 5000000
VALID_READS = 5000000
WRITES = 0
RUNS = 5

class BPECost(object):

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('exp_logger')

    def name(self):
        return "BPE Cost"

    def run(self, tiering=False):

        local_cfg = copy.deepcopy(self.config)
        bits = list(range(1, 11))
        local_cfg['L'] = 3

        time_results = []

        for bpe in bits:
            self.log.info('Bits per Element %d', bpe)
            local_cfg['bpe'] = bpe

            result = {}
            result['T'] = local_cfg['T']
            result['K'] = local_cfg['K']
            result['Z'] = local_cfg['Z']
            result["B"] = local_cfg['B']
            result["E"] = local_cfg['E']
            result["bpe"] = bpe
            result["L"] = local_cfg['L']
            result['num_writes'] = WRITES
            result['num_non_empty_reads'] = VALID_READS
            result['num_empty_reads'] = EMPTY_READS

            for run in range(RUNS):
                db = RocksDBWrapper(**local_cfg)
                (write_time, valid_read_time, empty_read_time) = db.run_workload(VALID_READS, EMPTY_READS, WRITES)

                result['write_time_' + str(run)] = write_time
                result['valid_read_time_' + str(run)] = valid_read_time
                result['empty_read_time_' + str(run)] = empty_read_time

                self.log.info('Run %d : Time (w, z1, z0) : (%d, %d, %d)', run + 1, write_time, valid_read_time, empty_read_time)
                del db

            result['write_time'] = np.average([result['write_time_' + str(run)] for run in range(RUNS)])
            result['valid_read_time'] = np.average([result['valid_read_time_' + str(run)] for run in range(RUNS)])
            result['empty_read_time'] = np.average([result['empty_read_time_' + str(run)] for run in range(RUNS)])
            self.log.info('Averages : Time (w, z1, z0) : (%d, %d, %d)', result['write_time'], result['valid_read_time'], result['empty_read_time'])
            time_results.append(result)

        df = pd.DataFrame(time_results)
        df.to_csv('size_ratio_cost.csv', index=False)
