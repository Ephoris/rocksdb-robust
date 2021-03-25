import copy
import logging

import numpy as np
import pandas as pd

from infrastructure.database import RocksDBWrapper

READ_NUM = 5000000
PRIME_READS = 1000000
RUNS = 5

class ReadCost(object):

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('exp_logger')

    def name(self):
        return "Read Cost"

    def calculate_buffer(self, T, L, N, E):
        return int((N * E) / ((T**L) - 1))

    def run(self, tiering=False):
        local_cfg = copy.deepcopy(self.config)
        size_ratios    = [2,  5, 10, 15, 20]
        initial_levels = [10, 5, 5,  3, 3]
        params = zip(size_ratios, initial_levels)

        time_results = []
        for T, L in params:
            B = self.calculate_buffer(T, L, local_cfg['N'], local_cfg['E'])
            self.log.info(f'Running Writes with {T=}, {L=}, B={B >> 20} MB')
            local_cfg['T'] = T
            local_cfg['K'] = local_cfg['Z'] = T - 1 if tiering else 1
            local_cfg['L'] = L

            result = {
                'T' : T,
                'L' : L,
                'K' : local_cfg['K'],
                'Z' : local_cfg['Z'],
                'B' : B,
                'E' : local_cfg['E'],
                'bpe' : local_cfg['bpe'],
                'num_reads' : READ_NUM
            }

            for run in range(RUNS):
                db = RocksDBWrapper(**local_cfg)
                _, valid_read_time, empty_read_time = db.run_workload(READ_NUM, READ_NUM, 0, PRIME_READS)

                result['valid_read_time_' + str(run)] = valid_read_time
                result['empty_read_time_' + str(run)] = empty_read_time

                self.log.info(f'Run {run} : Valid, Empty Read ({valid_read_time}, {empty_read_time}) ms')
                del db

            result['valid_read_time'] = np.average([result['valid_read_time_' + str(run)] for run in range(RUNS)])
            result['empty_read_time'] = np.average([result['empty_read_time_' + str(run)] for run in range(RUNS)])
            self.log.info(f'Average Read Times: ({result["valid_read_time"]}, {result["empty_read_time"]}) ms')
            time_results.append(result)

        df = pd.DataFrame(time_results)
        df.to_csv('size_ratio_cost.csv', index=False)
