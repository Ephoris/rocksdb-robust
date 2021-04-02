import copy
import logging

import numpy as np
import pandas as pd

from infrastructure.database import RocksDBWrapper

READ_NUM = 5000000
PRIME_READS = 1000000
RUNS = 1
N = 2 * 20971520 

class ReadCost(object):

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('exp_logger')

    def name(self):
        return "Read Cost"

    def calculate_buffer(self, T, L, N, E):
        return int((N * E) / ((T**L) - 1))

    def run(self, compaction_policy='both'):
        local_cfg = copy.deepcopy(self.config)
        size_ratios    = [2,  5, 10, 15, 20]
        initial_levels = [10, 3, 3,  2, 2]

        if compaction_policy == 'both':
            compactions = ['tiering', 'leveling']
        else:
            compactions = [compaction_policy]

        time_results = []
        for policy in compactions:
            self.log.info(f'Compaction policy: {policy}')
            params = zip(size_ratios, initial_levels)
            for T, L in params:
                B = self.calculate_buffer(T, L, N, local_cfg['E'])
                local_cfg['T'] = T
                local_cfg['K'] = local_cfg['Z'] = T - 1 if policy == 'tiering' else 1
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
                self.log.info(f'DB configured with {T=}, {L=}, B={B >> 20} MB | DB size {(N * local_cfg["E"]) >> 30} GB')

                for run in range(RUNS):
                    db = RocksDBWrapper(**local_cfg)
                    _, valid_read_time, empty_read_time = db.run_workload(READ_NUM, READ_NUM, 0, PRIME_READS)

                    result['valid_read_time_' + str(run)] = valid_read_time
                    result['empty_read_time_' + str(run)] = empty_read_time

                    self.log.info(f'Run {run} : Valid, Empty Read ({valid_read_time}, {empty_read_time}) ms')
                    del db

                result['valid_read_time'] = np.average([result['valid_read_time_' + str(run)] for run in range(RUNS)])
                result['empty_read_time'] = np.average([result['empty_read_time_' + str(run)] for run in range(RUNS)])
                self.log.info(f'Average read times: ({result["valid_read_time"]}, {result["empty_read_time"]}) ms')
                time_results.append(result)
                # Do a periodic write to save progress
                df = pd.DataFrame(time_results)
                df.to_csv('read_cost.csv', index=False)

            df = pd.DataFrame(time_results)
            df.to_csv('read_cost.csv', index=False)
