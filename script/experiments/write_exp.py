import copy
import logging

import numpy as np
import pandas as pd

from infrastructure.database import RocksDBWrapper

RUNS = 5

class WriteCost(object):

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('exp_logger')

    def name(self):
        return "Write Cost"

    def find_number_writes(self, Linit, T, B, E):
        write_num = (T - 1) * (T ** (Linit)) * (B / E)

        return int(write_num)

    def run(self, compaction_policy):
        local_cfg = copy.deepcopy(self.config)
        size_ratios    = [2,  5, 10, 15, 20]
        initial_levels = [11, 5, 3,  3,  3]

        if compaction_policy == 'both':
            compactions = ['tiering', 'leveling']
        else:
            compactions = [compaction_policy]

        time_results = []
        for policy in compactions:
            self.log.info(f'Compaction policy: {policy}')
            params = zip(size_ratios, initial_levels)
            for T, L in params:
                local_cfg['T'] = T
                local_cfg['K'] = local_cfg['Z'] = T - 1 if policy == 'tiering' else 1
                local_cfg['L'] = L
                write_num = self.find_number_writes(L, T, local_cfg['B'], local_cfg['E'])

                result = {
                    'T' : T,
                    'L' : L,
                    'K' : local_cfg['K'],
                    'Z' : local_cfg['Z'],
                    'B' : local_cfg['B'],
                    'E' : local_cfg['E'],
                    'bpe' : local_cfg['bpe'],
                    'num_writes' : write_num
                }
                self.log.info(f'DB configured with {T=}, {L=} | Writing to DB {(write_num * local_cfg["E"]) >> 30} GB')

                for run in range(RUNS):
                    db = RocksDBWrapper(**local_cfg)
                    write_time, _, _ = db.run_workload(0, 0, write_num, 1000)
    
                    result['write_time_' + str(run)] = write_time

                    self.log.info('Run %d | Write time: %d ms', run + 1, write_time)
                    del db

                result['write_time'] = np.average([result['write_time_' + str(run)] for run in range(RUNS)])
                self.log.info('Average write time: %d ms', result['write_time'])
                time_results.append(result)
                # Do a periodic write to save progress
                df = pd.DataFrame(time_results)
                df.to_csv('write_cost.csv', index=False)

        df = pd.DataFrame(time_results)
        df.to_csv('write_cost.csv', index=False)
