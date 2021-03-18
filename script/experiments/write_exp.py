import copy
import logging

import numpy as np
import pandas as pd

from experiments.database import RocksDBWrapper

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

    def run(self, tiering=False):
        local_cfg = copy.deepcopy(self.config)
        size_ratios    = [2,  5, 10, 15]
        initial_levels = [11, 4, 3,  2]
        params = zip(size_ratios, initial_levels)

        time_results = []
        for T, L in params:
            self.log.info(f'Running Writes with {T=}, {L=}')
            local_cfg['T'] = T
            local_cfg['K'] = local_cfg['Z'] = T - 1 if tiering else 1
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
            self.log.info(f'Writing {(write_num * local_cfg["E"]) >> 30} GB')

            for run in range(RUNS):
                db = RocksDBWrapper(**local_cfg)
                write_time, _, _ = db.run_workload(0, 0, write_num)
                # write_time = 1

                result['write_time_' + str(run)] = write_time

                self.log.info('Run %d : Write Time %d ms', run + 1, write_time)
                del db

            result['write_time'] = np.average([result['write_time_' + str(run)] for run in range(RUNS)])
            self.log.info('Average Write Time: %d ms', result['write_time'])
            time_results.append(result)

        df = pd.DataFrame(time_results)
        df.to_csv('size_ratio_cost.csv', index=False)
