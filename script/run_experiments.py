#!/usr/bin/env python
import logging
import sys

from experiments.database import RocksDBWrapper
from experiments.size_ratio_cost import SizeRatioCost

config = {
    "db_path" : "/Users/ndhuynh/sandbox/tmp",
    "T" : 2,
    "K" : 1,
    "Z" : 1,
    "B" : 1048576 / 2,
    "E" : 1024,
    "bpe" : 5.0,
    "N" : 100000,
    "destroy" : True
}


def init_logger():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='[%(levelname)s][%(asctime)s.%(msecs)04d][%(filename)s] : %(message)s',
        datefmt='%H:%M:%S'
    )

    exp_log = logging.getLogger('exp_logger')

    return exp_log


def main():
    log = init_logger()
    log.info('Welcome to experiment runner')

    # db = RocksDBWrapper(**config)
    # (w, z1, z0) = db.run_workload(100, 0, 0)
    # print(w, z1, z0)

    log.info('Running Size Ratio Experiment')
    job = SizeRatioCost(config)
    job.run(tiering=False)

    return 0


if __name__ == '__main__':
    main()
