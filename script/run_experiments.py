#!/usr/bin/env python
import logging
import sys

from experiments.bpe_cost import BPECost
from experiments.size_ratio_cost import SizeRatioCost
from experiments.write_exp import WriteCost
from experiments.read_exp import ReadCost

config = {
    'db_path' : '/scratch/ndhuynh/db',
    'T' : 10,
    'K' : 9,
    'Z' : 9,
    'B' : 1048576 * 8,
    'E' : 1024,
    'bpe' : 8.0,
    'L' : 3,
    'destroy' : True
}

experiments = [
    # BPECost,
    # SizeRatioCost,
    # WriteCost,
    ReadCost,
]


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
    tiering = False

    log = init_logger()
    log.info('Welcome to experiment runner')
    for experiment in experiments:
        job = experiment(config)
        log.info(f'Running job {job.name()}')
        job.run(tiering=tiering)

    return 0


if __name__ == '__main__':
    main()
