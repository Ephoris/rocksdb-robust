#!/usr/bin/env python
import logging
import sys

from experiments.database import RocksDBWrapper
from experiments.size_ratio_cost import SizeRatioCost
from experiments.bpe_cost import BPECost

config = {
    "db_path" : "/scratchNVM5/ndhuynh/tmp",
    "T" : 10,
    "K" : 9,
    "Z" : 9,
    "B" : 1048576 * 8,
    "E" : 1024,
    "bpe" : 9.0,
    "L" : 3,
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
    tiering = False

    log = init_logger()
    log.info('Welcome to experiment runner')

    # jobs = [SizeRatioCost, BPECost]
    jobs = [BPECost]
    for job_obj in jobs:
        job = job_obj(config)
        log.info("Running %s", job.name())
        job.run(tiering)

    return 0


if __name__ == '__main__':
    main()
