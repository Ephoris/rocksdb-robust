#!/usr/bin/env python
import logging
import sys
import argparse

from experiments.bpe_cost import BPECost
from experiments.size_ratio_cost import SizeRatioCost
from experiments.write_exp import WriteCost
from experiments.read_exp import ReadCost


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'exp', nargs='+',
        choices=['BPECost', 'SizeRatioCost', 'WriteCost', 'ReadCost'],
        default=[],
        help='experiment(s) to run'
    )

    parser.add_argument(
        '-c', '--compaction_policy',
        choices=['tiering', 'leveling', 'both'],
        default='both',
        help='default compaction policy, both will run experiments on both'
    )

    parser.add_argument('-p', '--db_path', default='/tmp/db/', help='path for temporary database')
    parser.add_argument('-T', '--size_ratio', default=10, type=int, help='size ratio')
    parser.add_argument('-K', '--lower_level_max', default=1, type=int, help='lower level number of files')
    parser.add_argument('-Z', '--lowest_level_max', default=1, type=int, help='lowest level number of files')
    parser.add_argument('-B', '--buffer_size', default=8, type=int, help='buffer size in MB')
    parser.add_argument('-E', '--entry_size', default=1024, type=int, help='entry size in bytes')
    parser.add_argument('-b', '--bpe', default=8.0, type=float, help='bits per element per bloom filter')
    parser.add_argument('-L', '--levels', default=3, type=int, help='starting number of levels')

    args = parser.parse_args()
    return args


def config_from_args(args):
    config = {
        'db_path' : args.db_path,
        'T' : args.size_ratio,
        'K' : args.lower_level_max,
        'Z' : args.lowest_level_max,
        'B' : args.buffer_size << 20, # Convert MB to bytes
        'E' : args.entry_size,
        'bpe' : args.bpe,
        'L' : args.levels,
        'destroy' : True
    }

    return config


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
    args = parse_args()
    config = config_from_args(args)

    log = init_logger()
    log.info('Welcome to the experiment runner')

    # if 'BPECost' in args.exp:
    #     job = BPECost(config)
    #     log.info(f'Running job {job.name()}')
    #     job.run(compaction_policy=args.compaction_policy)
    # if 'SizeRatioCost' in args.exp:
    #     job = SizeRatioCost(config)
    #     log.info(f'Running job {job.name()}')
        # job.run(compaction_policy=args.compaction_policy)
    if 'ReadCost' in args.exp:
        job = ReadCost(config)
        log.info(f'Running job {job.name()}')
        job.run(compaction_policy=args.compaction_policy)
    if 'WriteCost' in args.exp:
        job = WriteCost(config)
        log.info(f'Running job {job.name()}')
        job.run(compaction_policy=args.compaction_policy)

    return 0


if __name__ == '__main__':
    main()
