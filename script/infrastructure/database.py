import logging
import os
import re
import subprocess
import sys

CREATE_DB_PATH = "../build/db_builder"
EXECUTE_DB_PATH = "../build/db_runner"
THREADS = 8

class RocksDBWrapper(object):

    def __init__(self, db_path, T, K, Z, B, E, bpe, L, destroy=True):
        self.db_path = db_path
        self.T = T  # Size ratio
        self.K = K  # Lower level size ratio
        self.Z = Z  # Largest level size ratio
        self.B = B  # Buffer size (bytes)
        self.E = E  # Entry size (bytes)
        self.bpe = bpe  # Bits per entry for bloom filter
        self.L = L  # Number of levels
        self.destroy = destroy  # destroy DB is exist in path
        self.log = logging.getLogger('exp_logger')

        self.time_prog = re.compile(r'\[[0-9:.]+\] \[info\] \(w, z1, z0\) : \((-?\d+), (-?\d+), (-?\d+)\)')
        self._create_db()

    def _create_db(self):
        cmd = [
            CREATE_DB_PATH,
            self.db_path,
            '-d',
            '-T {}'.format(self.T),
            '-K {}'.format(self.K),
            '-Z {}'.format(self.Z),
            '-B {}'.format(int(self.B)),
            '-E {}'.format(self.E),
            '-b {}'.format(self.bpe),
            '-L {}'.format(self.L),
            '--parallelism {}'.format(THREADS),
        ]
        cmd = ' '.join(cmd)
        self.log.info("Creating DB...")
        # self.log.info(cmd)

        completed_process = subprocess.Popen(
            cmd,
            # stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            shell=True,
        ).communicate()[0]

        return completed_process

    def run_workload(self, reads, empty_reads, writes, prime=1000000):
        cmd = [
            EXECUTE_DB_PATH,
            self.db_path,
            '-e {}'.format(empty_reads),
            '-r {}'.format(reads),
            '-w {}'.format(writes),
            f'-p {prime}',
            '--parallelism {}'.format(THREADS),
        ]
        cmd = ' '.join(cmd)
        self.log.info("Running workload...")
        # self.log.info(cmd)

        completed_process = subprocess.Popen(
            cmd,
            # stdin=None,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            shell=True,
        ).communicate()[0]

        time_results = self.time_prog.search(completed_process)

        return (int(result) for result in time_results.groups())
