import os
import re
import subprocess
import sys

CREATE_DB_PATH = "../build/db_builder"
EXECUTE_DB_PATH = "../build/db_runner"

class RocksDBWrapper(object):

    def __init__(self, db_path, T, K, Z, B, E, bpe, N, destroy=True):
        self.db_path = db_path
        self.T = T  # Size ratio
        self.K = K  # Lower level size ratio
        self.Z = Z  # Largest level size ratio
        self.B = B  # Buffer size (bytes)
        self.E = E  # Entry size (bytes)
        self.bpe = bpe  # Bits per entry for bloom filter
        self.N = N  # Number of entries
        self.destroy = destroy  # destroy DB is exist in path

        self.time_prog = re.compile(r'\[[0-9:.]+\] \[info\] \(w, z1, z0\) : \((\d+), (\d+), (\d+)\)')
        self._create_db()

    def _create_db(self):
        cmd = [
            CREATE_DB_PATH,
            self.db_path,
            '-d' if self.destroy else '',
            '-T {}'.format(self.T),
            '-K {}'.format(self.K),
            '-Z {}'.format(self.Z),
            '-B {}'.format(self.B),
            '-E {}'.format(self.E),
            '-b {}'.format(self.bpe),
            '-N {}'.format(self.N),
        ]
        cmd = ' '.join(cmd)

        completed_process = subprocess.run(cmd, capture_output=True, universal_newlines=True, shell=True)

        return completed_process.stdout

    def run_workload(self, reads, empty_reads, writes):
        cmd = [
            EXECUTE_DB_PATH,
            self.db_path,
            '-e {}'.format(empty_reads),
            '-r {}'.format(reads),
            '-w {}'.format(writes),
        ]
        cmd = ' '.join(cmd)

        completed_process = subprocess.run(cmd, capture_output=True, universal_newlines=True, shell=True)
        time_results = self.time_prog.search(completed_process.stdout)

        return (int(result) for result in time_results.groups())