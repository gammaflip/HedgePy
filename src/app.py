import os
import argparse
from multiprocessing import current_process, Process, Queue, Pipe
from api import api
from gui import gui


MAIN_PROC: Process = current_process()
API_PROC: Process
GUI_PROC: Process


def parse_cli_args():
    parser = argparse.ArgumentParser(prog='HedgePy')
    parser.add_argument('user', nargs=1, default='postgres')
    parser.add_argument('password', nargs=1)
    parser.add_argument('-n', '--nogui')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_cli_args()

