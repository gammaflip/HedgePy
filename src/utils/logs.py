import os
import sys
import time
import config
import logging
import cProfile
from io import TextIOWrapper
from pandas import Timestamp
from typing import Optional, Literal
from types import NoneType
from multiprocessing import Pool


_DFMT, _TFMT, _DTFMT = '%Y%m%d', '%H%M%S%f', '%Y-%m-%d %H:%M:%S'
_CFMT = '%(asctime)s | %(module)s %(name)s | [%(levelname)s] %(message)s'
_HEADER = 'TIME | MODULENAME LOGGERNAME | [LEVEL] MESSAGE'
_DIR = os.path.join(config.DATA, '_logs')

DEBUG: bool = False  # True enables profiler and sets log level to 10; False disables profiler and sets log level to 20
STREAM: TextIOWrapper = sys.stdout  # override in GUI implementation (applies only to logger)
LOGGER: logging.Logger = None


def _init():
    tstamp = Timestamp.now()
    folder = _make_folder(tstamp)
    return folder, tstamp


def _make_folder(tstamp: Timestamp) -> str:
    folder = os.path.join(_DIR, tstamp.strftime(_DFMT))

    if not os.path.exists(folder):
        os.mkdir(folder)

    return folder


def _make_file(folder: str, tstamp: Timestamp, extension: str = '.log'):
    try:
        assert not os.path.exists(
            file := os.path.join(folder, tstamp.strftime(_TFMT) + extension)
        )
    except AssertionError:
        raise OSError(f'Duplicate {extension[1:]} file: {file}')

    with open(file, 'w+') as f:
        f.write(_HEADER + '/n')

    return file


def _init_logger(folder: str,
                 tstamp: Timestamp,
                 logger_name: str,
                 logger_level: Literal[0, 10, 20, 30, 40, 50]
                 ) -> tuple[logging.Logger, str]:
    fmt = logging.Formatter(fmt=_CFMT, datefmt=_DTFMT)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)

    logger_file = _make_file(folder, tstamp, '.log')

    for handler in [logging.FileHandler(logger_file), logging.StreamHandler(STREAM)]:
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger, logger_file


def _init_profiler(folder: str,
                   tstamp: Timestamp,
                   ) -> tuple[cProfile.Profile, str]:
    profiler = cProfile.Profile(subcalls=True, builtins=True)
    profiler.enable()

    profiler_file = _make_file(folder, tstamp, '.pro')

    return profiler, profiler_file


def _profiler_subroutine(profiler: cProfile.Profile, profiler_file: str, counter: int):
    profiler.dump_stats(profiler_file)
    profiler.clear()

    time.sleep(1)
    counter += 1
    return counter


def start(debug: Optional[bool] = None, scope: Optional[str] = None):
    global LOGGER, DEBUG
    debug = DEBUG if isinstance(debug, NoneType) else debug
    scope = __name__ if not scope else scope
    level = 10 if debug else 20
    folder, tstamp = _init()

    logger, logger_file = _init_logger(folder, tstamp, scope, level)
    LOGGER = logger

    if debug:
        profiler, profiler_file = _init_profiler(folder, tstamp, scope)
        return logger, logger_file, profiler, profiler_file

    else:
        return logger, logger_file
