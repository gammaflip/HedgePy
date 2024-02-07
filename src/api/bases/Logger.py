import os
import logging
from logging.handlers import QueueHandler, RotatingFileHandler
import inspect
import datetime
import sys
from pathlib import Path
from typing import Literal, TextIO, Optional
from queue import Queue
from _io import TextIOWrapper

from src import config

ROOT = Path(config.PROJECT_ENV['SERVER_ROOT']) / 'logs'
TODAY = ROOT / datetime.datetime.today().strftime(config.DFMT)

if not ROOT.exists():
    os.mkdir(str(ROOT))


def logger():
    frame = inspect.currentframe().f_back
    ns = frame.f_globals
    func_name = ns['__name__']
    file_name = Path(ns['__file__']).stem
    logger = logging.getLogger(f'{func_name}.{file_name}')
    return logger


HandlerOutputType = Queue | Path | TextIO | TextIOWrapper
OutputLevelType = Literal[0, 1, 2, 3, 4, 5]
ElevateType = tuple[HandlerOutputType, OutputLevelType, Optional[int]]
DEFAULT_FORMAT = '[%(asctime)s] %(name)s: (%(levelname)s) %(message)s'
ERROR_FORMAT = f"{'-'*40}\n"\
               "%(asctime)s\n"\
               "ERROR: [%(levelname)s] %(message)s\n" \
               f"{'-'*40}\n\n"\
               "STACK TRACE:\n"\
               "%(stack_info)"


def handler(
        out: HandlerOutputType,
        lvl: OutputLevelType = 1,
        fmt: str = DEFAULT_FORMAT,
        rotate: Optional[int] = None,
) -> logging.Handler:
    if isinstance(out, Queue):
        hdl = QueueHandler(out)
    elif isinstance(out, Path):
        out = str(out)
        if rotate:
            hdl = RotatingFileHandler(out, maxBytes=rotate, backupCount=1)
        else:
            hdl = logging.FileHandler(out)
    elif isinstance(out, TextIO) or isinstance(out, TextIOWrapper):
        hdl = logging.StreamHandler(out)
    else:
        raise ValueError("Invalid output type")

    hdl.setLevel(logging.getLevelName(lvl * 10))

    fmt = logging.Formatter(fmt)
    hdl.setFormatter(fmt)

    return hdl

