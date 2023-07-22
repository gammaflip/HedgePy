import sys
from importlib import import_module


if (_ := 'B:\dev\git\HedgePy\src') not in sys.path:
    sys.path.append(_)


def import_source(name: str) -> object:
    return import_module(name)
