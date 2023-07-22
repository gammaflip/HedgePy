import os
from importlib.utils import find_spec
from ..HedgePy import src

def import_source(name: str):
    name, pkg = 'src.' + name, '.HedgePy'
    spec = find_spec(name, pkg)
    