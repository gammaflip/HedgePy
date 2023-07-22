import os
from dotenv import dotenv_values


PROJECT_ENV = dict(dotenv_values())
SYSTEM_ENV = os.environ
PATH = {
    'root': 'P:',
    'skema': 'P:/skema',
    'db': 'P:/db',
    'gui_assets': 'B:/dev/git/HedgePy/src/gui/assets'
}
