import os
from dotenv import dotenv_values


PROJECT_ENV = dict(dotenv_values())
SYSTEM_ENV = os.environ

DFMT = '%Y-%m-%d'
TFMT = '%H:%M:%S'
TFMT_MS = TFMT + '.%f'
DTFMT = DFMT + ' ' + TFMT
DTFMT_MS = DFMT + ' ' + TFMT_MS
