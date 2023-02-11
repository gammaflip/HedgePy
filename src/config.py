import os
from dotenv import dotenv_values

PROJECT_ENV = dict(dotenv_values())
SYSTEM_ENV = os.environ