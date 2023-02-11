import base64
import psycopg
from psycopg_pool import ConnectionPool
from src import config
from src.api.bases import SQLUser


PASSWORD = bytes('password', encoding='utf-8')  # NOTE: strictly for dev purposes; to be replaced with gui input
USER = SQLUser('postgres')
POOL = ConnectionPool(USER.url('m1lom1lo'))
