import json
import asyncio
from src import config
from src.api.bases import IO, Query, Data
from typing import Sequence, Optional
from pathlib import Path
from contextlib import asynccontextmanager


_DB_POOL: Optional[IO.DBPool] = None
_HTTP_POOL: Optional[IO.HTTPPool] = None
TEMPLATE_DIR = Path(config.PROJECT_ENV['ROOT']) / 'src' / 'api' / 'templates'


async def _connect_to_db(credentials: dict) -> None:
    global _DB_POOL
    _DB_POOL = IO.DBPool(credentials)


async def _connect_to_http() -> None:
    global _HTTP_POOL
    _HTTP_POOL = IO.HTTPPool()


async def connect(password: str, **kwargs) -> True:
    credentials = {'password': password}
    credentials.update(**kwargs)
    await asyncio.gather(
        _connect_to_db(credentials),
        _connect_to_http())
    return True


async def db_transaction(request: IO.DBRequest) -> IO.Result:
    return await IO.db_transaction(_DB_POOL, request)


async def pong() -> bool:
    return all((_DB_POOL.alive, _HTTP_POOL.alive))


async def db_snapshot() -> IO.Result:
    query = Query.snapshot()
    return await db_transaction(query)


async def load_template(name: str) -> dict:
    with open(TEMPLATE_DIR / f'{name}.json', 'r') as f:
        return json.load(f)


async def save_template(name: str, template: dict):
    with open(TEMPLATE_DIR / f'{name}.json', 'w') as f:
        json.dump(template, f)


async def load_table(name: str, schema: str, columns: Optional[tuple[Data.Field]] = None) -> Data.Data:
    query = Query.select_values(schema=schema, table=name, columns=columns)
    return await IO.db_transaction(_DB_POOL, query)


# def initialize(dbname: str, dbuser: str, dbpass: str, **dbkwargs) -> tuple[Profile, Session, Connection]:
#     profile = Profile(user=dbuser, dbname=dbname, kwargs=dbkwargs)
#     session = Session(profile=profile)
#     conn = session.new_connection(profile=profile, password=dbpass)
#     return profile, session, conn


# def make_db_query(qry: str, **kwargs) -> Query | CopyQuery:
#     f = getattr(query, qry)
#     return f(**kwargs)


# def execute_db_query(qry: DBRequest | CopyDBRequest, conn: Connection, commit: bool = True) -> Result:
#     with conn.cursor(row_factory=DEFAULT_ROW_FACTORY) as cur:
#         try:
#             if isinstance(qry, DBRequest):
#                 cur.execute(**qry.to_cursor)
#             elif isinstance(qry, CopyDBRequest):
#                 with cur.copy(qry.to_cursor) as copy:
#                     copy.write(qry.values)
#         except Exception as e:
#             print(f'Exception: {e}')
#             conn.handle.rollback()
#             return Result(result=e)
#
#         if commit:
#             conn.handle.commit()
#
#         if qry.returns:
#             res = cur.fetchall()
#             res = Data(fields=qry.returns, records=res)
#             return Result(result=res)
#         else:
#             return Result(result=None)
#
#
# def execute_db_script(script: str, conn: Connection) -> Result:
#     filepath = Path(config.PROJECT_ENV['ROOT']) / 'src' / 'api' / 'sql' / f'{script}.sql'
#     qry = DBRequest(body=open(filepath).read())
#     res = execute_db_query(qry=qry, conn=conn)
#     return res
#
#
# def execute_vendor_query(vendor: str, endpoint: str, **kwargs) -> Result:
#     endpoint = VENDOR_DIR[vendor][endpoint]
#     res = endpoint(**kwargs)
#     return Result(result=res)
#
#
# def register_endpoints(conn: Connection):
#     for vendor in VENDOR_DIR:
#         q = api.bases.Query.insert_row(schema="meta", table="vendors", columns=("vendor",), row=(vendor.name,))
#
#         try:
#             execute_db_query(q, conn)
#         except UniqueViolation:
#             pass
#
#         for endpoint in vendor:
#             q = api.bases.Query.insert_row(
#                 schema="meta",
#                 table="endpoints",
#                 columns=("endpoint", "vendor", "signature"),
#                 row=(endpoint.name, vendor.name, Json(endpoint.map))
#             )
#
#             try:
#                 execute_db_query(q, conn)
#             except UniqueViolation:
#                 pass
