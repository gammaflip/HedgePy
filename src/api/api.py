from src.api import query
from src.api.bases.Data import Query, CopyQuery, Result
from src.api.bases.Database import Database, Schema, Table, Column, Profile, Connection, Session
from src.api.bases.Vendor import (
    ResourceMap,
)
from src.api import vendors
from src.api.bases.Data import Data
from src import config
from psycopg import Cursor
from psycopg.errors import UniqueViolation
from psycopg.types.json import Json
from typing import Optional, Callable, Any
from pathlib import Path


DEFAULT_ROW_FACTORY = query.tuple_row
VENDOR_DIR = ResourceMap(vendors)


def initialize(dbname: str, dbuser: str, dbpass: str, **dbkwargs) -> tuple[Profile, Session, Connection]:
    profile = Profile(user=dbuser, dbname=dbname, kwargs=dbkwargs)
    session = Session(profile=profile)
    conn = session.new_connection(profile=profile, password=dbpass)
    return profile, session, conn


def execute_db_query(qry: Query | CopyQuery, conn: Connection, commit: bool = True) -> Any:
    with conn.handle.cursor(row_factory=DEFAULT_ROW_FACTORY) as cur:
        try:
            if isinstance(qry, Query):
                cur.execute(**qry.to_cursor)
            elif isinstance(qry, CopyQuery):
                with cur.copy(qry.to_cursor) as copy:
                    copy.write(qry.values)

        except Exception as e:
            print(f'Exception: {e}')
            conn.handle.rollback()
            return Result(result=e)

        if commit:
            conn.handle.commit()

        if qry.returns:
            res = cur.fetchall()
            res = Data(fields=qry.returns, records=res)
            return Result(result=res)
        else:
            return Result(result=None)


def execute_db_script(script: str, conn: Connection):
    filepath = Path(config.PROJECT_ENV['ROOT']) / 'src' / 'api' / 'sql' / f'{script}.sql'
    assert filepath.exists(), f'File does not exist: {filepath}'
    q = Query(open(filepath).read())
    res = execute_db_query(q, conn)
    return res


def execute_vendor_query(vendor: str, endpoint: str, *args, **kwargs):
    return VENDOR_DIR[vendor][endpoint](*args, **kwargs)


def register_endpoints(conn: Connection, rm: ResourceMap):
    for vendor in rm:
        q = query.insert_row(schema="meta", table="vendors", columns=("vendor",), row=(vendor.name,))

        try:
            execute_db_query(q, conn)
        except UniqueViolation:
            pass

        for endpoint in vendor:
            q = query.insert_row(
                schema="meta",
                table="endpoints",
                columns=("endpoint", "vendor", "signature"),
                row=(endpoint.name, vendor.name, Json(endpoint.map))
            )

            try:
                execute_db_query(q, conn)
            except UniqueViolation:
                pass
