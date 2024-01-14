from src import config
from src.api import query
from src.api import vendors
from src.api.bases.Data import Data, Result
from src.api.bases.Database import Query, CopyQuery
from src.api.bases.Database import Database, Schema, Table, Column, Profile, Connection, Session
from src.api.bases.Vendor import ResourceMap
from psycopg.errors import UniqueViolation
from psycopg.types.json import Json
from pathlib import Path


DEFAULT_ROW_FACTORY = query.tuple_row
VENDOR_DIR = ResourceMap(vendors)


def initialize(dbname: str, dbuser: str, dbpass: str, **dbkwargs) -> tuple[Profile, Session, Connection]:
    profile = Profile(user=dbuser, dbname=dbname, kwargs=dbkwargs)
    session = Session(profile=profile)
    conn = session.new_connection(profile=profile, password=dbpass)
    return profile, session, conn


def make_db_query(qry: str, **kwargs) -> Query | CopyQuery:
    f = getattr(query, qry)
    return f(**kwargs)


def execute_db_query(qry: Query | CopyQuery, conn: Connection, commit: bool = True) -> Result:
    with conn.cursor(row_factory=DEFAULT_ROW_FACTORY) as cur:
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


def execute_db_script(script: str, conn: Connection) -> Result:
    filepath = Path(config.PROJECT_ENV['ROOT']) / 'src' / 'api' / 'sql' / f'{script}.sql'
    qry = Query(body=open(filepath).read())
    res = execute_db_query(qry=qry, conn=conn)
    return res


def execute_vendor_query(vendor: str, endpoint: str, **kwargs) -> Result:
    endpoint = VENDOR_DIR[vendor][endpoint]
    res = endpoint(**kwargs)
    return Result(result=res)


def register_endpoints(conn: Connection):
    for vendor in VENDOR_DIR:
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
