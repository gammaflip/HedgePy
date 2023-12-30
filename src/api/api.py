from src.api.session import Profile, Session, Connection
from src.api.query import RowFactories, query, insert_row, insert_rows
from src.api.bases.Data import Query, Result
from src.api.bases.Database import Database, Schema, Table, Column, Index
from src.api.bases.Vendor import (
    Vendor,
    Endpoint,
    Getter,
    Formatter,
    Authorization,
    ResourceMap,
)
from src.api import vendors
from src.api.bases.Data import Data, Field, Symbol
from src import config
from psycopg.errors import UniqueViolation, ProgrammingError
from psycopg.types.json import Json
from typing import Optional, Callable, Any
from pandas import Timestamp
from json import dumps
from pathlib import Path


DEFAULT_ROW_FACTORY = RowFactories.tuple_row
VENDOR_DIR = ResourceMap(vendors)


def initialize(
    dbname: str, dbuser: str, dbpass: str, **dbkwargs
) -> tuple[Profile, Session, Connection]:
    profile = Profile(user=dbuser, dbname=dbname, kwargs=dbkwargs)
    session = Session(profile=profile)
    conn = session.new_connection(profile=profile, password=dbpass)
    return profile, session, conn


def execute_db_query(
    query: Query, conn: Connection, row_factory: Optional[Callable] = None, commit: bool = True
) -> Any:
    if not row_factory:
        row_factory = DEFAULT_ROW_FACTORY

    with conn.handle.cursor(row_factory=row_factory) as cur:
        try:
            cur.execute(*query.prepared)
        except Exception as e:
            print(f'Exception: {e}')
            conn.handle.rollback()
            return Result(e)

        if commit:
            conn.handle.commit()

        if query.returns:
            res = cur.fetchall()
            res = Data(query.returns, res)
            return Result(res)
        else:
            return Result(None)


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
        q = insert_row(schema="meta", table="vendors", columns=("vendor",), row=(vendor.name,))

        try:
            execute_db_query(q, conn)
        except UniqueViolation:
            pass

        for endpoint in vendor:
            q = insert_row(
                schema="meta",
                table="endpoints",
                columns=("endpoint", "vendor", "signature"),
                row=(endpoint.name, vendor.name, Json(endpoint.map))
            )

            try:
                execute_db_query(q, conn)
            except UniqueViolation:
                pass


def db_snapshot(conn: Connection) -> dict:  # TODO: REFACTOR WITH UPDATED execute_db_query FUNCTION
    db_exclude = ("postgres", "template0", "template1")
    schema_exclude = ("public", "information_schema", "pg_catalog", "pg_toast")
    rf = RowFactories.value_row

    db_query = query("read", Database)
    db_tup = tuple(
        [
            Database(db)
            for db in execute_db_query(db_query, conn, rf)
            if db not in db_exclude
        ]
    )
    res = dict.fromkeys(db_tup)

    for db in db_tup:
        schema_query = query("read", Schema, database=db.name)
        schema_tup = tuple(
            [
                Schema(db, schema)
                for schema in execute_db_query(schema_query, conn, rf)
                if schema not in schema_exclude
            ]
        )
        res[db] = dict.fromkeys(schema_tup)

        for schema in schema_tup:
            table_query = query("read", Table, database=db.name, schema=schema.name)
            table_tup = tuple(
                [
                    Table(schema, table)
                    for table in execute_db_query(table_query, conn, rf)
                ]
            )
            res[db][schema] = dict.fromkeys(table_tup)

            for table in table_tup:
                column_query = query(
                    "read",
                    Column,
                    database=db.name,
                    schema=schema.name,
                    table=table.name,
                )
                column_tup = tuple(
                    [
                        Column(table, *column)
                        for column in execute_db_query(column_query, conn)
                    ]
                )
                res[db][schema][table] = column_tup

    return res



