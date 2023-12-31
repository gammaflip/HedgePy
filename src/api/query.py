from psycopg.cursor import Cursor
from psycopg.sql import SQL, Identifier, Literal
from psycopg.rows import RowMaker, tuple_row, dict_row, class_row, args_row, kwargs_row
from pandas import Series
from numpy import array
from enum import Enum
from typing import Sequence, Optional, Any, Literal as StrLiteral
from src.api.bases.Data import Query
from src.api.bases.Database import Database, Schema, Table, Column, Index


"""
PSYCOPG ROW FACTORIES
"""


def np_row(cursor: Cursor) -> RowMaker:
    def row_maker(values: Sequence) -> array:
        return array(values)

    return row_maker


def pd_row(cursor: Cursor) -> RowMaker:
    cols = [c.name for c in cursor.description]

    def row_maker(values: Sequence) -> Series:
        return Series(data=values, index=cols)

    return row_maker


def value_row(cursor: Cursor) -> RowMaker:
    def row_maker(values: Sequence) -> Any:
        assert len(values) == 1
        return values[0]

    return row_maker


class RowFactories(Enum):
    tuple_row = tuple_row
    dict_row = dict_row
    class_row = class_row
    args_row = args_row
    kwargs_row = kwargs_row
    np_row = np_row
    pd_row = pd_row
    value_row = value_row


"""
POSTGRESQL IO BINDINGS
"""


def insert_values(
        schema: str,
        table: str,
        columns: tuple[str],
        values: tuple,
        conditions: Optional[tuple[tuple[str, StrLiteral['=', '>', '<', '>=', '<='], str], ...]] = None
) -> Query:
    assert len(columns) == len(values)
    columns = SQL(', ').join([f'{Identifier(col_name)}=%s' for col_name in columns])

    if conditions:
        conditions = SQL(' AND ').join([f'{Identifier(name)}{op}{Literal(value)}' for name, op, value in conditions])
        body = SQL("""UPDATE {schema}.{table} SET {columns} WHERE {conditions};""").\
            format(schema=schema, table=table, columns=columns, conditions=conditions)

    else:
        body = SQL("""UPDATE {schema}.{table} SET {columns};""").\
            format(schema=schema, table=table, columns=columns)

    return Query(body=body, values=values)


def insert_row(schema: str, table: str, columns: tuple[str], row: tuple) -> Query:
    placeholders = SQL(", ").join([SQL("(%s)") for col in columns])
    body = SQL("""INSERT INTO {schema}.{table} ({col_names}) VALUES ({placeholders});""")\
        .format(schema=Identifier(schema),
                table=Identifier(table),
                col_names=SQL(", ").join(map(Identifier, columns)),
                placeholders=placeholders)
    return Query(body=body, values=row)


def insert_rows(schema: str, table: str, columns: tuple[str], rows: tuple[tuple]) -> tuple[Query, tuple[tuple]]:
    body = SQL("""COPY {schema}.{table} ({col_names}) FROM STDIN;""")\
        .format(schema=Identifier(schema),
                table=Identifier(table),
                col_names=SQL(", ").join([Identifier(_) for _ in columns]),)
    return Query(body=body), rows


"""
POSTGRESQL CLUD BINDINGS
"""


def create_database(name: str) -> Query:
    body = SQL("""CREATE DATABASE {name};""")\
        .format(name=Identifier(name))
    return Query(body=body)


def create_schema(name: str) -> Query:
    body = SQL("""CREATE SCHEMA {name};""")\
        .format(name=Identifier(name))
    return Query(body=body)


def create_table(name: str, schema: str, columns: Optional[tuple[tuple[str, str]]]) -> Query:
    columns = SQL(", ").join([f'{Identifier(cname)} {ctype}' for (cname, ctype) in columns]) if columns else ""
    body = SQL("""CREATE TABLE {schema}.{name}({columns});""")\
        .format(schema=Identifier(schema), name=Identifier(name), columns=columns)
    return Query(body=body)


def create_column(name: str, schema: str, table: str, dtype: str) -> Query:
    body = SQL("""ALTER TABLE {schema}.{table} ADD COLUMN {name} {dtype};""")\
        .format(schema=Identifier(schema), table=Identifier(table), name=Identifier(name), dtype=dtype)
    return Query(body=body)


def create_index(table: str, columns: tuple[str], schema: Optional[str] = None) -> Query:
    ix_name = "_".join([table, *columns])
    columns = SQL(", ").join([Identifier(column) for column in columns])
    body = SQL("""CREATE INDEX {ix_name} ON {schema}.{table} ({columns});""")\
        .format(ix_name=Identifier(ix_name), schema=Identifier(schema), table=Identifier(table), columns=columns)
    return Query(body=body)


def list_database() -> Query:
    body = SQL("""SELECT datname FROM pg_catalog.pg_database;""")
    return Query(body=body, returns=(('name', str),))


def list_schema(database: str) -> Query:
    body = SQL("""SELECT schema_name FROM information_schema.schemata WHERE catalog_name=%s;""")
    return Query(body=body, returns=(('name', str),), values=(database,))


def list_table(database: str, schema: str) -> Query:
    body = SQL("""SELECT table_name FROM information_schema.tables WHERE table_catalog=%s AND table_schema=%s;""")
    return Query(body=body, returns=(('name', str),), values=(database, schema))


def list_column(database: str, schema: str, table: str) -> Query:
    body = SQL(
        """SELECT column_name, data_type, column_default FROM information_schema.columns 
        WHERE table_catalog=%s AND table_schema=%s AND table_name=%s;"""
    )
    return Query(body=body, returns=(('name', str), ('type', str), ('default', str)), values=(database, schema, table))


def list_index(table: str, schema: str) -> Query:
    body = SQL("""SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename=%s AND schemaname=%s;""")
    return Query(body=body, returns=(('name', str),), values=(table, schema))


def update_database(name: str, new_name: Optional[str] = None, new_param: Optional[tuple[str, str]] = None) -> Query:
    if new_name:
        body = SQL("""ALTER DATABASE {name} RENAME TO {new_name};""")\
            .format(name=Identifier(name), new_name=Identifier(new_name))
        return Query(body=body)

    elif new_param:
        param, value = new_param
        body = SQL("""ALTER DATABASE {name} SET {param} TO %s;""")\
            .format(name=Identifier(name), param=Identifier(param))
        return Query(body=body, values=(value,))

    else:
        body = SQL("""ALTER DATABASE {name} RESET ALL;""")\
            .format(name=Identifier(name))
        return Query(body=body)


def update_schema(name: str, new_name: str) -> Query:
    body = SQL("""ALTER SCHEMA {name} RENAME TO {new_name};""")\
        .format(name=Identifier(name), new_name=Identifier(new_name))
    return Query(body=body)


def update_table(
        name: str,
        new_name: Optional[str] = None,
        add_column: Optional[tuple[str, str]] = None,
        drop_column: Optional[str] = None
) -> Query:
    if new_name:
        body = SQL("""ALTER TABLE {name} RENAME TO {new_name};""")\
            .format(name=Identifier(name), new_name=Identifier(new_name))

    elif add_column:
        col_name, col_type = add_column
        body = SQL("""ALTER TABLE {name} ADD COLUMN {col_name} {col_type};""")\
            .format(name=Identifier(name), col_name=Identifier(col_name), col_type=Literal(col_type))

    elif drop_column:
        body = SQL("""ALTER TABLE {name} DROP COLUMN {drop_column} CASCADE;""")\
            .format(name=Identifier(name), drop_column=Identifier(drop_column))

    else:
        raise Exception("Must provide one of 'new_name', 'add_column', or 'drop_column'")

    return Query(body=body)


def update_column(
    name: str,
    table: str,
    schema: str,
    new_name: Optional[str] = None,
    new_type: Optional[str] = None,
    new_default: Optional[str] = None,
) -> Query:
    if new_name:
        body = SQL("""ALTER TABLE {schema}.{table} RENAME COLUMN {name} TO {new_name};""")\
            .format(schema=Identifier(schema),
                    table=Identifier(table),
                    name=Identifier(name),
                    new_name=Identifier(new_name))
        return Query(body=body)

    elif new_type:
        body = SQL("""ALTER TABLE {schema}.{table} ALTER COLUMN {name} TYPE {new_type};""")\
            .format(schema=Identifier(schema),
                    table=Identifier(table),
                    name=Identifier(name),
                    new_type=Identifier(new_type))
        return Query(body=body)

    elif new_default:
        body = SQL("""ALTER TABLE {schema}.{table} ALTER COLUMN {name} SET DEFAULT %s;""")\
            .format(schema=Identifier(schema), table=Identifier(table), name=Identifier(name))
        return Query(body=body, values=(new_default,))

    else:
        raise Exception("Must provide one of 'new_type', 'new_default', or 'new_name'")


def update_index(name: str, new_name: str, schema: str) -> Query:
    body = SQL("""ALTER INDEX {schema}.{name} RENAME TO {new_name};""")\
        .format(schema=Identifier(schema), name=Identifier(name), new_name=Identifier(new_name))
    return Query(body=body)


def delete_database(name: str) -> Query:
    body = SQL("""DROP DATABASE {name} CASCADE;""")\
        .format(name=Identifier(name))
    return Query(body=body)


def delete_schema(name: str) -> Query:
    body = SQL("""DROP SCHEMA {name} CASCADE;""")\
        .format(name=Identifier(name))
    return Query(body=body)


def delete_table(name: str, schema: str) -> Query:
    body = SQL("""DROP TABLE {schema}.{name} CASCADE;""")\
        .format(schema=Identifier(schema), name=Identifier(name))
    return Query(body=body)


def delete_column(name: str, table: str, schema: Optional[str] = None) -> Query:
    body = SQL("""ALTER TABLE {schema}.{table} DROP COLUMN {name} CASCADE;""")\
        .format(schema=Identifier(schema), table=Identifier(table), name=Identifier(name))
    return Query(body=body)


def delete_index(name: str, schema: Optional[str] = None) -> Query:
    body = SQL("""DROP INDEX {schema}.{name} CASCADE;""")\
        .format(schema=Identifier(schema), name=Identifier(name))
    return Query(body=body)


"""
QUERY FUNCTION
"""


def query(
    action: StrLiteral["create", "read", "update", "delete"],
    obj: Database | Schema | Table | Column | Index,
    *args,
    **kwargs
) -> Any:
    try:
        f = globals()["_".join([action, obj.__name__.lower()])]
    except KeyError:
        raise NotImplementedError

    return f(*args, **kwargs)
