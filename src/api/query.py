from psycopg.cursor import Cursor
from psycopg.sql import SQL, Identifier
from psycopg.rows import RowMaker, tuple_row, dict_row, class_row, args_row, kwargs_row
from pandas import Series
from numpy import array
from enum import Enum
from typing import Sequence, Optional, Any, Literal
from src.api.bases.Data import Query, Result
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


def insert_row(schema: str, table: str, columns: tuple[str], row: tuple) -> Query:
    # placeholders = SQL(", ").join([SQL("%({}s)").format(SQLLiteral(col)) for col in columns])
    # placeholders = SQL(", ").join([SQL("%({})s").format(col) for col in columns])
    placeholders = SQL(", ").join([SQL("(%s)") for col in columns])
    body = SQL("""INSERT INTO {schema}.{table} ({col_names}) VALUES ({placeholders});""").format(
        schema=Identifier(schema),
        table=Identifier(table),
        col_names=SQL(", ").join(map(Identifier, columns)),
        placeholders=placeholders)

    return Query(body=body, values=row)


def insert_rows(
    schema: str, table: str, columns: tuple[str], rows: tuple[tuple]
) -> tuple[Query, tuple[tuple]]:
    body = SQL("""COPY {schema}.{table} ({col_names}) FROM STDIN;""").format(
        schema=Identifier(schema),
        table=Identifier(table),
        col_names=SQL(", ").join([Identifier(_) for _ in columns]),
    )

    return Query(body=body), rows


"""
POSTGRESQL CRUD BINDINGS
"""


def create_database(name: str, tablespace: Optional[str] = None) -> Query:
    if tablespace:
        body = SQL("""CREATE DATABASE {name} WITH TABLESPACE {tablespace};""").format(
            name=Identifier(name), tablespace=Identifier(tablespace)
        )

        return Query(body=body)

    else:
        body = SQL("""CREATE DATABASE {name};""").format(name=Identifier(name))

        return Query(body=body)


def create_schema(name: str) -> Query:
    body = SQL("""CREATE SCHEMA {name};""").format(name=Identifier(name))

    return Query(body=body)


def create_table(name: str, schema: str) -> Query:
    body = SQL("""CREATE TABLE {schema}.{name}();""").format(
        schema=Identifier(schema), name=Identifier(name)
    )

    return Query(body=body)


def create_column(
    name: str, table: str, dtype: str, schema: str, default: Optional[Any] = None
) -> Query:
    if default:
        body = SQL(
            """ALTER TABLE {schema}.{table} ADD COLUMN {name} {dtype} DEFAULT %s;"""
        ).format(
            schema=Identifier(schema),
            table=Identifier(table),
            name=Identifier(name),
            dtype=Identifier(dtype),
        )

        return Query(body=body, values=(default,))

    else:
        body = SQL(
            """ALTER TABLE {schema}.{table} ADD COLUMN {name} {dtype};"""
        ).format(
            table=Identifier(table), name=Identifier(name), dtype=Identifier(dtype)
        )

        return Query(body=body)


def create_index(
    table: str, col_name: tuple[str], schema: Optional[str] = None
) -> Query:
    body = SQL("""CREATE INDEX {name} ON {schema}.{table} ({column});""")

    name = "_".join([table, *col_name])
    return Query(
        body=body.format(
            name=Identifier(name),
            schema=Identifier(schema),
            table=Identifier(table),
            column=SQL(", ").join([Identifier(_) for _ in col_name]),
        )
    )


def read_database(tablespace: Optional[str] = None) -> Query:
    if tablespace:
        body = SQL(
            """SELECT datname FROM pg_catalog.pg_database WHERE dattablespace=%s;"""
        )

        return Query(body=body, values=(tablespace,))

    else:
        body = SQL("""SELECT datname FROM pg_catalog.pg_database;""")

        return Query(body=body)


def read_schema(database: str) -> Query:
    body = SQL(
        """SELECT schema_name FROM information_schema.schemata WHERE catalog_name=%s;"""
    )

    return Query(body=body, values=(database,))


def read_table(database: str, schema: str) -> Query:
    body = SQL(
        """SELECT table_name FROM information_schema.tables WHERE table_catalog=%s AND table_schema=%s;"""
    )

    return Query(body=body, values=(database, schema))


def read_column(
    database: str,
    schema: str,
    table: str,
) -> Query:
    body = SQL(
        """
    SELECT column_name, data_type, column_default FROM information_schema.columns 
    WHERE table_catalog=%s AND table_schema=%s AND table_name=%s;
    """
    )

    return Query(body=body, values=(database, schema, table))


def read_index(name, table: str, schema: str) -> Query:
    body = SQL(
        """
    SELECT * FROM pg_catalog.pg_indexes 
    WHERE indexname=%s AND tablename=%s AND schemaname=%s;
    """
    )

    return Query(body=body, values=(name, table, schema))


def update_database(
    name: str, new_tablespace: Optional[str] = None, new_name: Optional[str] = None
) -> Query:
    if new_tablespace:
        body = SQL("""ALTER DATABASE {name} SET TABLESPACE {new_tablespace};""").format(
            name=Identifier(name), new_tablespace=Identifier(new_tablespace)
        )

        return Query(body=body)

    elif new_name:
        body = SQL("""ALTER DATABASE {name} RENAME TO {new_name};""").format(
            name=Identifier(name), new_name=Identifier(new_name)
        )

        return Query(body=body)

    else:
        raise ValueError("Either new_tablespace or new_name must be specified.")


def update_schema(name: str, new_name: str) -> Query:
    body = SQL("""ALTER SCHEMA {name} RENAME TO {new_name};""").format(
        name=Identifier(name), new_name=Identifier(new_name)
    )

    return Query(body=body)


def update_table(
    col_name: tuple[str], col_values: bytes, name: str, schema: str
) -> Query:
    body = SQL("""COPY {schema}.{name} ({col_name}) FROM STDIN""").format(
        schema=Identifier(schema),
        name=Identifier(name),
        col_name=SQL(", ").join([Identifier(_) for _ in col_name]),
    )

    return Query(body=body, values=col_values)


def update_column(
    name: str,
    table: str,
    schema: Optional[str] = None,
    new_name: Optional[str] = None,
    new_type: Optional[str] = None,
    new_default: Optional[str] = None,
) -> Query:
    if new_name:
        body = SQL(
            """ALTER TABLE {schema}.{table} RENAME COLUMN {name} TO {new_name};"""
        ).format(
            schema=Identifier(schema),
            table=Identifier(table),
            name=Identifier(name),
            new_name=Identifier(new_name),
        )

        return Query(body=body)

    elif new_type:
        body = SQL(
            """ALTER TABLE {schema}.{table} ALTER COLUMN {name} TYPE {new_type};"""
        ).format(
            schema=Identifier(schema),
            table=Identifier(table),
            name=Identifier(name),
            new_type=Identifier(new_type),
        )

        return Query(body=body)

    elif new_default:
        body = SQL(
            """ALTER TABLE {schema}.{table} ALTER COLUMN {name} SET DEFAULT %s;"""
        ).format(
            schema=Identifier(schema), table=Identifier(table), name=Identifier(name)
        )

        return Query(body=body, values=(new_default,))

    raise ValueError("Either new_name, new_type, or new_default must be specified.")


def update_index(name: str, new_name: str, schema: str) -> Query:
    body = SQL("""ALTER INDEX {schema}.{name} RENAME TO {new_name};""").format(
        schema=Identifier(schema), name=Identifier(name), new_name=Identifier(new_name)
    )

    return Query(body=body)


def delete_database(name: str) -> Query:
    body = SQL("""DROP DATABASE {name};""").format(name=Identifier(name))

    return Query(body=body)


def delete_schema(name: str) -> Query:
    body = SQL("""DROP SCHEMA {name};""").format(name=Identifier(name))

    return Query(body=body)


def delete_table(name: str, schema: str) -> Query:
    body = SQL("""DROP TABLE {schema}.{name};""").format(
        schema=Identifier(schema), name=Identifier(name)
    )

    return Query(body=body)


def delete_column(name: str, table: str, schema: Optional[str] = None) -> Query:
    body = SQL("""ALTER TABLE {schema}.{table} DROP COLUMN {name};""").format(
        schema=Identifier(schema), table=Identifier(table), name=Identifier(name)
    )

    return Query(body=body)


def delete_index(name: str, schema: Optional[str] = None) -> Query:
    body = SQL("""DROP INDEX {schema}.{name};""").format(
        schema=Identifier(schema), name=Identifier(name)
    )

    return Query(body=body)


"""
QUERY FUNCTION
"""


def query(
    action: Literal["create", "read", "update", "delete"],
    obj: Database | Schema | Table | Column | Index,
    *args,
    **kwargs
) -> Any:
    try:
        f = globals()["_".join([action, obj.__name__.lower()])]
    except KeyError:
        raise NotImplementedError

    return f(*args, **kwargs)
