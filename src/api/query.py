from psycopg.cursor import Cursor
from psycopg.sql import SQL, Identifier, Composed
from psycopg.rows import RowMaker, tuple_row, dict_row, class_row, args_row, kwargs_row
from pandas import Series
from numpy import array
from enum import Enum
from dataclasses import dataclass
from typing import Sequence, Optional, Any, Literal


"""
INTERNAL QUERY CLASS 
"""


@dataclass
class Query:
    body: SQL | Composed
    values: Optional[tuple | bytes] = None
    copy: bool = False

    def prepare(self) -> dict:
        if self.values:
            return {'query': self.body, 'params': self.values}
        else:
            return {'query': self.body}


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


class ROW_FACTORIES(Enum):
    tuple_row = tuple_row
    dict_row = dict_row
    class_row = class_row
    args_row = args_row
    kwargs_row = kwargs_row
    np_row = np_row
    pd_row = pd_row
    value_row = value_row


"""
POSTGRESQL BINDINGS
"""

# CREATE


def create_database(
        name: str,
        tablespace: Optional[str] = None
) -> Query:

    if tablespace:
        body = SQL("""CREATE DATABASE {name} WITH TABLESPACE {tablespace};""")\
            .format(
            name=Identifier(name),
            tablespace=Identifier(tablespace)
        )

        return Query(body=body)

    else:
        body = SQL("""CREATE DATABASE {name};""")\
            .format(
            name=Identifier(name)
        )

        return Query(body=body)


def create_schema(
        name: str
) -> Query:

    body = SQL("""CREATE SCHEMA {name};""")\
        .format(
        name=Identifier(name)
    )

    return Query(body=body)


def create_table(
        name: str,
        schema: Optional[str] = None
) -> Query:

    if schema:
        body = SQL("""CREATE TABLE {schema}.{name}();""")\
            .format(
            schema=Identifier(schema),
            name=Identifier(name)
        )

        return Query(body=body)

    else:
        body = SQL("""CREATE TABLE {name}();""")\
            .format(
            name=Identifier(name)
        )

        return Query(body=body)


def create_column(
        name: str,
        table: str,
        dtype: str,
        default: Optional[Any] = None,
        schema: Optional[str] = None
) -> Query:

    if default and schema:
        body = SQL("""ALTER TABLE {schema}.{table} ADD COLUMN {name} {dtype} DEFAULT %s;""")\
            .format(
            schema=Identifier(schema),
            table=Identifier(table),
            name=Identifier(name),
            dtype=Identifier(dtype)
        )

        return Query(body=body, values=(default,))

    elif schema:
        body = SQL("""ALTER TABLE {schema}.{table} ADD COLUMN {name} {dtype};""")\
            .format(
            schema=Identifier(schema),
            table=Identifier(table),
            name=Identifier(name),
            dtype=Identifier(dtype)
        )

        return Query(body=body)

    elif default:
        body = SQL("""ALTER TABLE {table} ADD COLUMN {name} {dtype} DEFAULT %s;""")\
            .format(
            table=Identifier(table),
            name=Identifier(name),
            dtype=Identifier(dtype)
        )

        return Query(body=body, values=(default,))

    else:
        body = SQL("""ALTER TABLE {table} ADD COLUMN {name} {dtype};""")\
            .format(
            table=Identifier(table),
            name=Identifier(name),
            dtype=Identifier(dtype)
        )

        return Query(body=body)


def create_index(
        table: str,
        column: str | list[str],
        schema: Optional[str] = None
) -> Query:

    if schema:
        body = SQL("""CREATE INDEX {name} ON {schema}.{table} ({column});""")

        if isinstance(column, str):
            name = '_'.join([table, column])
            return Query(body=body.format(
                    name=Identifier(name),
                    schema=Identifier(schema),
                    table=Identifier(table),
                    column=Identifier(column)
                )
            )

        elif isinstance(column, list):
            name = '_'.join([table, *column])
            return Query(
                body=body.format(
                    name=Identifier(name),
                    schema=Identifier(schema),
                    table=Identifier(table),
                    column=SQL(', ').join(column)
                )
            )

    else:
        body = SQL("""CREATE INDEX {name} ON {table} ({column});""")

        if isinstance(column, str):
            name = '_'.join([table, column])
            return Query(
                body=body.format(
                    name=Identifier(name),
                    table=Identifier(table),
                    column=Identifier(column)
                )
            )

        elif isinstance(column, list):
            name = '_'.join([table, *column])
            return Query(
                body=body.format(
                    name=Identifier(name),
                    table=Identifier(table),
                    column=SQL(', ').join(column)
                )
            )


# READ [THESE FUNCTIONS RUN ON OBJECT.__init__(); PERFORMANCE SHOULD BE CONSIDERED]
# DOWNSTREAM RESULT FROM QUERY IS SQLOBJECT'S METADATA


def read_database(
        name: str,
        tablespace: Optional[str] = None
) -> Query:
    if tablespace:
        body = SQL(
            """SELECT * FROM pg_catalog.pg_database WHERE datname=%s AND dattablespace=%s;"""
        )

        return Query(body=body, values=(name, tablespace))

    else:
        body = SQL(
            """SELECT * FROM pg_catalog.pg_database WHERE datname=%s;"""
        )

        return Query(body=body, values=(name,))


def read_schema(
        name: str,
        database: str
) -> Query:
    body = SQL(
        """SELECT * FROM information_schema.schemata WHERE schema_name=%s AND catalog_name=%s;"""
    )

    return Query(body=body, values=(name, database))


def read_table(
        name: str,
        schema: Optional[str] = None
) -> Query:
    if schema:
        body = SQL(
            """SELECT * FROM information_schema.tables WHERE table_name=%s AND table_schema=%s;"""
        )

        return Query(body=body, values=(name, schema))

    else:
        body = SQL(
            """SELECT * FROM information_schema.tables WHERE table_name=%s;"""
        )

        return Query(body=body, values=(name,))


def read_column(
        name,
        table: str,
        schema: Optional[str] = None
) -> Query:
    if schema:
        body = SQL("""
        SELECT * FROM information_schema.columns 
        WHERE column_name=%s AND table_name=%s AND table_schema=%s;
        """)

        return Query(body=body, values=(name, table, schema))

    else:
        body = SQL("""
        SELECT * FROM information_schema.columns 
        WHERE column_name=%s AND table_name=%s;
        """)

        return Query(body=body, values=(name, table))


def read_index(
        name,
        table: str,
        schema: Optional[str] = None
) -> Query:
    if schema:
        body = SQL("""
        SELECT * FROM pg_catalog.pg_indexes 
        WHERE indexname=%s AND tablename=%s AND schemaname=%s;
        """)

        return Query(body=body, values=(name, table, schema))

    else:
        body = SQL("""
        SELECT * FROM pg_catalog.pg_indexes
        WHERE indexname=%s AND tablename=%s;
        """)

        return Query(body=body, values=(name, table))


# UPDATE


def update_database(
        name: str,
        new_tablespace: Optional[str] = None,
        new_name: Optional[str] = None
) -> Query:
    if new_tablespace:
        body = SQL("""ALTER DATABASE {name} SET TABLESPACE {new_tablespace};""")\
            .format(
            name=Identifier(name),
            new_tablespace=Identifier(new_tablespace)
        )

        return Query(body=body)

    elif new_name:
        body = SQL("""ALTER DATABASE {name} RENAME TO {new_name};""")\
            .format(
            name=Identifier(name),
            new_name=Identifier(new_name)
        )

        return Query(body=body)

    else:
        raise ValueError('Either new_tablespace or new_name must be specified.')


def update_schema(
        name: str,
        new_name: str
) -> Query:
    body = SQL("""ALTER SCHEMA {name} RENAME TO {new_name};""")\
        .format(
        name=Identifier(name),
        new_name=Identifier(new_name)
    )

    return Query(body=body)


def update_table(
        col_name: tuple[str],
        col_values: bytes,
        name: str,
        schema: Optional[str] = None,
) -> Query:
    col_name = SQL(', ').join([Identifier(_) for _ in col_name])

    if schema:
        body = SQL("""COPY {schema}.{name} ({col_name}) FROM STDIN""")\
            .format(
            schema=schema,
            name=name,
            col_name=col_name
        )

        return Query(body=body, values=col_values, copy=True)

    else:
        body = SQL("""COPY {name} ({col_name}) FROM STDIN""")\
            .format(
            name=name,
            col_name=col_name
        )

        return Query(body=body, values=col_values, copy=True)


def update_column(
        name: str,
        table: str,
        schema: Optional[str] = None,
        new_name: Optional[str] = None,
        new_type: Optional[str] = None,
        new_default: Optional[str] = None,
) -> Query:
    if schema:
        if new_name:
            body = SQL("""ALTER TABLE {schema}.{table} RENAME COLUMN {name} TO {new_name};""")\
                .format(
                schema=Identifier(schema),
                table=Identifier(table),
                name=Identifier(name),
                new_name=Identifier(new_name)
            )

            return Query(body=body)

        elif new_type:
            body = SQL("""ALTER TABLE {schema}.{table} ALTER COLUMN {name} TYPE {new_type};""")\
                .format(
                schema=Identifier(schema),
                table=Identifier(table),
                name=Identifier(name),
                new_type=Identifier(new_type)
            )

            return Query(body=body)

        elif new_default:
            body = SQL("""ALTER TABLE {schema}.{table} ALTER COLUMN {name} SET DEFAULT %s;""")\
                .format(
                schema=Identifier(schema),
                table=Identifier(table),
                name=Identifier(name)
            )

            return Query(body=body, values=(new_default,))

    else:
        if new_name:
            body = SQL("""ALTER TABLE {table} RENAME COLUMN {name} TO {new_name};""")\
                .format(
                table=Identifier(table),
                name=Identifier(name),
                new_name=Identifier(new_name)
            )

            return Query(body=body)

        elif new_type:
            body = SQL("""ALTER TABLE {table} ALTER COLUMN {name} TYPE {new_type};""")\
                .format(
                table=Identifier(table),
                name=Identifier(name),
                new_type=Identifier(new_type)
            )

            return Query(body=body)

        elif new_default:
            body = SQL("""ALTER TABLE {table} ALTER COLUMN {name} SET DEFAULT %s;""")\
                .format(
                table=Identifier(table),
                name=Identifier(name)
            )

            return Query(body=body, values=(new_default,))

    raise ValueError('Either new_name, new_type, or new_default must be specified.')


def update_index(
        name: str,
        new_name: str,
        schema: Optional[str] = None
) -> Query:
    if schema:
        body = SQL("""ALTER INDEX {schema}.{name} RENAME TO {new_name};""")\
            .format(
            schema=Identifier(schema),
            name=Identifier(name),
            new_name=Identifier(new_name)
        )

    else:
        body = SQL("""ALTER INDEX {name} RENAME TO {new_name};""")\
            .format(
            name=Identifier(name),
            new_name=Identifier(new_name)
        )

    return Query(body=body)


"""
QUERY
"""

ACTIONS = Literal['create', 'read', 'update', 'delete', 'list']
SQL_OBJECT_TYPES = Literal['database', 'schema', 'table', 'column', 'index']


def query(action: ACTIONS, sql_object_type: SQL_OBJECT_TYPES, **kwargs) -> Query:
    fname = action + '_' + sql_object_type
    f = globals().get(fname)
    return f(**kwargs)
