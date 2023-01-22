import pandas as pd
import numpy as np
import psycopg
from psycopg.cursor import Cursor
from psycopg.sql import SQL, Identifier, Composed
from psycopg.rows import RowMaker, tuple_row, dict_row, class_row, args_row, kwargs_row
from dataclasses import dataclass
from typing import Optional, Sequence, Union, Any, Callable, Self
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum


"""
SCHEMAS
"""

class SQLTablespaces(StrEnum):
    PRIMARY = 'pg_default'


@dataclass
class SQLObjectTree: 
    database: str | list | None
    schema: str | list | None
    table: str | list | None
    column: str | list | None
    index: Any | tuple | list | None


"""
ADDITIONAL ROW FACTORIES
"""


def np_row(cursor: Cursor) -> RowMaker:

    def row_maker(values: Sequence) -> np.ndarray:
        return np.array(values)

    return row_maker


def pd_row(cursor: Cursor) -> RowMaker:
    cols = [c.name for c in cursor.description]

    def row_maker(values: Sequence) -> pd.Series:
        return pd.Series(data=values, index=cols)

    return row_maker


def value_row(cursor: Cursor) -> RowMaker:

    def row_maker(values: Sequence) -> Any:
        assert len(values) == 1
        return values[0]

    return row_maker


"""
MIXINS, BASES 
"""

class SQLObject(ABC):

    def __init__(self, name: str, parent: Optional[Self] = None):
        """Args:
            name (str): Corresponds to name in PostgreSQL. 
            parent (Optional[Self], optional): Used to populate tree. 
            Must subclass SQLObject. None only when instantiating 
            Database objects. 
        """
        self._name: str = name
        self._metadata, self._tree = self._resolve_args()

    @abstractmethod
    def _resolve_args(self, name, parent) -> tuple[dict, SQLObjectTree]: 
        """To subclass SQLObject, implement _resolve_args such that it 
        populates self._metadata and self._tree"""
        ...

    @property
    def name(self): return self._name

    @property
    def identifier(self): return Identifier(self.name)

    @property
    def tree(self): return self._tree

    @property
    def metadata(self): return self._metadata


class SQLConnection:
    
    def __init__(self, url: str, autocommit: bool = False):
        self._url = url  # NOTE: this contains password and will be refactored
        self._autocommit = autocommit
        self._endpoint = psycopg.connect

    def __enter__(self): 
        return self._endpoint(self._url, autocommit=self._autocommit)

    def __exit__(self):
        self._endpoint.close()


@dataclass
class SQLUser:
    name: str

    def url(self, password: str):
        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
        return f'postgres://{self.name}:{password}@localhost/'  


class IDBObject(ABC):
    _name: str
    _parent: Any

    def __str__(self): return f'{self.__class__.__name__}: {self.name}'

    def __repr__(self): return self.__str__()

    @property
    def path(self) -> list:
        p = [self]
        parent = self.parent
        while parent is not None:
            p.append(parent)
            parent = parent.parent
        return [_ for _ in reversed(p)] if len(p) > 0 else None

    @property
    def sql_list(self) -> list[Identifier]: return [Identifier(node.name) for node in self.path]

    @property
    def sql_dict(self) -> dict[str, Any]:
        return dict(zip([str.lower(node.__class__.__name__) for node in self.path], self.sql_list))

    @property
    def db_handle(self) -> Any: return self.path[0]

    @property
    @abstractmethod
    def children(self): ...

    @classmethod
    @abstractmethod
    def create(cls, name, parent, *args, **kwargs): ...

    @classmethod
    @abstractmethod
    def read(cls, name, parent, *args, **kwargs): ...

    @abstractmethod
    def update(self, *args, **kwargs): ...

    @abstractmethod
    def delete(self, *args, **kwargs): ...

    @abstractmethod
    def list(self): ...


class Database(IDBObject):

    def __init__(self, name: str):
        self._name = name
        self._parent = None

    @property
    def name(self): return self._dbname

    @classmethod
    def create(cls, name: str, **kwargs):
        # generate autocommit db connection
        kwargs['autocommit'] = True
        _, conn = cls._parse_kwargs(None, **kwargs)

        # check if database already exists; if not, create the database
        if name in cls._list(conn):
            raise ValueError(f'database {name} already exists')
        else:
            query = SQL("CREATE DATABASE {name} WITH TABLESPACE {tablespace}")\
                .format(name=Identifier(name),
                        tablespace=Identifier(SQLTablespaces.PRIMARY))
            with conn.cursor() as cur:
                cur.execute(query)

        # remove autocommit and return instance of class
        _ = kwargs.pop('autocommit')
        return cls(name, **kwargs)


    @staticmethod
    def _parse_kwargs(name: Optional[str] = None, **kwargs) -> tuple[Union[str, None], psycopg.Connection]:
        conn = psycopg.connect(dbname=name, cursor_factory=psycopg.ClientCursor, **kwargs)
        return name, conn

    def connect(self, password: str, **kwargs): 
        self.conn = psycopg.connect(dbname=self.dbname, user=self.user, password=password, **kwargs)

    def execute(self,
                query: str,
                params: Optional[list[Union[str, list[str]]]] = None,
                identifiers: Optional[dict] = None,
                cur_kwargs: Optional[dict] = None,
                exe_kwargs: Optional[dict] = None,
                row_factory: Optional[Callable] = tuple_row,
                debug: bool = False) -> Union[Any, tuple[Any | str]]:

        # add identifiers to query, clean up args/kwargs
        query, params = self._parse_query(query, params, identifiers)
        cur_kwargs = {} if cur_kwargs is None else cur_kwargs
        exe_kwargs = {} if exe_kwargs is None else exe_kwargs

        # execute query, passing in params
        # return "mogrified" query alongside results if debug (else solely results)
        with self.cursor(row_factory=row_factory, **cur_kwargs) as cur:
            cur.execute(query=query, params=params, **exe_kwargs)

            if cur.rowcount == -1:
                res = None  # explicitly return None as cur.fetchall() would fail
            else:
                res = cur.fetchall()

            if debug:
                query = cur.mogrify(query=query, params=params)
                res = (res, query)

        return res

    def _parse_query(self, query, params, identifiers):
        params = self._parse_params(params)
        identifiers = self._parse_identifiers(identifiers)
        query = SQL(query).format(**identifiers)
        return query, params

    @staticmethod
    def _parse_params(params) -> list:
        parsed = []
        if params is not None:
            for param in params:
                if isinstance(param, str):
                    parsed.append(param)
                elif isinstance(param, list):
                    inner = Composed("")
                    for _ in param:
                        inner += _
                    inner = inner.join(", ")
                    parsed.append(SQL("(") + inner + SQL(")"))
                else:
                    raise TypeError(f'invalid type for param: {param.__class__.__name__}')
        return parsed

    @staticmethod
    def _parse_identifiers(identifiers) -> dict:
        parsed = {}
        if identifiers is not None:
            for ix, obj in identifiers.items():
                if isinstance(obj, str):
                    parsed[ix] = Identifier(obj)
                elif isinstance(obj, Identifier):
                    parsed[ix] = obj
                elif isinstance(obj, list):
                    inner = Composed("")
                    for _ in obj:
                        inner += Identifier(_)
                    inner = inner.join(", ")
                    parsed[ix] = SQL("(") + inner + SQL(")")
                else:
                    raise TypeError(f'invalid type for identifier: {obj.__class__.__name__}')
        return parsed

    @property
    def parent(self): return None

    @property
    def children(self):
        return self.execute(query="SELECT schema_name FROM information_schema.schemata WHERE catalog_name=%s;",
                            params=[self.name],
                            row_factory='value')


    @classmethod
    def read(cls, name: str, **kwargs):
        name, conn = cls._parse_kwargs(name, **kwargs)
        if name is None:
            raise ValueError(f'kwargs missing dbname')
        elif name not in cls._list(conn):
            raise ValueError(f'database {name} does not exist')
        return cls(name, **kwargs)

    def update(self):
        raise NotImplementedError

    def delete(self, **kwargs):
        kwargs['autocommit'] = True
        name, conn = self._parse_kwargs(None, **kwargs)
        self.conn.close()

        query = SQL("DROP DATABASE {name} WITH (FORCE);").format(name=Identifier(self.name))
        with conn.cursor() as cur:
            cur.execute(query)

    @staticmethod
    def _list(conn: psycopg.Connection):
        query = SQL("SELECT datname FROM pg_database;")
        with conn.cursor() as cur:
            cur.execute(query)
            res = [_ for (_,) in cur.fetchall()]
        return res

    def list(self):
        return self._list(self.conn)


class Schema(IDBObject):
    def __init__(self, name: str, parent: Database):
        self.name = name
        self.parent = parent

    @property
    def children(self):
        res = self.db_handle.execute(
            query="SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
            params=[self.name],
            row_factory='value')
        return res

    @classmethod
    def create(cls, name: str, parent: Database, **kwargs):
        if name in cls._list(parent.conn):
            raise ValueError(f'schema {name} already exists')
        else:
            parent.execute(query="CREATE SCHEMA {name};",
                           identifiers={'name': Identifier(name)})
        return cls(name=name, parent=parent)

    @classmethod
    def read(cls, name: str, parent: Database, **kwargs):
        if name not in cls._list(parent.conn):
            raise ValueError(f'schema {name} does not exist')
        else:
            return cls(name, parent)

    def update(self):
        raise NotImplementedError

    def delete(self):
        self.db_handle.execute(
            query="DROP SCHEMA {name} CASCADE;",
            identifiers={'name': Identifier(self.name)})

    @staticmethod
    def _list(conn: psycopg.Connection):
        query = SQL("SELECT schema_name FROM information_schema.schemata;")
        with conn.cursor() as cur:
            cur.execute(query)
            return [_ for (_,) in cur.fetchall()]

    def list(self):
        return self._list(self.parent.conn)


class Table(IDBObject):
    def __init__(self, name: str, parent: Schema):
        self.name = name
        self.parent = parent

    @property
    def children(self):
        return self.db_handle.execute(query=
                                      """
                                      SELECT column_name FROM information_schema.columns
                                      WHERE table_schema = %s AND table_name = %s;
                                      """,
                                      params=[self.parent.name, self.name],
                                      row_factory='value')

    @classmethod
    def create(cls, name: str, parent: Schema, **kwargs):
        if name in cls._list(parent.db_handle.conn, parent.name):
            raise ValueError(f'table {name} already exists under schema {parent.name}')
        else:
            parent.db_handle.execute(query="CREATE TABLE {schema}.{name}();",
                                     identifiers={'schema': Identifier(parent.name),
                                                  'name': Identifier(name)})
            return cls(name=name, parent=parent)

    @classmethod
    def read(cls, name: str, parent: Schema, **kwargs):
        if name not in cls._list(parent.db_handle.conn, parent.name):
            raise ValueError(f'table {name} does not exist under schema {parent.name}')
        else:
            return cls(name=name, parent=parent)

    def update(self, *args, **kwargs):  # add column
        pass

    def delete(self, **kwargs):
        self.db_handle.execute(query="DROP TABLE {schema}.{table} CASCADE;", identifiers=self.sql_dict)

    @staticmethod
    def _list(conn: psycopg.Connection, schema: str):
        query = SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = {schema};")\
            .format(schema=schema)
        with conn.cursor() as cur:
            cur.execute(query)
            return [_ for (_,) in cur.fetchall()]

    def list(self):
        return self._list(self.db_handle.conn, self.parent.name)


class Column(IDBObject):
    def __init__(self, name: str, dtype: str, parent: Table):
        self.name = name
        self.dtype = dtype
        self.parent = parent

    @staticmethod
    def _parse_kwargs(**kwargs) -> tuple[Any, Union[dict, None]]:
        if 'dtype' not in kwargs.keys():
            raise ValueError('dtype not in kwargs')
        elif not isinstance(kwargs['dtype'], str):
            raise ValueError('argument dtype must be a string')
        dtype = kwargs.pop('dtype')
        return dtype, kwargs

    @property
    def children(self):
        raise NotImplementedError

    @classmethod
    def create(cls, name, parent, **kwargs):
        if name in parent.children:
            raise ValueError(f'column {name} already exists in table {parent.name}')
        else:
            dtype, kwargs = cls._parse_kwargs(**kwargs)
            parent.db_handle.execute(query="ALTER TABLE {schema}.{table} ADD COLUMN {column} {dtype};",
                                     identifiers={'schema': Identifier(parent.parent.name),
                                                  'table': Identifier(parent.name),
                                                  'column': Identifier(name),
                                                  'dtype': Identifier(dtype)},
                                     **kwargs)
            return cls(name, dtype, parent)

    @classmethod
    def read(cls, name, parent, **kwargs):
        if name not in parent.children:
            raise ValueError(f'column {name} does not exist in table {parent.name} under schema {parent.parent.name}')
        else:
            dtype = parent.db_handle.execute(query="""
            SELECT udt_name FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s AND column_name = %s;""",
                                             params=[parent.parent.name, parent.name, name],
                                             row_factory='value',
                                             **kwargs)
            return cls(name, dtype[0], parent)

    def update(self, *args, **kwargs):
        ...

    def delete(self, **kwargs):
        self.db_handle.execute(query="ALTER TABLE {schema}.{table} DROP COLUMN {column};", identifiers=self.sql_dict)

    def list(self):
        ...


class Index(Column):
    ...
