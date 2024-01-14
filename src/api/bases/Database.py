from abc import ABC
from contextlib import contextmanager
from dataclasses import dataclass
from types import NoneType
from typing import Optional, Self, Any, Callable, Type
from uuid import uuid4, UUID
import psycopg.conninfo
from psycopg.sql import Identifier, SQL, Composed
from src.api.bases.Data import Field, map_type


"""
PostgreSQL OBJECT BASES/MIXINS
"""


class SQLObject(ABC):
    """Base object for database objects to inherit from"""

    SHORT_NAME = 'Obj'

    def __init__(self, name: str, parent: Optional[Self] = None):
        self._uuid: UUID = uuid4()
        self._name = name
        self._parent = parent

    def __str__(self): return f'[{self.SHORT_NAME}] {self._name}'

    def __repr__(self): return self.__str__()

    @property
    def uuid(self): return self._uuid

    @property
    def name(self): return self._name

    @property
    def parent(self): return self._parent

    @property
    def handle(self): return Identifier(self.name)


"""
PostgreSQL OBJECTS
"""


class Database(SQLObject):
    SHORT_NAME = 'Db'

    def __init__(self, name: str):
        super().__init__(name=name, parent=None)


class Schema(SQLObject):
    SHORT_NAME = 'Sch'

    def __init__(self, database: Database, name: str):
        super().__init__(name=name, parent=database)
        self._database = database

    @property
    def database(self): return self._database


class Table(SQLObject):
    SHORT_NAME = 'Tbl'

    def __init__(self, schema: Schema, name: str):
        super().__init__(name=name, parent=schema)
        self._schema = schema

    @property
    def handle(self): return SQL('.').join((self._schema.handle, Identifier(self.name)))


class Column(SQLObject):
    SHORT_NAME = 'Col'

    def __init__(self, table: Table, name: str, dtype: str, default: Any):
        super().__init__(name=name, parent=table)
        self._dtype = dtype
        self._default = default

    @property
    def dtype(self): return self._dtype

    @property
    def default(self): return self._default

    @property
    def handle(self): return self.name


class Index(SQLObject):
    SHORT_NAME = 'Idx'

    def __init__(self, table: Table, column: Column | tuple[Column], name: str, unique: bool = False):
        super().__init__(name=name, parent=table)
        self._column = column
        self._unique = unique

    @property
    def column(self):
        return self._column

    @property
    def unique(self): return self._unique


class Reference:
    def __init__(
            self,
            database: Database,
            schema: Optional[Schema] = None,
            table: Optional[Table] = None,
            column: Optional[Column] = None
    ):
        self._database = database
        self._schema = schema
        self._table = table
        self._column = column

    @property
    def handle(self): return self._database, self._schema, self._table, self._column

    def __str__(self):
        res = f"@{self._database.name}"
        for sql_obj in (self._schema, self._table, self._column):
            if sql_obj:
                res += f".{sql_obj.name}"
        return res

    def __repr__(self): return self.__str__()


"""
CONNECTION OBJECTS
"""


@dataclass
class Profile:
    user: Optional[str] = 'postgres'
    dbname: Optional[str] = 'hedgepy'
    kwargs: Optional[dict] = None

    def __post_init__(self):
        if isinstance(self.dbname, NoneType):
            self.dbname = self.user
        if isinstance(self.kwargs, NoneType):
            self.kwargs = {}

        self._uuid = uuid4()

    def conninfo(self, password: str) -> str:
        return psycopg.conninfo.make_conninfo(user=self.user, password=password, dbname=self.dbname, **self.kwargs)

    @property
    def uuid(self) -> str:
        return str(self._uuid)


class Connection:
    def __init__(self, profile: Profile, password: str, autocommit: bool = False):
        self._connection = None
        self._connect(profile=profile, password=password, autocommit=autocommit)
        self._profile = profile
        self._uuid = uuid4()

    def _connect(self, profile: Profile, password: str, autocommit: bool):
        conninfo = profile.conninfo(password=password)
        with psycopg.connect(conninfo=conninfo, autocommit=autocommit) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        self._connection = psycopg.connect(conninfo=conninfo, autocommit=autocommit)

    @property
    def uuid(self) -> str:
        return str(self._uuid)

    @property
    def handle(self) -> psycopg.Connection:
        return self._connection

    def toggle_autocommit(self):
        self.handle.autocommit = not self.handle.autocommit

    @contextmanager
    def cursor(self, row_factory: Callable) -> psycopg.Cursor:
        with self.handle.cursor() as cur:
            cur.row_factory = row_factory
            yield cur

    def close(self):
        self.handle.close()
        self._connection = None

    def reset(self, password: str):
        if not self.handle.closed:
            self.close()

        self._connect(profile=self._profile, password=password, autocommit=self.handle.autocommit)


class Session:
    def __init__(self, profile: Profile):
        self._profiles: dict[str, Profile] = {profile.uuid: profile}
        self._connections: dict[str, dict[str, Connection]] = {profile.uuid: {}}

    def new_profile(
            self,
            user: str,
            dbname: str,
            kwargs: Optional[dict] = None
    ) -> str:
        profile = Profile(user=user, dbname=dbname, kwargs=kwargs)
        self._profiles[profile.uuid] = profile
        return profile.uuid

    def new_connection(
            self,
            profile: Profile,
            password: str,
            autocommit: bool = False
    ) -> Connection:
        conn = Connection(profile=profile, password=password, autocommit=autocommit)
        self._connections[profile.uuid][conn.uuid] = conn
        return conn

    def close_connection(self, profile_uuid: str, conn_uuid: str):
        conn = self._connections[profile_uuid].pop(conn_uuid)
        conn.close()

    def get_connection(self, profile_uuid: str, conn_uuid: str) -> Connection:
        return self._connections[profile_uuid][conn_uuid]


@dataclass
class Query:
    body: SQL | Composed | str
    values: Optional[tuple | tuple[tuple]] = None
    returns: Optional[tuple[Field, ...] | tuple[tuple[str, Type], ...]] = None

    def __post_init__(self):
        if not isinstance(self.body, SQL | Composed):
            self.body = SQL(self.body)
        if self.returns and not all((isinstance(ret, Field) for ret in self.returns)):
            returns = []
            for ret in self.returns:
                if not isinstance(ret, Field):
                    name, typ = ret
                    returns.append(Field(name=name, dtype=map_type(typ)))
                else:
                    returns.append(ret)
            self.returns = tuple(returns)

    @property
    def to_cursor(self) -> dict: return {'query': self.body, 'params': self.values}


class CopyQuery(Query):
    @property
    def to_cursor(self) -> SQL: return self.body
