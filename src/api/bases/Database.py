from abc import ABC
from typing import Optional, Self, Any
from uuid import uuid4, UUID
from psycopg.sql import Identifier, SQL


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

    def __init__(self, name: str, tablespace: Optional[str] = None):
        super().__init__(name=name, parent=None)
        self._tablespace = tablespace

    @property
    def tablespace(self): return self._tablespace


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
CUSTOM DATABASE OBJECTS
"""


