from abc import ABC
from typing import Optional, Any, Self
from datetime import datetime


"""
PostgreSQL OBJECT BASES/MIXINS
"""


DBTYPES = ('text', 'float', 'int', 'bool', 'timestamp', 'jsonb')
PYTYPES = (str, float, int, bool, datetime, dict)


def map_type(typ: type | str) -> str | type:
    """Cast between Python and PostgreSQL types"""
    if isinstance(typ, str) and typ in DBTYPES:
        return dict(zip(DBTYPES, PYTYPES))[typ]
    elif isinstance(typ, type) and typ in PYTYPES:
        return dict(zip(PYTYPES, DBTYPES))[typ]
    else:
        raise TypeError(f'invalid dtype')


class SQLObject(ABC):
    """Base object for database objects to inherit from"""

    def __init__(self, name: str, parent: Optional[Self] = None):
        self._name: str = name

        if parent and parent.parent:
            self._parent: list[Self] | list = parent.parent + [parent]
        elif parent:
            self._parent: list[Self] | list = [parent]
        else:
            self._parent: list[Self] | list = []

    @property
    def name(self): return self._name

    @property
    def parent(self): return self._parent

    def __str__(self): return f'{self.__class__.__name__}: {".".join(_.name for _ in self.parent)}.{self.name}'

    def __repr__(self): return str(self)


"""
PostgreSQL OBJECTS
"""


class Database(SQLObject):
    def __init__(self, name: str, tablespace: Optional[str] = None):
        super().__init__(name, None)
        self._tablespace = tablespace

    @property
    def tablespace(self): return self._tablespace


class Schema(SQLObject):
    def __init__(self, name: str, database: Database):
        super().__init__(name, database)


class Table(SQLObject):
    def __init__(self, name: str, schema: Schema):
        super().__init__(name, schema)


class Column(SQLObject):
    def __init__(
            self,
            name: str,
            table: Table,
            dtype: str,
            pkey: Optional[bool] = False,
            fkey: Optional[str] = None,
    ):

        super().__init__(name, table)

        try:
            assert dtype in DBTYPES
        except AssertionError:
            raise ValueError(f'invalid dtype: {dtype}')

        self._dtype = dtype
        self._pkey = pkey
        self._fkey = fkey

    @property
    def dtype(self): return self._dtype

    @property
    def primary_key(self): return self._pkey

    @property
    def foreign_key(self): return self._fkey
