import copy
import json
import pandas as pd
from collections import UserDict
from dataclasses import dataclass
from enum import Enum
from abc import ABC
from typing import Any, Optional
from uuid import uuid4


"""
DATA OBJECTS
"""


class Packet(UserDict):
    """Primary vessel for data to/from user/db/vendors. Meant to bridge gap between unstructured third-party vendor
    query results and the database's schema. Provides methods to reshape data to/from array-like and dict-like
    subclasses. May be instantiated empty, with a dict, or from json via the from_json classmethod."""

    @classmethod
    def from_json(cls, s: str): return cls(d=json.loads(s))

    def to_json(self): return json.dumps(self.data)

    @property
    def df(self) -> pd.DataFrame:
        flattened = self.flatten()
        return pd.DataFrame([[v] for (i, v) in flattened],
                            index=[tuple([_ for _ in i]) for (i, v) in flattened])

    @property
    def dims(self) -> tuple[int, int]:
        d = self.flatten()
        n_rows = len(d)
        n_cols = max(*[len(d[v][0]) + 1 for v in range(n_rows)])
        return n_rows, n_cols

    def flatten(self) -> list[list]:
        d, res, path = copy.deepcopy(self.data), [], []
        res, path = self._flatten(d, res, path)
        return res

    def _flatten(self, subdict: dict, res: list, path: list) -> tuple[list[list], list]:
        while subdict:
            key = [_ for _ in subdict.keys()][0]  # this was chosen over pop to preserve order of keys
            path, value = path + [key], subdict.pop(key)
            if isinstance(value, dict):
                res, path = self._flatten(value, res, path)
            else:
                res += [(path.copy(), value)]
            path.pop()
        return res, path

    def index(self, path: list) -> Any:
        d = self.data.copy()
        for p in path:
            d = d[p]
        return d


class DTypes(Enum):
    """Manually define mappings for types to/from user/db/vendors. Use py and db methods to convert back and forth."""
    varchar = str
    float8 = int
    bool = bool
    timestamp = pd.Timestamp
    json = Packet

    @classmethod
    def py(cls, db: str): return cls[db].value

    @classmethod
    def db(cls, py: type): return cls(py).name


class Tablespaces(Enum):
    PRIMARY = 'pg_default'


"""
PostgreSQL OBJECT BASES/MIXINS
"""


class SQLObject(ABC):
    """Base object for database objects to inherit from"""

    def __init__(self, name: str):
        self._name = name
        self._uuid = uuid4()

    @property
    def name(self): return self._name

    @property
    def uuid(self): return self._uuid

    def __str__(self): return f'{self.__class__.__name__}: {self.name} ({self.uuid})'

    def __repr__(self): return str(self)  # be less lazy


"""
PostgreSQL OBJECTS
"""


class Database(SQLObject):
    def __init__(self, name: str, tablespace: str):
        super().__init__(name)
        self._tablespace = tablespace

        try:
            getattr(Tablespaces, tablespace)
        except AssertionError:
            raise Exception(f'tablespace {tablespace} does not exist')

    @property
    def tablespace(self): return self._tablespace


class Schema(SQLObject):
    def __init__(self, name: str):
        super().__init__(name)


class Table(SQLObject):
    def __init__(self, name: str):
        super().__init__(name)


class Column(SQLObject):
    def __init__(self, name: str, dtype: str, default: Optional[Any]):
        super().__init__(name)
        self._dtype = dtype
        self._default = default

        try:
            pytype = getattr(DTypes, dtype).value
            if default:
                assert isinstance(default, pytype)
        except AttributeError:
            raise Exception(f'invalid dtype: {dtype}')
        except AssertionError:
            raise Exception(f'default value {default} type does not match dtype {dtype}')

    @property
    def dtype(self): return getattr(DTypes, self._dtype)

    @property
    def default(self): return self._default


class Index(SQLObject):
    def __init__(self, columns: list[Column], unique: bool = True):
        super().__init__('')
        self._columns = columns
        self._unique = unique

    @property
    def columns(self): return self._columns

    @property
    def unique(self): return self._unique


@dataclass
class SQLUser:
    name: str

    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    def url(self, password: str): return f'postgres://{self.name}:{password}@localhost/'
