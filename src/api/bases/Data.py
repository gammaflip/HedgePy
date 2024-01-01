from sys import getsizeof
from numpy import array, ndarray
from pandas import DataFrame, Timestamp
from typing import Any, Optional, Type, Sequence, Callable
from functools import reduce
from dataclasses import dataclass
from psycopg.sql import SQL, Composed

"""
DATA TYPES
"""

DB_TYPES = {'text', 'float', 'int', 'bool', 'timestamp', 'array', 'jsonb'}
PY_TYPES = {str, float, int, bool, Timestamp, tuple, dict}
_DB_TO_PY = dict(zip(DB_TYPES, PY_TYPES))
_PY_TO_DB = dict(zip(PY_TYPES, DB_TYPES))


def map_type(typ: type | str | object) -> str | type:
    """Cast between Python and PostgreSQL types"""

    # return Python type from string
    if isinstance(typ, str) and typ in DB_TYPES:
        return _DB_TO_PY[typ]

    # raise error if type is string, and value not in _DB_TYPES
    elif isinstance(typ, str):
        raise TypeError(f'"{typ}" is not a valid PostgreSQL type')

    # return DB type from Python type
    elif typ in PY_TYPES:
        return _PY_TO_DB[typ]

    # return DB type from type of Python type
    elif _ := type(typ) in PY_TYPES:
        return _PY_TO_DB[_]

    # return DB type from typing.Generic
    elif hasattr(typ, '__origin__') and typ.__origin__ in PY_TYPES:
        return _PY_TO_DB[typ.__origin__]

    # raise error for uncaught type
    else:
        raise TypeError(f'"{typ}" ({_}) is not a valid Python type')


"""
DATA OBJECTS
"""


@dataclass
class Field:
    name: str
    dtype: PY_TYPES

    @property
    def dbtype(self) -> str:
        return map_type(self.dtype)


@dataclass
class Symbol:
    name: str
    figi: Optional[str] = None


class Data:
    def __init__(self, fields: Sequence[Field, ...], records: Sequence[Sequence[Any, ...], ...]):
        self._fields: tuple[Field, ...] = tuple(fields)
        self._records: tuple[tuple[Any, ...], ...] = self._ingest(fields, records)
        self._size: float = reduce(lambda x, y: x + y, (sum(getsizeof(val) for val in tup) for tup in records),)

    @staticmethod
    def _ingest(fields, records) -> tuple[tuple[Any, ...], ...]:
        if not all(len(record) == len(fields) for record in records):
            raise ValueError("All rows must have the same length")
        else:
            return tuple(tuple(record) for record in records)

    @property
    def dims(self) -> tuple[int, int]:
        return len(self._records), len(self._fields)

    @property
    def records(self) -> tuple[tuple[Any, ...], ...]:
        return self._records

    @property
    def fields(self) -> tuple[Field, ...]:
        return self._fields

    @property
    def arr(self) -> ndarray:
        if all(field.dtype == self._fields[0].dtype for field in self._fields):
            return array(self._records, dtype=self._fields[0].dtype)
        else:
            return array(self._records)

    @property
    def df(self) -> DataFrame:
        return DataFrame(self._records, columns=[field.name for field in self._fields])\
            .astype({field.name: field.dtype for field in self._fields if field.dtype != Timestamp})
        # let pandas handle Timestamps internally

    def __sizeof__(self):
        return self._size

    def __str__(self):
        return f"Data: ({self.dims[0]} x {self.dims[1]}) [{round(self._size / 1e6, 2)}MB]"

    def __repr__(self):
        return self.__str__()


"""
QUERY OBJECTS
"""


@dataclass
class Query:
    body: SQL | Composed | str
    values: Optional[tuple | tuple[tuple]] = None
    returns: Optional[tuple[Field, ...] | tuple[tuple[str, Type], ...]] = None

    def __post_init__(self):
        if not isinstance(self.body, SQL | Composed):
            self.body = SQL(self.body)

    @property
    def to_cursor(self) -> dict: return {'query': self.body, 'params': self.values}


class CopyQuery(Query):
    @property
    def to_cursor(self): raise NotImplementedError()


@dataclass
class Result:
    result: Data | Exception | None

    def __post_init__(self):
        self.timestamp = Timestamp.now()
