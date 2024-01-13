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

PY_TYPES = {str, float, int, bool, Timestamp, tuple, dict}
DB_TYPES = {'text', 'float', 'int', 'bool', 'timestamp', 'array', 'json'}
_DB_TYPE_ALIASES = {'character varying': 'text', 'integer': 'int', 'boolean': 'bool'}
_PY_TO_DB = dict(zip(PY_TYPES, DB_TYPES))
_DB_TO_PY = dict(zip(DB_TYPES, PY_TYPES))


def map_type(typ: type | str | object) -> str | type:
    """Cast between Python and PostgreSQL types"""

    # return Python type from string
    if isinstance(typ, str) and typ in DB_TYPES:
        return _DB_TO_PY[typ]

    # return Python type from alias of string
    elif isinstance(typ, str) and typ in _DB_TYPE_ALIASES.keys():
        return _DB_TO_PY[_DB_TYPE_ALIASES[typ]]

    # return DB type from Python type
    elif typ in PY_TYPES:
        return _PY_TO_DB[typ]

    # return DB type from type of Python type
    elif _ := type(typ) in PY_TYPES:
        return _PY_TO_DB[_]

    # return DB type from typing.Generic
    elif hasattr(typ, '__origin__'):
        if typ.__origin__ in PY_TYPES:
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
    def __init__(self, fields: Sequence[Field], records: Sequence[Sequence]):
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
        if not all((isinstance(ret, Field) for ret in self.returns)):
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


@dataclass
class Result:
    result: Data | Exception | None

    def __post_init__(self):
        self.timestamp = Timestamp.now()
