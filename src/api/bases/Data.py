import re
from datetime import date, time, datetime
from typing import Type
from sys import getsizeof
from numpy import array, ndarray
from pandas import DataFrame, Timestamp
from typing import Any, Optional, Sequence
from functools import reduce
from dataclasses import dataclass

"""
DATA TYPES
"""

TYPE_MAP = {
  # | sql type | py type | regex |
    "text":      (str,      r"(?P<str>.*)"),
    "bool":      (bool,     r"(?P<bool>true|false)"),
    "null":      (None,     r"(?P<none>NULL)"),
    "int":       (int,      r"(?P<sign>\-)?(?P<int>[0-9]*)"),
    "float":     (float,    r"(?P<sign>\-)?(?P<int>[0-9]*)(?P<dec>\.)(?P<frac>[0-9]*)?"),
    "date":      (date,     r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"),
    "time":      (time,     r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).(?P<microsecond>\d{6})"),
    "timestamp": (datetime, r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) "
                            r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).(?P<microsecond>\d{6})")
}


def resolve_type(value: str) -> Type:
    for dbtype, (pytype, pattern) in reversed(TYPE_MAP.items()):
        pattern = re.compile(pattern)
        if match := pattern.match(value):
            if match.group() == value:
                return pytype


def map_type():


    return


def _db_to_py_type(typ: str) -> Type:
    pytype, regex = TYPE_MAP[typ.lower()]
    return pytype


def _py_to_db_type(typ: Type) -> str:
    for dbtype, (pytype, pattern) in reversed(TYPE_MAP.items()):
        if pytype == typ:
            return dbtype


"""
DATA OBJECTS
"""


@dataclass
class Field:
    name: str
    dtype: str

    """TODO: REWRITE VENDOR IMPLEMENTATIONS (STR REPRESENTATION -> PY TYPE), <-- PREFERRED 
    OR CHANGE TYPE_MAP INTERFACE (PY TYPE -> STR REPRESENTATION)"""

    @property
    def dbtype(self) -> str:
        return _db_to_py_type(self.dtype)


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


@dataclass
class Result:
    result: Data | Exception | None

    def __post_init__(self):
        self.timestamp = Timestamp.now()
