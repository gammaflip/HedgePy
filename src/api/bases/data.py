import copy
import json
from pandas import DataFrame, Timestamp
from collections import UserDict
from collections.abc import Mapping
from enum import Enum
from typing import Any, Optional, Iterator, Type, Sequence, Protocol, Self
from types import NoneType


"""
DATA OBJECTS
"""


class Blob:
    def __init__(
            self,
            cols: Optional[Sequence] = None,
            data: Optional[Sequence[tuple]] = None,
            metadata: Optional[Mapping] = None,
    ):
        if isinstance(cols, NoneType):
            cols = ()
        if isinstance(data, NoneType):
            data = [()]

        self._validate(cols=cols, data=data)

        self.cols = cols
        self.data = data
        self.metadata = metadata

    @staticmethod
    def _validate(cols: Sequence, data: Sequence):
        for row in data:
            assert len(row) == len(cols)

    def extend(self, other: Self | Sequence[tuple]):
        if not isinstance(other, Sequence):
            other = other.data
        self._validate(self.cols, other)
        self.data += other

    def df(self) -> DataFrame: return DataFrame(data=self.data, columns=self.cols)


class Packet(UserDict):
    """Primary vessel for data to/from user/db/vendors. Meant to bridge gap between unstructured third-party vendor
    query results and the database's schema. Provides methods to reshape data to/from array-like and dict-like
    subclasses. May be instantiated empty, with a dict, or from json via the from_json classmethod."""

    def __init__(self, schema: Optional[Mapping]):
        ...

    @classmethod
    def from_json(cls, s: str): return cls(d=json.loads(s))

    def to_json(self): return json.dumps(self.data)

    @property
    def df(self) -> DataFrame:
        flattened = self.flatten()
        return DataFrame(
            data=[[v] for (i, v) in flattened],
            index=[tuple([_ for _ in i]) for (i, v) in flattened]
        )

    @property
    def dims(self) -> tuple[int, int]:
        flattened = self.flatten()
        n_rows = len(flattened)
        n_cols = max(*[len(flattened[v][0]) + 1 for v in range(n_rows)])
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
    timestamp = Timestamp
    json = Packet

    @classmethod
    def py(cls, db: str): return cls[db].value

    @classmethod
    def db(cls, py: type): return cls(py).name


"""
TYPES
"""


class TypeMap:
    TYPES = {'varchar': str,
             'float8': float,
             'int4': int,
             'bool': bool,
             'timestamp': Timestamp,
             'json': dict}

    def __call__(self, lookup: str | Type) -> Type | str:
        d = self.TYPES if isinstance(lookup, str) else {v: k for k, v in self.TYPES.items()}
        return d[lookup]
