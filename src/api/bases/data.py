import copy
import json
import numpy as np
import config
from functools import singledispatchmethod, reduce
from itertools import chain, accumulate
from collections import UserDict, deque
from abc import ABC, abstractmethod
from typing import Optional, Any, NewType, Mapping
from types import FunctionType
from psycopg import types as dbtypes
from uuid import uuid4


class ADO(ABC):
    def __init__(self, name: Optional[str] = None):
        self._name = name
        self._uuid = uuid4()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'{self.__class__.__name__}: {self.name}'

    @property
    def name(self): return self._name if self._name else self.uuid

    @property
    def uuid(self): return self._uuid


class Packet(ADO, UserDict):
    def __init__(
        self, 
        d: dict, name: Optional[str] = None
        ):
        ADO.__init__(self, name=name)
        UserDict.__init__(self, d)

    @classmethod
    def from_json(cls, s: str): return cls(d=json.loads(s))  # ensure compatible with row_factory

    def to_json(self): return json.dumps(self.data)

    def index(self, path: list) -> Any:
        d = self.data.copy()
        for p in path:
            d = d[p]
        return d

    def flatten(self) -> list[list]:
        d = copy.deepcopy(self.data)
        res, path = self._flatten(d, [], [])
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

    def walk(self, d: Optional[dict] = None):
        d = copy.deepcopy(self.data) if not d else d
        while d:
            key = [_ for _ in d.keys()][0]
            value = d.pop(key)
            if isinstance(value, dict):
                yield key
                self.walk(value)
            else:
                yield key, value


class FormattedPacket(Packet):
    @classmethod
    def from_json(cls, s: str, fmt: Optional[Mapping]): 
        d = json.loads(s)
        



class Queue(ADO):
    def __init__(self, name: Optional[str] = None,
                 handler: Optional[dict[type, FunctionType]] = None,
                 initial: Optional[list] = None):
        super().__init__(name)
        self.current = None
        self._queue = deque(initial) if initial else deque()
        self._handler = handler

    def __iter__(self):
        return self.queue.__iter__()

    def __next__(self):
        if len(self.queue) > 0:
            value, func = self.queue.pop(), None
            if self.handler:
                func = self.handler.get(value.__class__)
            return func(value) if func else value
        else:
            raise StopIteration('empty queue')

    @property
    def handler(self): return self._handler

    @property
    def queue(self): return self._queue

    @property
    def list(self): return [_ for _ in self.queue]


class DType(NewType):
    def __init__(self, pytype: type, dbtype: dbtypes.TypeInfo):
        super().__init__(pytype.__name__, pytype)
        self._pyclass = pytype
        self._dbtype = dbtype

    @property
    def pyclass(self): return self._pyclass

    @property
    def pymod(self): return self._pyclass.__module__

    @property
    def pyname(self): return self.pyclass.__name__

    @property
    def pytype(self): return type(self.pyname, self.pyclass.__bases__, dict(self.pyclass.__dict__))

    def pyinstance(self, *args, **kwargs):
        return self.pytype(*args, **kwargs)

    @property
    def dbtype(self): return self._dbtype

    @property
    def dbname(self): return self.dbtype.name
