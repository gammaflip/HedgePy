import copy
import json
import numpy as np
import pandas as pd
from pandas import Timestamp, Timedelta
from collections import UserDict, deque
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Any, Type, Mapping, Callable


@dataclass
class DType:
    db: str
    py: Type


class DTypes(Enum):
    STR = DType('varchar', str)
    FLOAT = DType('float8', float)
    INT = DType('int4', int)
    BOOL = DType('bool', bool)
    DATETIME = DType('timestamp', Timestamp)
    JSON = DType('json', dict)


class Packet(UserDict):
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
        



class Queue():
    def __init__(self, name: Optional[str] = None,
                 handler: Optional[dict[type, Callable]] = None,
                 initial: Optional[list] = None):
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
