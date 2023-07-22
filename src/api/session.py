from psycopg import Connection, connect
from psycopg.conninfo import make_conninfo
from api.query import RowFactories
from typing import Optional, Callable, Literal, ForwardRef
from types import NoneType
from uuid import uuid4, UUID
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum

POOL: Optional[ForwardRef('Pool')] = None
DEFAULT_ROWFACTORY = RowFactories.tuple_row
DEFAULT_N = 6


@dataclass
class Profile:
    user: str = 'postgres'
    row_factory: Callable = DEFAULT_ROWFACTORY
    name: Optional[str] = None

    def connection(self, password: str, **kwargs) -> Connection:
        return connect(
            conninfo=make_conninfo(f'user={self.user} password={password}', **kwargs),
            row_factory=self.row_factory
        )


class Pool:
    def __init__(
            self,
            password: str,
            num: Optional[int] = None,
            base_profile: Optional[Profile] = None,
    ):
        self.base_profile = Profile() if isinstance(base_profile, NoneType) else base_profile
        self._num = num if num else DEFAULT_N
        self._pool = OrderedDict()
        self._common = self.make_conn(password)

        self.start(password)

    def __getitem__(self, key: Optional[int | str]) -> Connection:
        return self.get_conn(pool='managed', handle=key)

    @property
    def info(self) -> dict:
        res = {}
        attrs = ['status', 'transaction_status', 'dbname', 'user', 'host', 'port']
        li1, li2 = [_ for _ in self._pool.items()], [_ for _ in range(len(self._pool))]
        for tup, ix in zip(li1, li2):
            name, conn = tup
            name = ix if not isinstance(ix, NoneType) else name.hex
            res[name] = {}
            for attr in attrs:
                value = getattr(conn.info, attr)
                res[name][attr] = value.name if isinstance(value, Enum) else value
        return res

    def make_conn(self,
                  password: str,
                  profile: Optional[Profile] = None,
                  **kwargs
                  ) -> Connection:
        # remove connection from generic pool if needed to maintain size
        if len(self._pool) == self._num:
            _ = self._pool.popitem(last=False)

        profile = self.base_profile if isinstance(profile, NoneType) else profile

        self._pool[uuid4()] = conn = profile.connection(password=password, **kwargs)

        return conn

    def start(self, password: str):
        for _ in range(self._num):
            self.make_conn(password)


def new(password: str, num: int = DEFAULT_N, base_profile: Optional[Profile] = None):
    global POOL
    POOL = Pool(password=password, num=num, base_profile=base_profile)
    POOL.start(password)
