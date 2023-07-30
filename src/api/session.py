import asyncio; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
from contextlib import asynccontextmanager, contextmanager
import psycopg.conninfo
from typing import Optional, Literal, Callable
from types import NoneType
from dataclasses import dataclass
from collections import OrderedDict
from uuid import uuid4
from src.api.query import query, Query, ROW_FACTORIES, ACTIONS, SQL_OBJECT_TYPES

DEFAULT_ROW_FACTORY = ROW_FACTORIES.tuple_row


@dataclass
class Profile:
    user: Optional[str] = 'postgres'
    dbname: Optional[str] = 'hedgepy'
    kwargs: Optional[dict] = None

    def __post_init__(self):
        if isinstance(self.dbname, NoneType):
            self.dbname = self.user
        if isinstance(self.kwargs, NoneType):
            self.kwargs = {}

        self._uuid = uuid4()

    def conninfo(self, password: str) -> str:
        return psycopg.conninfo.make_conninfo(user=self.user, password=password, dbname=self.dbname, **self.kwargs)

    @property
    def uuid(self) -> str:
        return str(self._uuid)


class Connection:
    def __init__(self, profile: Profile, password: str, autocommit: bool = False):
        self._connect(profile=profile, password=password, autocommit=autocommit)
        self._profile = profile

    def _connect(self, profile: Profile, password: str, autocommit: bool):
        conninfo = profile.conninfo(password=password)
        with psycopg.connect(conninfo=conninfo, autocommit=autocommit) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        self._connection = psycopg.connect(conninfo=conninfo, autocommit=autocommit)

    @property
    def handle(self) -> psycopg.Connection:
        return self._connection

    def toggle_autocommit(self):
        self.handle.autocommit = not self.handle.autocommit

    @contextmanager
    def cursor(self, row_factory: Callable = DEFAULT_ROW_FACTORY) -> psycopg.Cursor:
        with self.handle.cursor() as cur:
            cur.row_factory = row_factory
            yield cur

    def close(self):
        self.handle.close()
        self._connection = None

    def reset(self, password: str):
        if not self.handle.closed:
            self.close()

        self._connect(profile=self._profile, password=password, autocommit=self.handle.autocommit)


class Session:
    def __init__(self, profile: Profile, password: str):
        self.host: tuple[Profile, Connection] = profile, Connection(profile=profile, password=password, autocommit=True)
        self._di: dict[Profile: Connection] = {profile: Connection(profile=profile, password=password)}

    def new_connection(
            self,
            password: str,
            user: Optional[str] = None,
            dbname: Optional[str] = None,
            autocommit: bool = False,
            **kwargs
    ):
        profile = Profile(user=user, dbname=dbname, kwargs=kwargs)
        self._di[profile] = Connection(profile=profile, password=password, autocommit=autocommit)

    def close_connection(self, uuid: str):
        for profile, conn in self._di.items():
            if profile.uuid == uuid:
                conn = self._di.pop(profile)
                conn.close()
                break


class AsyncPool:
    def __init__(self, num: Optional[int] = None, base_profile: Optional[Profile] = None):
        self.profile = Profile() if isinstance(base_profile, NoneType) else base_profile
        self._pool = asyncio.LifoQueue(maxsize=self._num)

    async def _get(self) -> psycopg.AsyncConnection:
        conn = await self._pool.get()
        return conn

    async def _put(self, conn: psycopg.AsyncConnection):
        await self._pool.put(conn)

    async def _new(self, password: str, **kwargs) -> psycopg.AsyncConnection:
        conn = await self.profile.async_connection(password=password, **kwargs)
        await self._put(conn)
        return conn

    async def start(self, password: str, **kwargs):
        for _ in range(self._num):
            _ = await self._new(password, **kwargs)

    @asynccontextmanager
    async def connection(self) -> psycopg.AsyncConnection:
        conn = await self._get()
        try:
            yield conn
        finally:
            await self._put(conn)

