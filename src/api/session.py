import asyncio; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
from contextlib import asynccontextmanager
import psycopg
from typing import Optional, Literal, Callable
from types import NoneType
from dataclasses import dataclass
from src.api.query import query, Query, ROW_FACTORIES, ACTIONS, SQL_OBJECT_TYPES

DEFAULT_N = 8
DEFAULT_ROW_FACTORY = ROW_FACTORIES.tuple_row


@dataclass
class Profile:
    user: str = 'postgres'
    row_factory: Optional[Callable] = DEFAULT_ROW_FACTORY

    async def connection(self, password: str, **kwargs) -> psycopg.AsyncConnection:
        conninfo = psycopg.conninfo.make_conninfo(f'user={self.user} password={password}', **kwargs)
        return await psycopg.AsyncConnection.connect(conninfo=conninfo)


class Pool:
    def __init__(self, num: Optional[int] = None, base_profile: Optional[Profile] = None):
        self.profile = Profile() if isinstance(base_profile, NoneType) else base_profile
        self._num = num if num else DEFAULT_N
        self._pool = asyncio.LifoQueue(maxsize=self._num)

    async def _get(self) -> psycopg.AsyncConnection:
        conn = await self._pool.get()
        return conn

    async def _put(self, conn: psycopg.AsyncConnection):
        await self._pool.put(conn)

    async def _new(self, password: str, **kwargs) -> psycopg.AsyncConnection:
        conn = await self.profile.connection(password=password, **kwargs)
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


class Task:
    def __init__(self, action: ACTIONS, sql_object_type: SQL_OBJECT_TYPES, **kwargs):
        self._query: Query = query(action, sql_object_type, **kwargs)

    async def execute(self, conn: psycopg.AsyncConnection, row_factory: Optional[Callable] = DEFAULT_ROW_FACTORY):
        prepared = self._query.prepare()
        async with conn.cursor(row_factory=row_factory) as cur:
            await cur.execute(**prepared)
            return await cur.fetchall()
