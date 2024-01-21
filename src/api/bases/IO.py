import asyncio; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import aiohttp
from psycopg import AsyncConnection, conninfo
from psycopg.sql import SQL, Composed
from psycopg_pool import AsyncConnectionPool
from typing import Optional
from contextlib import asynccontextmanager, AbstractAsyncContextManager


class DBPool(AbstractAsyncContextManager):
    _pool: AsyncConnectionPool = None

    def __init__(self, conn_info: dict):
        conn_info = conninfo.make_conninfo(**conn_info)
        self._pool = AsyncConnectionPool(conninfo=conn_info, open=False)

    async def __aenter__(self):
        if self._pool:
            if self._pool.closed:
                await self._pool.open()
            return self
        raise ConnectionError("Connection pool lost")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._pool.__aexit__(exc_type, exc_val, exc_tb)

    @asynccontextmanager
    async def connection(self):
        async with self._pool.connection() as conn:
            yield conn


async def db_transaction(conn, query: SQL, params: Optional[tuple] = None) -> list | None:
    async with conn.cursor() as cur:
        await cur.execute(query=query, params=params)
        return await cur.fetchall()
