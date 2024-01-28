import asyncio; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import aiohttp
import psycopg
import psycopg_pool
from pandas import Timestamp
from typing import Optional, Type, Literal
from dataclasses import dataclass
from contextlib import asynccontextmanager, AbstractAsyncContextManager
from src.api.bases.Data import Data, Field, map_type


@dataclass
class DBRequest:
    body: psycopg.sql.SQL | psycopg.sql.Composed | str
    values: Optional[tuple | tuple[tuple]] = None
    returns: Optional[tuple[Field, ...] | tuple[tuple[str, Type], ...]] = None

    def __post_init__(self):
        if not isinstance(self.body, psycopg.sql.SQL | psycopg.sql.Composed):
            self.body = psycopg.sql.SQL(self.body)
        if self.returns and not all((isinstance(ret, Field) for ret in self.returns)):
            returns = []
            for ret in self.returns:
                if not isinstance(ret, Field):
                    name, typ = ret
                    returns.append(Field(name=name, dtype=typ))
                else:
                    returns.append(ret)
            self.returns = tuple(returns)

    @property
    def to_cursor(self) -> dict: return {'query': self.body, 'params': self.values}


class CopyDBRequest(DBRequest):
    @property
    def to_cursor(self) -> psycopg.sql.SQL: return self.body


@dataclass
class HTTPRequest:
    meth: Literal['GET', 'POST', 'PUT'] = 'GET'
    url: str = "http://httpbin.org/get"
    headers: Optional[dict] = None
    params: Optional[dict] = None

    @property
    def to_session(self) -> dict:
        return {
            'method': self.meth.lower(),
            'url': self.url,
            'headers': self.headers,
            'params': self.params
        }


@dataclass
class Result:
    content: Data | Exception | None

    def __post_init__(self):
        self.timestamp = Timestamp.now()


class DBPool(AbstractAsyncContextManager):
    _pool: psycopg_pool.AsyncConnectionPool = None

    def __init__(self, conn_info: dict):
        conn_info = psycopg.conninfo.make_conninfo(**conn_info)
        self._pool = psycopg_pool.AsyncConnectionPool(conninfo=conn_info, open=False)

    @property
    def alive(self) -> bool:
        return not self._pool.closed

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


async def db_transaction(pool: DBPool, request: DBRequest) -> Result:
    async with pool as pool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(**request.to_cursor)
                if request.returns:
                    res = await cur.fetchall()
                    res = Data(fields=request.returns, records=res)
                else:
                    res = None
    return Result(res)


class HTTPPool(AbstractAsyncContextManager):
    _pool: aiohttp.ClientSession = None

    def __init__(
            self,
            base_url: Optional[str] = None,
            cookies: Optional[dict] = None,
            headers: Optional[dict] = None,
            **kwargs
    ):
        self._pool = aiohttp.ClientSession(base_url=base_url, cookies=cookies, headers=headers, **kwargs)

    @property
    def alive(self) -> bool:
        return not self._pool.closed if self._pool else False

    async def __aenter__(self):
        if self._pool:
            if self._pool.closed:
                self._pool = aiohttp.ClientSession(
                    base_url=self._pool._base_url,
                    cookies=self._pool.cookie_jar.filter(),
                    headers=self._pool.headers
                )
            return await self._pool.__aenter__()
        raise ConnectionError("Connection pool lost")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._pool.__aexit__(exc_type, exc_val, exc_tb)


async def http_transaction(pool: HTTPPool, request: HTTPRequest) -> dict | None:
    async with pool as session:
        async with session.request(**request.to_session) as response:
            return await response.json()
