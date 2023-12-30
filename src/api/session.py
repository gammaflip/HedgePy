from contextlib import asynccontextmanager, contextmanager
import psycopg.conninfo
from typing import Optional, Literal, Callable
from types import NoneType
from dataclasses import dataclass
from uuid import uuid4


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
        self._connection = None
        self._connect(profile=profile, password=password, autocommit=autocommit)
        self._profile = profile
        self._uuid = uuid4()

    def _connect(self, profile: Profile, password: str, autocommit: bool):
        conninfo = profile.conninfo(password=password)
        with psycopg.connect(conninfo=conninfo, autocommit=autocommit) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        self._connection = psycopg.connect(conninfo=conninfo, autocommit=autocommit)

    @property
    def uuid(self) -> str:
        return str(self._uuid)

    @property
    def handle(self) -> psycopg.Connection:
        return self._connection

    def toggle_autocommit(self):
        self.handle.autocommit = not self.handle.autocommit

    @contextmanager
    def cursor(self, row_factory: Callable) -> psycopg.Cursor:
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
    def __init__(self, profile: Profile):
        self._profiles: dict[str, Profile] = {profile.uuid: profile}
        self._connections: dict[str, dict[str, Connection]] = {profile.uuid: {}}

    def new_profile(
            self,
            user: str,
            dbname: str,
            kwargs: Optional[dict] = None
    ) -> str:
        profile = Profile(user=user, dbname=dbname, kwargs=kwargs)
        self._profiles[profile.uuid] = profile
        return profile.uuid

    def new_connection(
            self,
            profile: Profile,
            password: str,
            autocommit: bool = False
    ) -> Connection:
        conn = Connection(profile=profile, password=password, autocommit=autocommit)
        self._connections[profile.uuid][conn.uuid] = conn
        return conn

    def close_connection(self, profile_uuid: str, conn_uuid: str):
        conn = self._connections[profile_uuid].pop(conn_uuid)
        conn.close()

    def get_connection(self, profile_uuid: str, conn_uuid: str) -> Connection:
        return self._connections[profile_uuid][conn_uuid]
