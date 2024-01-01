import inspect
from typing import Callable, Optional
from types import NoneType
from types import ModuleType
from pandas import Timestamp
from dataclasses import dataclass
from requests import Response
from src.api.bases.Data import Data


@dataclass
class _MetaFunction:
    func: Callable

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return self.func.__repr__()


class Authorization(_MetaFunction):
    def __post_init__(self):
        self._token = None
        self._last_auth = None

    @property
    def token(self) -> str:
        return self._token

    @property
    def last_auth(self) -> Timestamp:
        return self._last_auth

    def refresh(self):
        self._token = self()
        self._last_auth = Timestamp.now()


class Formatter(_MetaFunction):
    def format(self, res: Response, params: dict) -> Data:
        return self(res, params)


class Getter(_MetaFunction):
    @property
    def signature(self) -> inspect.Signature:
        return inspect.signature(self.func)

    def bind(self, *args, **kwargs):
        return self.signature.bind(*args, **kwargs)


@dataclass
class Endpoint:
    name: str
    getter: Getter
    formatter: Optional[Formatter]

    def __call__(self, *args, **kwargs):
        res, params = self.getter(*args, **kwargs)
        if self.formatter:
            return self.formatter.format(res, params)
        else:
            return res

    @property
    def signature(self) -> inspect.Signature:
        return self.getter.signature

    @property
    def map(self):
        return {k: {
            'default': not (isinstance(v.default, NoneType) or isinstance(v.default, inspect._empty)),
            'kind': v.kind.value,
            'annotation': str(v.annotation)
        } for k, v in self.signature.parameters.items()}

    def __getitem__(self, item):
        return self.signature.parameters[item]


class Vendor:
    def __init__(self, name: str, vendor_module: ModuleType):
        self._name = name
        self._endpoints = {'auth': None}

        for func_key, func_value in vendor_module.__dict__.items():
            if func_key == 'authenticate':
                self._endpoints['auth'] = Endpoint('auth', Getter(func_value), None)

            elif func_key.startswith('get'):
                name = func_key[4:]
                getter = func_value

                if 'fmt_' + func_key[4:] in vendor_module.__dict__.keys():
                    formatter = vendor_module.__dict__['fmt_' + func_key[4:]]
                else:
                    formatter = None

                self._endpoints[name] = Endpoint(name, Getter(getter), Formatter(formatter))

    @property
    def name(self): return self._name

    def __getitem__(self, item):
        return self._endpoints[item]

    def __iter__(self):
        return iter(self._endpoints.values())


class ResourceMap:
    def __init__(self, root: ModuleType):
        self._map = dict()

        for vendor_key, vendor_value in root.__dict__.items():
            if not vendor_key.startswith('_'):
                self._map[vendor_key] = Vendor(vendor_key, vendor_value)

    @property
    def map(self):
        return {k: list(v._endpoints.keys()) for k, v in self._map.items()}

    def __getitem__(self, item):
        return self._map[item]

    def __iter__(self):
        return iter(self._map.values())
