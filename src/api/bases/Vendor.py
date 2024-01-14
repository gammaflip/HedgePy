import inspect
from typing import Callable, Optional
from types import NoneType, ModuleType
from pandas import Timestamp, Timedelta
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
        self._timestamp = None

    @property
    def token(self) -> str:
        return self._token

    @token.setter
    def token(self, token: str):
        self._token = token

    @property
    def timestamp(self) -> Timestamp:
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp: Timestamp):
        self._timestamp = timestamp

    def refresh(self):
        self.token, self.timestamp = self(), Timestamp.now()


class Formatter(_MetaFunction):
    def format(self, res: Response, params: dict) -> Data:
        return self(res, params)


@dataclass
class Getter(_MetaFunction):
    PARAM_MAP = {
        'symbol': Optional[str | list[str]],
        'field': Optional[str | list[str]],
        'start': Optional[Timestamp | list[Timestamp]],
        'end': Optional[Timestamp],
        'resolution': Optional[Timedelta]
    }

    def __post_init__(self):
        self._signature = inspect.signature(self.func)
        self._params = {}
        self._kwargs = {}

        for param_name, param in self._signature.parameters.items():
            default = param.default
            if param_name in self.PARAM_MAP:
                self._params[param_name] = default
            else:
                self._kwargs[param_name] = default

    @property
    def signature(self) -> inspect.Signature:
        return self._signature

    def bind(self, **kwargs) -> dict:
        kwargs_out = self._params.copy() | self._kwargs.copy()
        for kwarg_name, kwarg_value in kwargs.items():
            if kwarg_name in kwargs_out:
                kwargs_out[kwarg_name] = kwarg_value
            else:
                raise ValueError(f"Unrecognized parameter: {kwarg_name}")
        return kwargs_out


@dataclass
class Endpoint:
    name: str
    getter: Getter
    formatter: Optional[Formatter]

    def __call__(self, **kwargs):
        bound_args = self.getter.bind(**kwargs)
        res, params = self.getter(**bound_args)
        if self.formatter:
            return self.formatter.format(res, params)
        else:
            return res

    @property
    def signature(self) -> inspect.Signature:
        return self.getter.signature

    @property
    def map(self):
        return {
            k:
                {
                    'default': not (isinstance(v.default, NoneType) or isinstance(v.default, inspect._empty)),
                    'kind': v.kind.value,
                    'annotation': str(v.annotation)
                }
            for k, v in self.signature.parameters.items()
        }

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
