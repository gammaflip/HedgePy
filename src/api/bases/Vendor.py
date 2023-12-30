import inspect
from typing import Callable, Optional
from types import NoneType
from types import ModuleType
from pandas import Timestamp
from dataclasses import dataclass
from requests import Response
from src.api.bases.Data import Data


# class PyFunction:
#     def __init__(self, function: Callable):
#         self._f: Callable = function
#         self._name: str = function.__name__
#         self._bound: dict | inspect._empty = inspect._empty
#
#         self._pos_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
#         self._kw_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
#         self._var_pos_args: Optional[tuple[str, inspect.Parameter]] = None
#         self._var_kw_args: Optional[tuple[str, inspect.Parameter]] = None
#         self._annotations: dict[str, Any] = dict()
#         self._defaults: dict[str, Any] = dict()
#         self._return: Any = None
#
#         sig = inspect.signature(function)
#
#         for name, param in sig.parameters.items():
#             self._annotations[name] = param.annotation
#             self._defaults[name] = param.default
#
#             match param.kind:
#                 case inspect.Parameter.POSITIONAL_ONLY:
#                     self._pos_args[name] = param
#                 case inspect.Parameter.KEYWORD_ONLY | inspect.Parameter.POSITIONAL_OR_KEYWORD:
#                     self._kw_args[name] = param
#                 case inspect.Parameter.VAR_POSITIONAL:
#                     self._var_pos_args = name, param
#                 case inspect.Parameter.VAR_KEYWORD:
#                     self._var_kw_args = name, param
#
#         self._return = sig.return_annotation
#
#     def __call__(self):
#         if self._bound and self._bound == inspect._empty:
#             raise TypeError('Function has not been bound')
#         elif self._bound:
#             args, kwargs = self.bound_signature
#             return self._f(*args, **kwargs)
#         else:
#             return self._f()
#
#     @property
#     def name(self) -> str:
#         return self._name
#
#     @property
#     def annotations(self) -> dict:
#         return self._annotations
#
#     @property
#     def defaults(self) -> dict:
#         return self._defaults
#
#     @property
#     def signature(self) -> dict[str, inspect.Parameter]:
#         return {
#             name: param
#             for name, param in chain(
#                 self._pos_args.items(),
#                 self._kw_args.items(),
#                 (self._var_pos_args,),
#                 (self._var_kw_args,)
#             )
#         }
#
#     @property
#     def expanded_signature(self) -> tuple[
#         OrderedDict[str, inspect.Parameter],
#         OrderedDict[str, inspect.Parameter],
#         Optional[tuple[str, inspect.Parameter]],
#         Optional[tuple[str, inspect.Parameter]],
#     ]:
#         return self._pos_args, self._kw_args, self._var_pos_args, self._var_kw_args
#
#     @property
#     def bound_signature(self) -> tuple[list[Any], dict[str, Any]]:
#         if self._bound == inspect._empty:
#             raise TypeError('Function has not been bound')
#
#         args_out, kwargs_out = [], {}
#
#         for name, value in self._bound.items():
#             if name in self._pos_args:
#                 args_out.append(value)
#             else:
#                 kwargs_out[name] = value
#
#         return args_out, kwargs_out
#
#     @property
#     def return_type(self) -> Any:
#         return self._return
#
#     def bind(self, args: Optional[tuple] = None, kwargs: Optional[dict] = None, *varargs, **varkwargs):
#         if self._bound != inspect._empty:
#             raise TypeError('Function has already been bound')
#
#         partial = dict(zip(self.signature.copy().keys(), [inspect._empty for _ in range(len(self.signature))]))
#
#         if args and len(args) == len(self._pos_args):
#             for arg, (name, param) in zip(args, self._pos_args.items()):
#                 partial = self._bind_arg(partial, name, param, arg)
#         elif args:
#             raise TypeError(f'Expected {len(self._pos_args)} positional arguments, got {len(args)}')
#
#         if kwargs:
#             for name, param in self._kw_args.items():
#                 if name in kwargs:
#                     value = kwargs.pop(name)
#                     partial = self._bind_arg(partial, name, param, value)
#             if kwargs:
#                 raise TypeError(f'Unexpected keyword arguments: {", ".join(kwargs.keys())}')
#
#         if varargs:
#             if len(self._var_pos_args) == 0:
#                 raise TypeError(f'Function does not accept variable positional arguments')
#             else:
#                 name, param = self._var_pos_args
#                 partial = self._bind_arg(partial, name, param, varargs)
#
#         if varkwargs:
#             if len(self._var_kw_args) == 0:
#                 raise TypeError(f'Function does not accept variable keyword arguments')
#             else:
#                 name, param = self._var_kw_args
#                 partial = self._bind_arg(partial, name, param, varkwargs)
#
#         self._bound = self._fill_missing(partial)
#
#     @staticmethod
#     def _bind_arg(partial: dict, name: str, param: inspect.Parameter, value: Any) -> dict:
#         if isinstance(param.annotation, type) and param.annotation != inspect._empty:
#             if not isinstance(value, param.annotation):
#                 try:
#                     value = param.annotation(value)
#                 except TypeError:
#                     raise TypeError(f'{name} must be of type {param.annotation}')
#
#         partial[name] = value
#         return partial
#
#     def _fill_missing(self, partial: dict) -> dict:
#         for name, param in self.signature.items():
#             if partial[name] is inspect._empty:
#                 if (param.kind != inspect.Parameter.VAR_POSITIONAL) and (param.kind != inspect.Parameter.VAR_KEYWORD):
#                     if param.default is inspect._empty:
#                         raise TypeError(f'Missing required argument: {name}')
#                     else:
#                         partial[name] = param.default
#
#         return partial

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
