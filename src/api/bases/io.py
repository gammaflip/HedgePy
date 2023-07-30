import inspect
from typing import Callable, Optional, Any
from collections import OrderedDict
from itertools import chain


class PyFunction:
    def __init__(self, function: Callable):
        self._f: Callable = function
        self._bound: dict | inspect._empty = inspect._empty

        self._pos_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
        self._kw_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
        self._pos_or_kw_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
        self._var_pos_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
        self._var_kw_args: OrderedDict[str, inspect.Parameter] = OrderedDict()
        self._annotations: dict[str, Any] = dict()
        self._defaults: dict[str, Any] = dict()
        self._return: Any = None

        sig = inspect.signature(function)

        for name, param in sig.parameters.items():
            self._annotations[name] = param.annotation
            self._defaults[name] = param.default

            match param.kind:
                case inspect.Parameter.POSITIONAL_ONLY:
                    self._pos_args[name] = param
                case inspect.Parameter.KEYWORD_ONLY:
                    self._kw_args[name] = param
                case inspect.Parameter.POSITIONAL_OR_KEYWORD:
                    self._pos_or_kw_args[name] = param
                case inspect.Parameter.VAR_POSITIONAL:
                    self._var_pos_args[name] = param
                case inspect.Parameter.VAR_KEYWORD:
                    self._var_kw_args[name] = param

        self._return = sig.return_annotation

    def __call__(self):
        if isinstance(self._bound, inspect._empty):
            raise TypeError('Function has not been bound yet')
        elif self._bound:
            args, kwargs = self.bound_signature
            return self._f(*args, **kwargs)
        else:
            return self._f()

    @property
    def annotations(self) -> dict:
        return self._annotations

    @property
    def defaults(self) -> dict:
        return self._defaults

    @property
    def signature(self) -> dict:
        return {
            name: param
            for name, param in chain(
                self._pos_args.items(),
                self._kw_args.items(),
                self._pos_or_kw_args.items(),
                self._var_pos_args.items(),
                self._var_kw_args.items()
            )
        }

    @property
    def bound_signature(self) -> tuple[list[Any], dict[str, Any]] | None:
        if self._bound and self._bound == inspect._empty:
            raise TypeError('Function has not been bound')

        elif self._bound:
            args_out, kwargs_out = [], {}
            for name, value in self._bound.items():
                if name in self._pos_args:
                    args_out.append(value)
                else:
                    kwargs_out[name] = value
            return args_out, kwargs_out

    @property
    def return_type(self) -> Any:
        return self._return

    def bind(self, args: Optional[tuple] = None, kwargs: Optional[dict] = None, *varargs, **varkwargs) -> bool:
        if self._bound and self._bound != inspect._empty:
            raise TypeError('Function has already been bound')

        partial = dict(zip(self.signature.copy().keys(), [inspect._empty for _ in range(len(self.signature))]))

        if args and len(args) == len(self._pos_args):
            for arg, (name, param) in zip(args, self._pos_args.items()):
                partial = self._bind_arg(partial, name, param, arg)
        elif args:
            raise TypeError(f'Expected {len(self._pos_args)} positional arguments, got {len(args)}')

        if kwargs:
            for name, param in chain(self._kw_args.items(), self._pos_or_kw_args.items()):
                if name in kwargs:
                    value = kwargs.pop(name)
                    partial = self._bind_arg(partial, name, param, value)
            if kwargs:
                raise TypeError(f'Unexpected keyword arguments: {", ".join(kwargs.keys())}')

        if varargs:
            if len(self._var_pos_args) == 0:
                raise TypeError(f'Function does not accept variable positional arguments')
            else:
                name, param = self._var_pos_args.popitem()
                partial = self._bind_arg(partial, name, param, varargs)

        if varkwargs:
            if len(self._var_kw_args) == 0:
                raise TypeError(f'Function does not accept variable keyword arguments')
            else:
                name, param = self._var_kw_args.popitem()
                partial = self._bind_arg(partial, name, param, varkwargs)

        self._bound = self._fill_missing(partial)

    @staticmethod
    def _bind_arg(partial: dict, name: str, param: inspect.Parameter, value: Any) -> dict:
        if isinstance(param.annotation, type) and param.annotation != inspect._empty:
            if not isinstance(value, param.annotation):
                try:
                    value = param.annotation(value)
                except TypeError:
                    raise TypeError(f'{name} must be of type {param.annotation}')

        partial[name] = value
        return partial

    def _fill_missing(self, partial: dict) -> dict:
        for name, param in self.signature.items():
            if partial[name] is inspect._empty:
                if (param.kind != inspect.Parameter.VAR_POSITIONAL) and (param.kind != inspect.Parameter.VAR_KEYWORD):
                    if param.default is inspect._empty:
                        raise TypeError(f'Missing required argument: {name}')
                    else:
                        partial[name] = param.default

        return partial
