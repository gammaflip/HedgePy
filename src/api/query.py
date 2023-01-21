import inspect
from inspect import Signature, BoundArguments
from driver import Database, Schema, Table, Column, Index
from typing import Literal, TypeVar, Callable

DBO = TypeVar('DBObject', Database, Schema, Table, Column, Index)
METH = Literal['create', 'read', 'update', 'delete', 'list']


def get_func(obj: DBO, meth: METH) -> Callable: return getattr(obj, meth)


def get_sig(func: Callable) -> inspect.Signature: return inspect.signature(func)


def check_implemented(func: Callable) -> bool: return 'NotImplementedError' not in inspect.getsource(func)


def bind_sig(sig: Signature, *args, **kwargs) -> BoundArguments: return sig.bind_partial(*args, **kwargs)


def query(obj: DBO, meth: METH, *args, **kwargs):
    func = get_func(obj, meth)
    if check_implemented(func):
        sig = get_sig(func)
        sig = bind_sig(sig, *args, **kwargs)
    else:
        raise NotImplementedError
