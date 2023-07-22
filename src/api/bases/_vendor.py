import os
import inspect
import importlib
import config
from typing import Optional, Literal, Any
from types import FunctionType, ModuleType
from _data.bases.data import Packet
from _data.bases.db import Database


class Vendor:

    def __init__(self, name: str) -> None:
        try: 
            assert name + '.py' in os.listdir(config.VENDORS)
        except AssertionError:
            raise ValueError(f'invalid vendor name provided: {name}')

        self._name = name
        self._path = os.path.join(config.VENDORS, self._name + '.py')

        self.connected = False
        self.glob = {}
        self.getters = {}
        self.metadata = {}
        
    @property
    def name(self): return self._name

    @property
    def path(self): return self._path

    def connect(self, db: Database) -> bool:
        mod = self._import(name=self.name, path=self.path)
        self.glob, self.getters = self._parse_namespace(mod)
        self.metadata = self._metadata(self.name, db)
        self.connected = True

        return self.connected

    @staticmethod
    def _import(name: str, path: str): 
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        
        return mod

    @staticmethod
    def _parse_namespace(mod: ModuleType) -> tuple[dict, dict]: 
        glob, getters = {}, {}
        
        for name in dir(mod):

            # ignore private variables; retrive object from namespace
            if not name.startswith('_'):  
                obj = getattr(mod, name)

                # all uppercase variables are stored as globals
                if name.isupper():
                    glob[name] = obj

                # of remaining variables, consider those defined in the 
                # module to 
                elif hasattr(obj, '__module__') and obj.__module__ is name:
                    ...
                    
        return glob, getters


    @staticmethod
    def _metadata(name: str, db: Database): ...


def Getter(func: FunctionType):
    sig = inspect.signature(func)


def Formatter(func: FunctionType) -> Packet:
    ...


# class Vendor:
#     def __init__(self, name: str):
#         self.name = name
#         self.path = os.path.join(config.API, name + '.py')
#         self.ns = self._init_ns(name=self.name, path=self.path)

#     @staticmethod
#     def _init_ns(name: str, path: str):
#         ns = {}
#         for k, v in runpy.run_path(path, run_name=name).items():
#             if (isinstance(v, type) | isinstance(v, FunctionType)) and v.__module__ == name:
#                 ns[k] = v
#         return ns

#     def __call__(self, oname: str, *args, **kwargs) -> Any:
#         return self.ns[oname](*args, **kwargs)

#     def get(self, oname: str) -> Callable:
#         return self.ns[oname]

#     def sig(self, oname: str) -> inspect.Signature:
#         return inspect.signature(self.get(oname))


# class Endpoint(ABC):
#     def __init__(self): ...


# class GenericRESTEndpoint(Endpoint):
#     def __init__(self, base_url: Optional[str] = None, suffix: Optional[str] = None):
#         super().__init__()
#         self._base = base_url if base_url else 'https://reqres.in/api/'
#         self._suffix = suffix

#     def _url(self, *args):
#         li = [self._base] + [arg for arg in args] if args else [self._base]
#         li = li + [self._suffix] if self._suffix else li
#         return ''.join(li)

#     def request(self,
#                 request: Literal['GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'],
#                 *args,  # for self._url
#                 **kwargs  # for requests.request
#                 ) -> requests.Response:
#         return requests.request(request,
#                                 self._url(*args),
#                                 **kwargs)
