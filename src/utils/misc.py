import sys
from importlib.util import spec_from_file_location, module_from_spec


def load_api(api_path: str):
    spec = spec_from_file_location("vendors", api_path)
    api = module_from_spec(spec)
    sys.modules[api_path] = api
    spec.loader.exec_module(api)
    return api