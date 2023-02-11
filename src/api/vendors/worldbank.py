# SEE https://datahelpdesk.worldbank.org/knowledgebase/topics/125589-developer-information


import requests
from typing import Union, Optional
from data.bases.data import Packet


ROOT = "http://api.worldbank.org/v2/"
DEFAULT_PARAMS = {"format": "json", "per_page": 32000, "page": 1}


def get(
    dims: tuple[tuple[str, Union[str, list[str]]]],
    params: Optional[dict[str, str]] = None,
) -> Packet:
    """Given dims, which correspond to directories, and optionally params, which 
    correspond to arguments (see Worldbank API docs for more information on directories,
    and arguments), return an unstructured Packet object

    :param dims: Tuple of tuples composed of two strings, A and B; append '{A}/{B}' to URL 
    for each tuple; B may alternatively be a list, in which case each tuple is appended to URL  
    as '{A}/{B_0};{B_1};[...]{B_N}' 
    :type dims: tuple[tuple[str, Union[str, list[str]]]]
    :param params: Optional dictionary of string key-value pairs, A and B; append '{A}={B}'
    to URL for each pair
    :type params: Optional[dict[str, str]], optional
    :return: Unstructured Packet of data from Worldbank API
    :rtype: Packet
    """

    response, params = _get(dims, params)
    container = _handle_response(response, dims, params, [])
    packet = _container_to_packet(container)
    return packet

def format(packet: Packet):
    ...

def _handle_response(response: list, dims: tuple, params: dict, container: list) -> list:
    if len(response) == 2:
        container = _handle_good_response(response, dims, params, container)
        return container

    else:
        _handle_bad_response(response)


def _handle_good_response(response: list, dims: tuple, params: dict, container: list) -> list:
    info, payload = response[0], response[1]
    container.append(payload)

    if len(container) < info["pages"]:
        params["page"] += 1
        response, params = _get(dims, params)
        container = _handle_good_response(response, dims, params, container)

    else:
        return container


def _container_to_packet(container: list) -> Packet:
    if isinstance(container[0], list):
        temp_container = []
        for page in container: 
            for item in page: 
                temp_container.append(item)
        container = temp_container

    d = dict(zip(range(len(container)), container))
    return Packet(d)


def _handle_bad_response(response: list):
    if len(response) == 1:
        message = response[0]["message"][0]
        id_, key, value = message["id"], message["key"], message["value"]
        raise ValueError(f"{key} (id: {id_}): {value}")

    else:
        raise ValueError(f"Failed to parse response: {response}")


def _get(dims: tuple, params: dict) -> tuple[list, dict]:
    params = _process_params(params)
    url = ROOT + _dims_to_str(dims) + _params_to_str(params)
    return requests.get(url).json(), params


def _dims_to_str(dims) -> str:
    res = ""

    for dim in dims:

        label, value = dim
        res += "{}/".format(label)

        if isinstance(value, list):
            value = ";".join(value)

        res += "{}/".format(value)

    res = res.rstrip("/") + "?"  # DO BETTER PLS

    return res


def _params_to_str(params) -> str:
    res = ""
    temp_params = params.copy()
    key, value = temp_params.popitem()
    res += "{}={}".format(key, value)

    if temp_params:
        for key, value in temp_params.items():
            res += "&{}={}".format(key, value)

    return res


def _process_params(params) -> dict:
    _ = params.copy() if isinstance(params, dict) else dict()
    params = DEFAULT_PARAMS.copy()
    params.update(_)

    return params
