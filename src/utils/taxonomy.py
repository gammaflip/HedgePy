import os
import json
import time
import requests
import pandas as pd
from typing import Optional, Union, Literal
from itertools import product

DIRECTORY = pd.DataFrame\
    .from_records(columns=['Name', 'Description', 'Type', 'API Endpoint', 'API Key'],
                  data=[['iso3166', 'country codes', 'json', None, None],
                        ['iso4217', 'currency codes', 'json', None, None],
                        ['figi', 'OpenFIGI', 'rest', 'https://api.openfigi.com/v3/',
                         'd80ae087-de0b-4c58-80ee-cc0a20600036']])\
    .set_index('Name')


def get_records(name: str,
                match_on: Optional[str] = None,
                values: Optional[Union[str, list[str]]] = None,
                output: Optional[Union[str, list[str]]] = None,
                labels: bool = False) -> pd.DataFrame:
    standard = DIRECTORY.loc[name]
    res = _get_records_json(standard, match_on, values, output) \
        if standard['Type'] == 'json' \
        else globals().get(f'_{name}')(standard, match_on, values, output)
    if labels:
        return pd.DataFrame(data=res, index=values, columns=output)
    else:
        return pd.DataFrame(res)


def figi(id_type: Optional[str] = None,
         id_value: Optional[str] = None,
         query: Optional[str] = None,
         labels: bool = False,
         **kwargs) -> Union[pd.DataFrame, list[dict]]:
    """FIGI API interface that implements mapping and search jobs (see https://www.openfigi.com/api)

    :param id_type: corresponds to idType under POST /v3/mapping
    :param id_value: corresponds to idValue under POST /v3/mapping
    :param query: corresponds to query under POST /v3/search
    :param labels: determines if DataFrame is returned (default: False)
    :param kwargs: passed to API query body
    :return: DataFrame or list of dicts depending upon labels argument
    """
    standard = DIRECTORY.loc['figi']
    url = standard['API Endpoint']
    headers = {'X-OPENFIGI-APIKEY': standard['API Key']}

    if id_type and id_value:
        url += 'mapping'
        res, dup = _figi_mapping(url=url,
                                 headers=headers,
                                 id_type=id_type,
                                 id_value=id_value,
                                 **kwargs)
    elif query:
        url += 'search'
        res, dup = _figi_search(url=url,
                                headers=headers,
                                query=query,
                                **kwargs)
    else:
        raise NotImplementedError()

    res = pd.DataFrame.from_records(res) if labels else res
    return res


def _get_records_json(standard: pd.Series, match_on, values, output):
    values = [None] if match_on is None else values
    values = [values] if not isinstance(values, list) else values
    output = [output] if not isinstance(output, list) else output
    p = os.path.join(os.environ['FINLAB_ROOT'], 'data', 'standards', f'{standard.name}.json')
    res = []
    with open(p) as f:
        records = json.load(f)
        while len(records) > 0:
            record = records.pop(0)
            if (match_on is None) | (record.get(match_on) in values):
                if output == [None]:
                    res.append(record)
                else:
                    res.append([record[k] for k in sorted(output)])
    return res


def _figi_mapping(url: str, headers: dict, id_type: str, id_value: str, **kwargs):
    body = {'idType': id_type, 'idValue': id_value}
    if kwargs:
        body.update(**kwargs)
    dup = {'url': url, 'json': [body], 'headers': headers}

    def inner(rec: None, u, b, h):
        request = requests.post(url=u, json=[b], headers=h)
        match request.status_code:
            case 429:
                print('Rate limit reached, sleeping 6s')
                time.sleep(6)
                return rec
            case 200:
                rec = request.json()[0].get('data')
                print(f'Results = {len(rec)}')
                return rec
            case _:
                raise requests.exceptions.HTTPError(request.status_code)

    records = None
    while not records:
        records = inner(records, url, body, headers)

    return records, dup


def _figi_search(url: str, headers: dict, query: str, **kwargs):
    body = {'query': query}
    if kwargs:
        body.update(**kwargs)
    dup = {'url': url, 'json': body, 'headers': headers}

    def inner(rec: list, s: Optional[str], u, b, h):
        if s:
            body.update({'start': s})
        request = requests.post(url=u, json=b, headers=h)
        match request.status_code:
            case 429:
                print('Rate limit reached, sleeping 60s')
                time.sleep(60)
                return s
            case 200:
                rec.extend(request.json().get('data'))
                print(f'Results = {len(rec)}')
                return request.json().get('next')
            case _:
                raise requests.exceptions.HTTPError(request.status_code)

    start = inner(records := [], None, url, body, headers)
    while start:
        start = inner(records, start, url, body, headers)

    return records, dup
