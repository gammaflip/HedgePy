# SEE https://datahelpdesk.worldbank.org/knowledgebase/articles/1886686-advanced-data-api-queries

import requests
from api.bases.data import Blob


ROOT = "http://api.worldbank.org/v2"


def get(sources: int = 2, country: str = 'USA', series: str = 'SP.POP.TOTL', time: str = 'all', page: int = 1) -> Blob:
    url = make_url(sources, country, series, time, page)
    response = requests.get(url)
    pages, response = process(response)

    if pages == 0:
        raise Exception(response)

    while page < pages:
        page += 1
        temp = get(sources, country, series, time, page)
        response.extend(temp)

    return response


def make_url(sources: int, country: str, series: str, time: str, page: int) -> str:
    url = ROOT
    url += f'/sources/{sources}'
    url += f'/country/{country}'
    url += f'/series/{series}'
    url += f'/time/{time}'
    url += '/data'
    url += '?format=json'
    url += '&per_page=10000'
    url += f'&page={page}'
    return url


def process(response: requests.Response) -> tuple[int, str | Blob]:
    if not response.ok:
        return 0, f'{response.status_code}: {response.text}'
    else:
        return fmt(response)


def fmt(response: requests.Response) -> tuple[int, Blob]:
    j = response.json()
    pages, data = j['pages'], j['source']['data']

    def row(d: dict) -> tuple:
        temp = {}
        while var := d['variable']:
            _ = var.pop()
            temp[_['concept']] = _['id'], _['value']
        return temp['Country'][0], temp['Series'][0], temp['Time'][1], d['value']

    cols = ('country', 'series', 'time', 'value')
    data = [row(_) for _ in data]
    return pages, Blob(cols=cols, data=data)
