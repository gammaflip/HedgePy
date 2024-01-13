from src import config
import requests
from typing import Literal, Optional
from src.api.bases.Data import Data, Field, Timestamp

# DOCS: https://fred.stlouisfed.org/docs/api/fred/
ROOT = 'https://api.stlouisfed.org/fred'
DFMT = '%Y-%m-%d'
DEFAULT_START = Timestamp('1776-07-04')
DEFAULT_END = Timestamp('9999-12-31')


def authenticate() -> str:
    return config.PROJECT_ENV['API_FRED_KEY']


def get_release(
        authorization: str,
        start: Timestamp = DEFAULT_START,
        end: Timestamp = DEFAULT_END,
        release_id: Optional[int] = None,
) -> tuple[requests.Response, dict]:
    if release_id:
        url = f'{ROOT}/release'
        params = {
            'release_id': release_id,
            'realtime_start': start.strftime(DFMT),
            'realtime_end': end.strftime(DFMT),
            'api_key': authorization,
            'file_type': 'json'
        }
    else:
        url = f'{ROOT}/releases'
        params = {
            'start': start.strftime(DFMT),
            'end': end.strftime(DFMT),
            'api_key': authorization,
            'file_type': 'json'
        }
    return requests.get(url, params=params), params


def get_release_dates(
        authorization: str,
        release_id: int,
        start: Timestamp = DEFAULT_START,
        end: Timestamp = DEFAULT_END,
) -> tuple[requests.Response, dict]:
    url = f'{ROOT}/release/dates'
    params = {
        "release_id": release_id,
        "realtime_start": start.strftime(DFMT),
        "realtime_end": end.strftime(DFMT),
        "api_key": authorization,
        "file_type": "json",
    }
    return requests.get(url, params=params), params


def get_release_series(
        authorization: str,
        release_id: int,
        start: Timestamp = DEFAULT_START,
        end: Timestamp = DEFAULT_END,
) -> tuple[requests.Response, dict]:
    url = f'{ROOT}/release/series'
    params = {
        "release_id": release_id,
        "realtime_start": start.strftime(DFMT),
        "realtime_end": end.strftime(DFMT),
        "api_key": authorization,
        "file_type": "json",
    }
    return requests.get(url, params=params), params


def get_series(
        authorization: str,
        series_id: str,
        start: Timestamp = DEFAULT_START,
        end: Timestamp = DEFAULT_END,
) -> tuple[requests.Response, dict]:
    url = f'{ROOT}/series'
    params = {
        "series_id": series_id,
        "realtime_start": start.strftime(DFMT),
        "realtime_end": end.strftime(DFMT),
        "api_key": authorization,
        "file_type": "json",
    }
    return requests.get(url, params=params), params


def get_series_observations(
        authorization: str,
        series_id: str,
        start: Timestamp = DEFAULT_START,
        end: Timestamp = DEFAULT_END,
) -> tuple[requests.Response, dict]:
    url = f'{ROOT}/series/observations'
    params = {
        "series_id": series_id,
        "realtime_start": start.strftime(DFMT),
        "realtime_end": end.strftime(DFMT),
        "api_key": authorization,
        "file_type": "json",
    }
    return requests.get(url, params=params), params


def fmt_release(res: requests.Response, params: dict) -> Data:
    fields = (
        Field(name='id', dtype=int),
        Field(name="realtime_start", dtype=Timestamp),
        Field(name="realtime_end", dtype=Timestamp),
        Field(name='name', dtype=str),
        Field(name='press_release', dtype=str),
        Field(name='link', dtype=str),
        Field(name='notes', dtype=str)
    )

    records = []
    for release in res.json()['releases']:
        record = list(release.values())
        while len(record) < len(fields):
            record.append(None)
        records.append(record)

    return Data(fields=fields, records=records)


def fmt_release_dates(res: requests.Response, params: dict) -> Data:
    fields = (
        Field(name='date', dtype=Timestamp),
    )

    records = []
    for date in res.json()['release_dates']:
        records.append((Timestamp(date['date'], unit='D'),))

    return Data(fields=fields, records=records)


def fmt_release_series(res: requests.Response, params: dict) -> Data:
    fields = (
        Field(name='id', dtype=str),
        Field(name='realtime_start', dtype=Timestamp),
        Field(name='realtime_end', dtype=Timestamp),
        Field(name='title', dtype=str),
        Field(name='observation_start', dtype=Timestamp),
        Field(name='observation_end', dtype=Timestamp),
        Field(name='frequency', dtype=str),
        Field(name='frequency_short', dtype=str),
        Field(name='units', dtype=str),
        Field(name='units_short', dtype=str),
        Field(name='seasonal_adjustment', dtype=str),
        Field(name='seasonal_adjustment_short', dtype=str),
        Field(name='last_updated', dtype=Timestamp),
        Field(name='popularity', dtype=int),
        Field(name='group_popularity', dtype=int),
        Field(name="notes", dtype=str),
    )

    records = []
    for series in res.json()['seriess']:
        record = list(series.values())
        while len(record) < len(fields):
            record.append(None)
        records.append(record)

    return Data(fields=fields, records=records)


def fmt_series(res: requests.Response, params: dict) -> Data:
    fields = (
        Field(name='id', dtype=str),
        Field(name='realtime_start', dtype=Timestamp),
        Field(name='realtime_end', dtype=Timestamp),
        Field(name='title', dtype=str),
        Field(name='observation_start', dtype=Timestamp),
        Field(name='observation_end', dtype=Timestamp),
        Field(name='frequency', dtype=str),
        Field(name='frequency_short', dtype=str),
        Field(name='units', dtype=str),
        Field(name='units_short', dtype=str),
        Field(name='seasonal_adjustment', dtype=str),
        Field(name='seasonal_adjustment_short', dtype=str),
        Field(name='last_updated', dtype=Timestamp),
        Field(name='popularity', dtype=int),
        Field(name="notes", dtype=str),
    )

    records = []
    for series in res.json()['seriess']:
        record = list(series.values())
        while len(record) < len(fields):
            record.append(None)
        records.append(record)

    return Data(fields=fields, records=records)


def fmt_series_observations(res: requests.Response, params: dict) -> Data:
    fields = (
        Field(name='date', dtype=Timestamp),
        Field(name='realtime_start', dtype=Timestamp),
        Field(name='realtime_end', dtype=Timestamp),
        Field(name="value", dtype=float),
    )

    records = []
    for observation in res.json()['observations']:
        record = list(observation.values())
        while len(record) < len(fields):
            record.append(None)
        records.append(record)

    return Data(fields=fields, records=records)

