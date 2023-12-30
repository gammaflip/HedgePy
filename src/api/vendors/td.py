import config
import requests
from typing import Literal, Optional
from src.api.bases.Data import Data, Field, Timestamp


# AUTH: https://developer.tdameritrade.com/content/simple-auth-local-apps
KEY = config.PROJECT_ENV['API_TD_KEY']
REFRESH_TOKEN = config.PROJECT_ENV['API_TD_TOKEN']
CLIENT_ID = KEY + '@AMER.OAUTHAP'
DFMT = '%Y-%m-%d'
DTFMT = '%Y-%m-%dT%H:%M:%S%z'
DTUNIT = 'ms'


"""
UTILITY FUNCTIONS
"""


def authenticate() -> tuple[str, None]:
    url = 'https://api.tdameritrade.com/v1/oauth2/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
    }

    res = requests.post(url, headers=headers, data=data)
    access_token = res.json()['access_token']

    return access_token, None


"""
RAW HTTP GET REQUESTS
"""


def get_instrument(
        authorization: str,
        symbol: str,
        projection: Literal['symbol-search', 'symbol-regex', 'desc-search', 'desc-regex', 'fundamental'] = 'symbol-search'
) -> tuple[requests.Response, dict]:
    url = f'https://api.tdameritrade.com/v1/instruments'
    headers = {'Authorization': f'Bearer {authorization}'}
    params = {'symbol': symbol, 'projection': projection}
    return requests.get(url, headers=headers, params=params), params


def get_market_hours(
        authorization: str,
        date: Timestamp,
        market: Literal['EQUITY', 'OPTION', 'FUTURE', 'BOND', 'FOREX'] = 'EQUITY',
) -> tuple[requests.Response, dict]:
    url = f'https://api.tdameritrade.com/v1/marketdata/hours'
    headers = {'Authorization': f'Bearer {authorization}'}
    params = {'date': date.strftime(DFMT), 'markets': market}

    return requests.get(url, headers=headers, params=params), params


def get_option_chain(
        authorization: str,
        symbol: str,
        contract_type: Literal['CALL', 'PUT', 'ALL'] = 'ALL',
        strike_count: int = 50,
        include_quotes: bool = False,
        range: Literal["ITM", "NTM", "OTM", "SAK", "SBK", "SNK", "ALL"] = 'ALL',
        expiry_month: Literal['ALL', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'] = 'ALL',
        option_type: Literal['S', 'NS', 'ALL'] = 'ALL',
        strike: Optional[float] = None,
        from_date: Optional[Timestamp] = None,
        to_date: Optional[Timestamp] = None,
) -> tuple[requests.Response, dict]:
    url = f'https://api.tdameritrade.com/v1/marketdata/chains'
    headers = {'Authorization': f'Bearer {authorization}'}
    params = {
        'symbol': symbol,
        'contractType': contract_type,
        'strikeCount': strike_count,
        'includeQuotes': include_quotes,
        'strategy': 'SINGLE',
        'range': range,
        'expiryMonth': expiry_month,
        'optionType': option_type,
        }

    if strike is not None:
        params['strike'] = strike

    if from_date is not None:
        params['fromDate'] = from_date.strftime(DFMT)

    if to_date is not None:
        params['toDate'] = to_date.strftime(DFMT)

    return requests.get(url, headers=headers, params=params), params


def get_price_history(
        authorization: str,
        symbol: str,
        period_type: Literal['day', 'month', 'year', 'ytd'] = 'day',
        period: int = 1,
        frequency_type: Literal['minute', 'daily', 'weekly', 'monthly'] = 'minute',
        frequency: int = 1,
        end_date: Optional[Timestamp] = None,
        start_date: Optional[Timestamp] = None,
) -> tuple[requests.Response, dict]:
    url = f'https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory'
    headers = {'Authorization': f'Bearer {authorization}'}
    params = {
        'periodType': period_type,
        'period': period,
        'frequencyType': frequency_type,
        'frequency': frequency,
    }

    if end_date is not None:
        params['endDate'] = end_date.strftime(DFMT)

    if start_date is not None:
        params['startDate'] = start_date.strftime(DFMT)

    return requests.get(url, headers=headers, params=params), params


def get_quote(
        authorization: str,
        symbol: str,
) -> tuple[requests.Response, dict]:
    url = f'https://api.tdameritrade.com/v1/marketdata/quotes'
    headers = {'Authorization': f'Bearer {authorization}'}
    params = {'symbol': symbol}

    return requests.get(url, headers=headers, params=params), params


"""
FORMATTED HTTP GET REQUESTS
"""


def fmt_instrument(res: requests.Response, params: dict) -> Data:
    match params['projection']:
        case 'symbol-search' | 'symbol-regex' | 'desc-search' | 'desc-regex':
            fields = (
                Field('cusip', str),
                Field('symbol', str),
                Field('description', str),
                Field('exchange', str),
                Field('assetType', str)
            )
            records = tuple(tuple(d.values()) for d in res.json().values())

        case 'fundamental':
            fields = (
                Field('symbol', str),
                Field('dividendAmount', float),
                Field('dividendDate', Timestamp),
                Field('dividendPayDate', Timestamp),
                Field('sharesOutstanding', float),
                Field('marketCap', float),
                Field('marketCapFloat', float),
                Field('beta', float)
            )
            res = res.json()[params['symbol']]['fundamental']
            records = tuple()
            for field in fields:
                if field.dtype == Timestamp:
                    records += (Timestamp(res[field.name], unit=DTUNIT),)
                else:
                    records += (res[field.name],)

        case _:
            raise ValueError(f'Invalid projection: {params["projection"]}')

    return Data(fields=fields, records=records)


def fmt_market_hours(res: requests.Response, params: dict) -> Data:
    _, outer = res.json()[params['markets'].lower()].popitem()
    inner = outer.pop('sessionHours')

    fields = (
        Field('date', Timestamp),
        Field('marketType', str),
        Field('exchange', str),
        Field('isOpen', bool),
        Field('preMarketStart', Timestamp),
        Field('preMarketEnd', Timestamp),
        Field('regularMarketStart', Timestamp),
        Field('regularMarketEnd', Timestamp),
        Field('postMarketStart', Timestamp),
        Field('postMarketEnd', Timestamp),
    )

    records = ((
        Timestamp(outer['date']),
        outer['marketType'],
        outer['exchange'],
        outer['isOpen'],
        Timestamp(inner['preMarket'][0]['start']),
        Timestamp(inner['preMarket'][0]['end']),
        Timestamp(inner['regularMarket'][0]['start']),
        Timestamp(inner['regularMarket'][0]['end']),
        Timestamp(inner['postMarket'][0]['start']),
        Timestamp(inner['postMarket'][0]['end'])
    ),)

    return Data(fields=fields, records=records)


def fmt_option_chain(res: requests.Response, params: dict) -> Data:
    calls = res.json()['callExpDateMap']
    puts = res.json()['putExpDateMap']
    expiries = tuple(calls.keys())
    strikes = tuple(_.keys() for _ in calls.values())

    data = tuple()
    fields = (
        Field('putCall', str),
        Field('symbol', str),
        Field('bid', float),
        Field('ask', float),
        Field('mark', float),
        Field('bidSize', int),
        Field('askSize', int),
        Field('tradeTimeInLong', Timestamp),
        Field('quoteTimeInLong', Timestamp),
        Field('volatility', float),
        Field('delta', float),
        Field('gamma', float),
        Field('theta', float),
        Field('vega', float),
        Field('rho', float),
        Field('openInterest', int),
        Field('strikePrice', float),
        Field('expirationDate', Timestamp),
        Field('multiplier', int),
    )

    for strike, expiry in zip(strikes, expiries):
        for K in strike:
            c = tuple()
            p = tuple()
            for field in fields:
                if field.dtype == Timestamp:
                    c += (Timestamp(calls[expiry][K][0][field.name], unit=DTUNIT),)
                    p += (Timestamp(puts[expiry][K][0][field.name], unit=DTUNIT),)
                else:
                    c += (calls[expiry][K][0][field.name],)
                    p += (puts[expiry][K][0][field.name],)
            data += (c,)
            data += (p,)

    return Data(fields=fields, records=data)


def fmt_price_history(res: requests.Response, params: dict) -> Data:
    fields = (
        Field('open', float),
        Field('high', float),
        Field('low', float),
        Field('close', float),
        Field('volume', int),
        Field('datetime', Timestamp),
    )

    records = tuple()
    for bar in res.json()['candles']:
        records += (
            bar['open'],
            bar['high'],
            bar['low'],
            bar['close'],
            bar['volume'],
            Timestamp(bar['datetime'], unit=DTUNIT),
        ),

    return Data(fields=fields, records=records)


def fmt_quote(res: requests.Response, params: dict) -> Data:
    _, data = res.json().popitem()
    return Data(
        fields=(
            Field('symbol', str),
            Field('bidPrice', float),
            Field('askPrice', float),
            Field('bidSize', int),
            Field('askSize', int),
            Field('quoteTimeInLong', Timestamp),
            Field('tradeTimeInLong', Timestamp),
        ),
        records=((
            data['symbol'],
            data['bidPrice'],
            data['askPrice'],
            data['bidSize'],
            data['askSize'],
            Timestamp(data['quoteTimeInLong'], unit=DTUNIT),
            Timestamp(data['tradeTimeInLong'], unit=DTUNIT),
        ),)
    )
