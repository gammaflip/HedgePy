import re
import requests
from pandas import Timestamp, DateOffset, Timedelta, date_range, bdate_range
from pandas.tseries import holiday as _holiday
from numpy import datetime64 as _datetime64
from typing import Union


class Tenor:
    _map = {'y': 'year', 'm': 'month', 'w': 'week', 'd': 'day',
            'h': 'hour', 't': 'minute', 's': 'second',
            'u': 'microsecond', 'n': 'nanosecond'}
    _regex = re.compile(r'(?P<num>-?\d+)(?P<unit>[ymwdhtsun])')

    def __init__(self, s: str):
        self.num, self.unit = self._regex.match(s).groups()

    def __str__(self):
        return ''.join([self.num, self.unit])

    def __repr__(self):
        return f'Tenor: {self.num} {self._map[self.unit]}'

    @classmethod
    def from_tuple(cls, tup: tuple[str, str]):
        return cls(tup[0] + tup[1])

    def date_offset(self):
        return DateOffset(**{self._map[self.unit]: int(self.num)})


class FwdTenor(Tenor):
    def __init__(self, s: str):
        self.fwd, self.tail = \
            super.__init__()
#            super().from_tuple(self._regex.findall(s)[0]), super().from_tuple(self._regex.findall(s)[1])

    def __str__(self):
        return ''.join([self.fwd.num, self.fwd.unit, self.tail.num, self.tail.unit])

    def __repr__(self):
        return f'Forward: {self.fwd.num} {self._map[self.fwd.unit]}'.rstrip('s') + ' / ' + \
               f'Tenor: {self.tail.num} {self._map[self.tail.unit]}'.rstrip('s')

    @classmethod
    def from_tuple(cls, tup: tuple[tuple[str, str], tuple[str, str]]):
        return cls(tup[0][0] + tup[0][1] + tup[1][0] + tup[1][1])


class Calendar(_holiday.AbstractHolidayCalendar):
    _ISLAMFIVE = 'Sun Mon Tue Wed Thu'
    _ISLAMSIX = 'Sun Mon Tue Wed Thu Sat'
    _WESTERN = 'Mon Tue Wed Thu Fri'
    _weekmasks = {364: _ISLAMSIX, 376: _ISLAMFIVE, 12: _ISLAMFIVE, 4: _ISLAMFIVE, 48: _ISLAMFIVE, 818: _ISLAMFIVE,
                  368: _ISLAMFIVE, 400: _ISLAMFIVE, 414: _ISLAMFIVE, 434: _ISLAMFIVE, 462: _ISLAMFIVE, 512: _ISLAMFIVE,
                  275: _ISLAMFIVE, 634: _ISLAMFIVE, 682: _ISLAMFIVE, 729: _ISLAMFIVE, 760: _ISLAMFIVE, 887: _ISLAMFIVE}

    def __init__(self, country_iso: int = None):
        self.weekmask = self._weekmasks[country_iso] if country_iso in self._weekmasks.keys() else self._WESTERN

    def weekdays(self):
        pass
