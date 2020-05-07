import re, protocols
from datetime import timedelta
from utils import *
from typing import *

class Limit(protocols.Limit):
    def __init__(self, name: str, amount: int, period: int):
        self._name = name
        self._amount = amount
        self._period = period

    @property
    def name(self) -> str: return self._name
    @property
    def period(self) -> timedelta: return timedelta(days = self._period)
    @property
    def amount(self) -> int: return self._amount
    @property
    def amount_text(self) -> str: return bytes2units(self._amount, ' ')
    @property
    def amount_html(self) -> str: return bytes2units(self._amount, '&nbsp;')

class LimitSet(protocols.LimitSet):
    def __init__(self, limit_names: Tuple[str, ...]) -> None:
        self._limit_names: Dict[str, int] = {}
        for lname in limit_names:
            ma_name = re.match(r'([0-9]+) days?', lname)
            if ma_name is None:
                raise RuntimeError('Failed to parse limit name %s' % repr(lname))
            period_days = int(ma_name.group(1))
            if period_days < 1:
                raise RuntimeError('Limit name with period under one day is not allowed.')
            self._limit_names[lname] = period_days
        self._limits: Dict[str, Limit] = {}

    def _check(self, limit: str) -> str:
        if limit not in self._limit_names:
            raise RuntimeError('Limit name %s is not in known limit names: %s' \
                               % (repr(limit), repr(self._limit_names)))
        return limit

    @property
    def limit_names(self) -> Tuple[str, ...]:
        return tuple(self._limit_names)
    def set(self, limit: str, amount: int) -> "LimitSet":
        limit = self._check(limit)
        self._limits[limit] = Limit(limit, amount, self._limit_names[limit])
        return self
    def limit(self, limit: str) -> Limit:
        return self._limits.get(self._check(limit), Limit(limit, 0, 0))
    def __call__(self, limit: str) -> Limit:
        return self._limits.get(self._check(limit), Limit(limit, 0, 0))
    def period(self, limit: str) -> timedelta:
        return timedelta(days = self._limit_names[self._check(limit)])

    def __repr__(self) -> str:
        limits = []
        for x in self.limit_names:
            limits.append('%s: %s' % (x, self.limit(x).amount_text))
        return '<%s>' % ', '.join(limits)
