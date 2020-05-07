import protocols as p
from datetime import datetime, timedelta
from utils import *
from typing import *

class AccountsReport(object):
    _limit_names: Tuple[str, ...]
    _accounts: Tuple[p.Account, ...]
    _storage: p.Storage
    
    def __init__(self,
                 limit_names: Tuple[str, ...],
                 accounts: Tuple[p.Account, ...],
                 storage: p.Storage):
        self._limit_names = limit_names
        self._accounts = accounts
        self._storage = storage

    def limits(self) -> Set[p.Limit]:
        result: Dict[p.Limit, Dict[p.Account, p.Usage]] = {}
        limits_set = set()
        for acc in self._accounts:
            for lname in acc.limit.limit_names:
                limits_set.add(acc.limit.limit(lname))
        return limits_set

    def account_usage(self, start_ts: datetime, router: str, limit_name: str) -> Dict[p.Account, p.Usage]:
        result: Dict[p.Account, p.Usage] = {}
        for account in self._accounts:
            if limit_name not in account.limit.limit_names:
                continue
            filters = []
            for host in account.hosts:
                filters.append("host = '%s'" % host.name)
            result[account] = self._storage.sum(start_ts, start_ts - account.limit.period(limit_name),
                                                "(%s) AND router = '%s'" % (' OR '.join(filters), router))
        return result

    def host_usage(self, start_ts: datetime, router: str, limit_name: str) -> Dict[p.Host, p.Usage]:
        result: Dict[p.Host, p.Usage] = {}
        for account in self._accounts:
            if limit_name not in account.limit.limit_names:
                continue
            for host in account.hosts:
                result[host] = self._storage.sum(start_ts, start_ts - account.limit.period(limit_name),
                                                 "host = '%s' AND router = '%s'" % (host.name, router))
        return result
