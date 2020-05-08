import protocols
from limits import LimitSet, Limit
from marks import Mark
from typing import *

class Account(protocols.Account):
    def __init__(self, short: str, name: str,
                 hosts: Tuple[protocols.Host, ...],
                 limit: protocols.LimitSet, mark: Mark) -> None:
        self._short = short
        self._name = name
        self._hosts = hosts
        self._limit = limit
        self._mark = mark

    def _set_storage(self, storage: protocols.Storage) -> None: self._storage = storage
    @property
    def short(self) -> str: return self._short
    @property
    def name(self) -> str: return self._name
    @property
    def hosts(self) -> Tuple[protocols.Host, ...]: return self._hosts
    def host_by_name(self, host_name: str) -> protocols.Host:
        for host in self._hosts:
            if host.name == host_name:
                return host
        raise ValueError('Host with name %s is not in the list.' % repr(host_name))
    @property
    def limit(self) -> protocols.LimitSet: return self._limit
    @property
    def mark(self) -> Mark: return self._mark
    
    def amount(self, limit_name: str) -> int:
        return self._limit.limit(limit_name).amount

    def __repr__(self) -> str:
        hosts = ','.join((x.name for x in self.hosts))
        mark = str(self.mark)
        return "<%s: %s %s %s %s>" % (self.short, self.name, hosts, mark, repr(self.limit))
        
