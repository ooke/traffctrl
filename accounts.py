import protocols as p
from marks import Mark
from typing import Tuple

class Account(p.Account):
    def __init__(self, short: str, name: str,
                 hosts: Tuple[p.Host, ...],
                 limit: p.LimitSet,
                 mark: Mark,
                 color: str = '#000',
                 ignore: bool = False,
                 no_hardlimit: bool = False) -> None:
        self._short = short
        self._name = name
        self._hosts = hosts
        self._limit = limit
        self._mark = mark
        self._color = color
        self._ignore = ignore
        self._no_hardlimit = no_hardlimit

    def _set_storage(self, storage: p.Storage) -> None: self._storage = storage
    @property
    def short(self) -> str: return self._short
    @property
    def name(self) -> str: return self._name
    @property
    def hosts(self) -> Tuple[p.Host, ...]: return self._hosts
    def host_by_name(self, host_name: str) -> p.Host:
        for host in self._hosts:
            if host.name == host_name:
                return host
        raise ValueError('Host with name %s is not in the list.' % repr(host_name))
    @property
    def limit(self) -> p.LimitSet: return self._limit
    @property
    def mark(self) -> Mark: return self._mark
    @property
    def color(self) -> str: return self._color
    
    def amount(self, limit_name: str) -> int:
        return self._limit.limit(limit_name).amount

    @property
    def ignore(self) -> bool: return self._ignore
    @property
    def no_hardlimit(self) -> bool: return self._no_hardlimit

    def __repr__(self) -> str:
        hosts = ','.join((x.name for x in self.hosts))
        mark = str(self.mark)
        return "<%s: %s %s %s %s>" % (self.short, self.name, hosts, mark, repr(self.limit))        
