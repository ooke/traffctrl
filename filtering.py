import os
import protocols as p
from marks import *
from typing import *
from utils import *

class Filtering(object):
    def __init__(self,
                 limit_names: Tuple[str, ...],
                 accounts: Tuple[p.Account, ...],
                 account_usage: Dict[str, Dict[p.Account, p.Usage]]) -> None:
        self._limit_names = limit_names
        self._accounts = accounts
        self._account_usage = account_usage

    def filter(self, directory: str, cmd: Callable[[str], Any] = os.system) -> None:
        if not os.path.exists('/sbin/iptables'): return
        cmd('''/sbin/iptables -t mangle -L FORWARD >%s/iptables.cache''' % directory)
        iptcache = '''cat %s/iptables.cache''' % directory
        cmd('''/sbin/iptables -t filter -L FORWARD >%s/iptablesf.cache''' % directory)
        ipfcache = '''cat %s/iptablesf.cache''' % directory
        cmd('''/sbin/iptables -t nat -L >%s/iptables.cache2''' % directory)
        iptcache2 = '''cat %s/iptables.cache2''' % directory
        for account in self._accounts:
            limited, hardlimited = False, False
            if not account.ignore:
                for limit in sorted(self._limit_names):
                    if limit not in self._account_usage \
                       or account not in self._account_usage[limit]:
                        continue
                    usage = self._account_usage[limit][account]
                    percent = int(usage.dat / account.limit(limit).amount * 100)
                    if percent >= 100:
                        for host in account.hosts:
                            mark = account.mark
                            for curmark in Mark:
                                if mark == curmark: continue
                                cmd('''%s | grep -q -w '%s.kozachuk.info.*MARK set %s' && { set -x; /sbin/iptables -t mangle -D FORWARD -s %s.kozachuk.info -j MARK --set-mark %d; }''' \
                                          % (iptcache, host.name, hex(curmark.value), host.name, curmark.value))
                                cmd('''%s | grep -q -w '%s.kozachuk.info.*MARK set %s' && { set -x; /sbin/iptables -t mangle -D FORWARD -d %s.kozachuk.info -j MARK --set-mark %d; }''' \
                                          % (iptcache, host.name, hex(curmark.value), host.name, curmark.value))
                            cmd('''%s | grep -q '\\<%s.kozachuk.info\\>.*anywhere' || { set -x; /sbin/iptables -t mangle -I FORWARD -s %s.kozachuk.info -j MARK --set-mark %d; }''' \
                                      % (iptcache, host.name, host.name, mark.value))
                            cmd('''%s | grep -q 'anywhere.*\\<%s.kozachuk.info\\>' || { set -x; /sbin/iptables -t mangle -I FORWARD -d %s.kozachuk.info -j MARK --set-mark %d; }''' \
                                      % (iptcache, host.name, host.name, mark.value))
                        limited = True
                        if percent >= 120 and not account.no_hardlimit:
                            for host in account.hosts:
                                cmd('''%s | grep -q "REJECT.*%s.kozachuk.info.*anywhere" || { set -x; /sbin/iptables -t filter -I FORWARD -s %s.kozachuk.info -j REJECT; }''' \
                                          % (ipfcache, host.name, host.name))
                                cmd('''%s | grep -q "REJECT.*anywhere.*%s.kozachuk.info" || { set -x; /sbin/iptables -t filter -I FORWARD -d %s.kozachuk.info -j REJECT; }''' \
                                          % (ipfcache, host.name, host.name))
                            hardlimited = True
                        break
            if not limited:
                for host in account.hosts:
                    for mark in Mark:
                        cmd('''%s | grep -q -w '%s.kozachuk.info' && { set -x; /sbin/iptables -t mangle -D FORWARD -s %s.kozachuk.info -j MARK --set-mark %d >/dev/null 2>&1; }''' \
                                  % (iptcache, host.name, host.name, mark.value))
                        cmd('''%s | grep -q -w '%s.kozachuk.info' && { set -x; /sbin/iptables -t mangle -D FORWARD -d %s.kozachuk.info -j MARK --set-mark %d >/dev/null 2>&1; }''' \
                                  % (iptcache, host.name, host.name, mark.value))
            if not hardlimited:
                for host in account.hosts:
                    cmd('''%s | grep -q "REJECT.*%s.kozachuk.info.*anywhere" && { set -x; /sbin/iptables -t filter -D FORWARD -s %s.kozachuk.info/32 -j REJECT; }''' \
                              % (ipfcache, host.name, host.name))
                    cmd('''%s | grep -q "REJECT.*anywhere.*%s.kozachuk.info" && { set -x; /sbin/iptables -t filter -D FORWARD -d %s.kozachuk.info/32 -j REJECT; }''' \
                              % (ipfcache, host.name, host.name))
        cmd('/sbin/iptables -t mangle -L FORWARD -x -v 2>&1 | grep -q "CONNMARK save" || /sbin/iptables -t mangle -A FORWARD -j CONNMARK --save-mark')
