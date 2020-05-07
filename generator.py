#!/usr/bin/env python3

import random, sys, string, itertools, os, pickle
import protocols as p
from accounts import Account
from limits import Limit, LimitSet
from hosts import Host
from marks import Mark
from utils import *
from typing import *
from pprint import pprint as pp
from datetime import datetime, timedelta

ConfigType = Dict[str, Union[int, str, datetime,
                             Dict[p.AddsEntry, List[Tuple[datetime, str, str, int]]],
                             Dict[str, Dict[str, int]]]]
class TestDataGenerator(object):
    def __init__(self, path: str, start_ts: datetime, days: int, devider: int = 1):
        self.path = path
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.start_ts = start_ts
        self.days = days
        self.devider = devider
        self.adds: List[p.AddsEntry] = []
        self.allrows: List[Tuple[datetime, str, str, int]] = []
        self.config: ConfigType = {
            'test_router_adds': 0,
            'start_ts': self.start_ts,
            'days': self.days,
            '01_read_data_0': 0,
            '01_read_data_1': 0,
            '01_read_data_2': 0,
            '01_read_data_3': 0,
            'dat_without_adds': 0,
            'dat_with_adds': 0,
            'adds_rows': cast(Dict[p.AddsEntry, List[Tuple[datetime, str, str, int]]], {}),
            'account_usage': cast(Dict[str, Dict[str, int]], {}),
        }

    def commit(self) -> None:
        with open(os.path.join(self.path, 'accounts.dat'), 'wb') as accounts_fd:
            pickle.dump(self.accounts, accounts_fd)
        with open(os.path.join(self.path, 'hosts.dat'), 'wb') as hosts_fd:
            pickle.dump(self.hosts, hosts_fd)
        with open(os.path.join(self.path, 'limits.dat'), 'wb') as limits_fd:
            pickle.dump(self.limits, limits_fd)
        with open(os.path.join(self.path, 'routers.dat'), 'wb') as routers_fd:
            pickle.dump(self.routers, routers_fd)
        with open(os.path.join(self.path, 'config.dat'), 'wb') as config_fd:
            pickle.dump(self.config, config_fd)

    def generate_limitset(self) -> LimitSet:
        limit_nums = [1]
        limit_amounts: List[int] = [random.randint(1,3)]
        for nums in range(random.randint(2, 4)):
            limit_nums.append(limit_nums[-1] * random.randint(2, 4))
            limit_amounts.append(int(limit_amounts[-1] * limit_nums[-1] * (0.3 * random.random() + 0.5)))
        limit_names = ["%d days" % x for x in limit_nums]
        limitset = LimitSet(tuple(limit_names))
        for name, amount in zip(limit_names, limit_amounts):
            limitset.set(name, amount * GiB)
        return limitset

    def generate_accounts(self) -> None:
        short_names: Set[str] = set()
        long_names: Set[str] = set()
        limitsets: Set[LimitSet] = set()
        allhosts: Set[str] = set()
        routers: Set[str] = set()
        letters = string.ascii_lowercase
        while len(short_names) < 8:
            short_names.add(''.join(random.choice(letters) for _ in range(2)))
        while len(long_names) < 8:
            long_names.add(''.join(random.choice(letters) for _ in range(random.randint(5, 9))))
        while len(routers) < 3:
            routers.add(''.join(random.choice(letters) for _ in range(random.randint(5, 9))))
        while len(limitsets) < 3:
            limitsets.add(self.generate_limitset())
        accounts = []
        for sn, name in zip(short_names, long_names):
            generated_hosts: Set[str] = set()
            while len(generated_hosts) < random.randint(2, 7):
                new_host = ''.join(random.choice(letters) for _ in range(random.randint(3, 8)))
                if new_host in allhosts: continue
                generated_hosts.add(new_host)
                allhosts.add(new_host)
            hosts: Tuple[Host, ...] = tuple(Host(x) for x in generated_hosts)
            mark = random.choice(list(Mark))
            limit = random.choice(list(limitsets))
            acc = Account(sn, name, hosts, limit, mark)
            accounts.append(acc)
        self.accounts = {o.short: o for o in accounts}
        self.hosts = tuple(allhosts)
        self.limits = tuple(limitsets)
        self.routers = tuple(routers)
        self.config['test_host'] = random.choice(self.hosts)
        self.config['test_router'] = random.choice(self.routers)

    def generate_data(self) -> None:
        start_ts, days_number = self.start_ts, self.days
        for acc in self.accounts.values():
            for host in acc.hosts:
                dirname = os.path.join(self.path, host.name)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                for day in range(days_number):
                    fts = start_ts + timedelta(days = -day)
                    curday = fts.day
                    fd = open(os.path.join(dirname, "day_%04d%02d%02d" % (fts.year, fts.month, fts.day)), 'a')
                    for minutes in range(int(24 * 60 / self.devider)):
                        ts = fts + timedelta(minutes = -minutes)
                        self.config['01_read_data_0'] = 1 + cast(int, self.config['01_read_data_0'])
                        if ts >= (start_ts - timedelta(hours = 3)):
                            if host.name == self.config['test_host']:
                                self.config['01_read_data_3'] = 1 + cast(int, self.config['01_read_data_3'])
                            self.config['01_read_data_1'] = 1 + cast(int, self.config['01_read_data_1'])
                        if ts <= (start_ts - timedelta(hours = 3)):
                            self.config['01_read_data_2'] = 1 + cast(int, self.config['01_read_data_2'])
                        if ts.day != curday:
                            curday = ts.day
                            fd.close()
                            fd = open(os.path.join(dirname, "day_%04d%02d%02d" % (ts.year, ts.month, ts.day)), 'a')
                        dat_in = random.randint(13 * KiB, 17 * MiB)
                        dat_out = random.randint(13 * KiB, 7 * MiB)
                        dat_pkg = int((dat_in + dat_out) / 1470)
                        dat = dat_in + dat_out
                        router = random.choice(self.routers)
                        self.config['dat_without_adds'] = dat + cast(int, self.config['dat_without_adds'])
                        self.allrows.append((ts, router, host.name, dat))
                        fd.write("%d %d %d %d %d %s\n" % (ts.hour, ts.minute, dat_in, dat_out, dat_pkg, router))
                    fd.close()
        self.allrows.sort()
        rows_to_change = []
        for curadd in sorted(self.adds):
            if curadd[1] != 'boost': continue
            currouter = curadd[2]
            curhours = cast(timedelta, curadd[4])
            for rowid in range(len(self.allrows)):
                row = self.allrows[rowid]
                ts, router, rh, dat = row
                if ts >= curadd[0] and router == currouter and ts <= (curadd[0] + curhours):
                    adds_rows = cast(Dict[p.AddsEntry, List[Tuple[datetime, str, str, int]]], self.config['adds_rows'])
                    adds_rows.setdefault(curadd, []).append(row)
                    rows_to_change.append((rowid, (ts, router, rh, 0)))
        for rowid, row in rows_to_change:
            self.allrows[rowid] = row
        rows_to_change = []
        for curadd in sorted(self.adds):
            if curadd[1] != 'data': continue
            currouter = curadd[2]
            curhost = curadd[3]
            curamount = cast(int, curadd[4])
            for rowid in range(len(self.allrows)):
                row = self.allrows[rowid]
                ts, router, rh, dat = row
                if ts >= curadd[0] and router == currouter and rh == curhost:
                    adds_rows = cast(Dict[p.AddsEntry, List[Tuple[datetime, str, str, int]]], self.config['adds_rows'])
                    adds_rows.setdefault(curadd, []).append(row)
                    amount = min(curamount, dat)
                    rows_to_change.append((rowid, (ts, router, rh, dat - amount)))
                    curamount -= amount
                    if curamount == 0: break
        for rowid, row in rows_to_change:
            self.allrows[rowid] = row
        for row in self.allrows:
            self.config['dat_with_adds'] = cast(int, self.config['dat_with_adds']) + row[3]

    def generate_reports(self) -> None:
        limit_names_set = set()
        for limit in self.limits:
            for limit_name in limit.limit_names:
                limit_names_set.add(limit_name)
        limit_names = tuple(sorted(limit_names_set))
        usage_data: Dict[str, Dict[str, int]] = {}
        for limit_name in limit_names:
            usage_data[limit_name] = {}
            for account in self.accounts.values():
                if limit_name not in account.limit.limit_names:
                    continue
                host_names = set(x.name for x in account.hosts)
                start_ts = self.start_ts
                end_ts = self.start_ts - account.limit.period(limit_name)
                usage_dat = 0
                for ts, router, host, dat in self.allrows:
                    if host in host_names and ts <= start_ts and ts >= end_ts and router == self.config['test_router']:
                        usage_dat += dat
                usage_data[limit_name][account.name] = usage_dat
        self.config['account_usage'] = usage_data

    def generate_adds(self) -> None:
        start_ts, days_number = self.start_ts, self.days
        with open(os.path.join(self.path, 'additional_contingent.dat'), 'w') as fd:
            adds_number = random.randint(13, 27)
            for _ in range(adds_number):
                adds_type = random.choice(('boost', 'data', 'data', 'data'))
                adds_router = random.choice(self.routers)
                if adds_router == self.config['test_router']:
                    self.config['test_router_adds'] = 1 + cast(int, self.config['test_router_adds'])
                adds_start_offset = random.randint(int((start_ts - timedelta(days = days_number)).timestamp()),
                                                   int((start_ts + timedelta(hours = 13)).timestamp()))
                tmp = datetime.fromtimestamp(adds_start_offset)
                adds_start = datetime(tmp.year, tmp.month, tmp.day, tmp.hour, tmp.minute)
                if adds_type == 'boost':
                    adds_hours = random.randint(3, 17)
                    self.adds.append((adds_start, adds_type, adds_router, None, timedelta(hours = adds_hours)))
                    fd.write('{} boost {} {}h\n'.format(adds_start.strftime('%Y-%m-%d %H:%M'),
                                                        adds_router, adds_hours))
                elif adds_type == 'data':
                    adds_host = random.choice(self.hosts)
                    adds_amount = units2bytes(bytes2units(random.randint(1*MiB, 30*GiB)))
                    self.adds.append((adds_start, adds_type, adds_router, adds_host, adds_amount))
                    fd.write('{} data {} {} {}\n'.format(adds_start.strftime('%Y-%m-%d %H:%M'),
                                                         adds_router, adds_host,
                                                         bytes2units(adds_amount)))

    def generate(self) -> None:
        self.generate_accounts()
        self.generate_adds()
        self.generate_data()
        self.generate_reports()
        self.commit()

if __name__ == '__main__':
    ts = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M")
    generator = TestDataGenerator(sys.argv[1],
                                  datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, 0),
                                  int(sys.argv[3]),
                                  int(sys.argv[4]) if len(sys.argv) > 4 else 1)
    generator.generate()
