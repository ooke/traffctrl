#!/usr/bin/env python3

import unittest, os, random, pickle, sys
import protocols as p
from accounts import Account
from hosts import Host
from limits import Limit, LimitSet
from marks import Mark
from storage import Storage
from additionals import Additionals
from reports import AccountsReport
from datetime import datetime as dt, timedelta as td
from pprint import pp
from utils import *
from typing import *

class BasicTests(unittest.TestCase):
    _data_path: str
    _adds_path: str
    config: Dict[str, Any]
    start_ts: dt
    days: int
    accounts: Dict[str, p.Account]
    hosts: Tuple[str, ...]
    limits: Tuple[LimitSet, ...]
    routers: Tuple[str, ...]
    stor: p.Storage
    adds: p.Additionals
    adds_applied: bool
    
    @classmethod
    def setUpClass(self) -> None:
        self._data_path = './data'
        if not os.path.exists(self._data_path):
            raise RuntimeError("Path %s does not exist" % repr(self._data_path))
        self._adds_path = os.path.join(self._data_path, 'additional_contingent.dat')
        with open(os.path.join(self._data_path, 'config.dat'), 'rb') as config_fd:
            self.config = pickle.load(config_fd)
        #pp(self.config, stream = sys.stderr)
        self.start_ts = self.config['start_ts']
        self.days = self.config['days']
        with open(os.path.join(self._data_path, 'accounts.dat'), 'rb') as accounts_fd:
            self.accounts = pickle.load(accounts_fd)
        with open(os.path.join(self._data_path, 'hosts.dat'), 'rb') as hosts_fd:
            self.hosts = pickle.load(hosts_fd)
        with open(os.path.join(self._data_path, 'limits.dat'), 'rb') as limits_fd:
            self.limits = pickle.load(limits_fd)
        with open(os.path.join(self._data_path, 'routers.dat'), 'rb') as routers_fd:
            self.routers = pickle.load(routers_fd)
        self.stor = Storage(tuple(self.accounts.values()), self._data_path)
        self.stor.load_data(self.start_ts, self.days + 1)
        self.adds = Additionals(self._adds_path)
        self.adds_applied = False

    def test_01_read_data(self) -> None:
        ret = self.stor._conn.execute('SELECT COUNT(*) FROM data').fetchone()
        self.assertEqual(ret[0], self.config['01_read_data_0'])
        
    def test_02_read_data(self) -> None:
        res0 = []; self.stor.apply_mask(self.start_ts - td(days = self.days+1),
                                        lambda row: res0.append(row))
        self.assertEqual(len(res0), self.config['01_read_data_0'])

    def test_03_read_data(self) -> None:
        res1 = []; self.stor.apply_mask(self.start_ts - td(hours = 3),
                                        lambda row: res1.append(row))
        self.assertEqual(len(res1), self.config['01_read_data_1'])
        
    def test_04_read_data(self) -> None:
        res2 = []; self.stor.apply_mask(self.start_ts - td(hours = 3),
                                        lambda row: res2.append(row),
                                        direction = 'past')
        self.assertEqual(len(res2), self.config['01_read_data_2'])
        
    def test_05_read_data(self) -> None:
        res3 = []; self.stor.apply_mask(self.start_ts - td(hours = 3),
                                        lambda row: res3.append(row),
                                        flt = "host = '%s'" % self.config['test_host'])
        self.assertEqual(len(res3), self.config['01_read_data_3'])

    def test_20_additionals_read(self) -> None:
        self.assertEqual(tuple(sorted(self.adds.routers)), tuple(sorted(self.routers)))
        self.assertEqual(len(list(self.adds[self.config['test_router']])),
                         self.config['test_router_adds'])
        
    def test_30_additionals_data_entries(self) -> None:
        adds = self.adds
        data = {'add': 30000,  'amount': 0}
        ts = self.start_ts - td(hours = 3)
        entries: List[Tuple[int, int]] = []
        rows: List[Tuple[int, int]] = []
        for row in self.stor._conn.execute('SELECT * FROM data WHERE year = %d AND month = %d AND day = %d AND hour = %d AND minute >= %d' \
                                           % (ts.year, ts.month, ts.day, ts.hour, ts.minute)):
            if adds._apply_data_entries(row, ts, data, entries) != False:
                rows.append((row[0], row[10]))
        self.assertEqual(data, {'add': 0, 'amount': 30000})

    def test_40_additionals_boost_entries(self) -> None:
        adds = self.adds
        ts = self.start_ts - td(days = 1)
        ets = ts + td(minutes = 1)
        entries: List[int] = []
        rows: List[int] = []
        for row in self.stor._conn.execute('SELECT * FROM data WHERE year = %d AND month = %d AND day = %d AND hour = %d AND minute > %d' \
                                           % (ts.year, ts.month, ts.day, ts.hour, ts.minute)):
            if adds._apply_boost_entries(row, ets, entries) != False:
                self.assertEqual(tuple(row[2:7]), (ets.year, ets.month, ets.day, ets.hour, ets.minute))

    def test_50_additionals_entries(self) -> None:
        datsum_before = self.stor._conn.execute('SELECT SUM(dat) FROM data').fetchone()[0]
        self.assertEqual(self.config['dat_without_adds'], datsum_before)
        collect_rows: Dict[p.AddsEntry, List[p.DataRow]] = {}
        self.assertFalse(self.adds_applied)
        self.adds.apply_to_storage(self.stor, collect_rows)
        self.adds_applied = True
        datsum_after = self.stor._conn.execute('SELECT SUM(dat) FROM data').fetchone()[0]
        gen_adds_set = set()
        for adds_name in self.adds.routers:
            for adds in self.adds[adds_name]:
                gen_adds_set.add(adds)
        self.assertEqual(list(sorted(gen_adds_set)), list(sorted(collect_rows.keys())))
        for adds_name in self.adds.routers:
            for adds in self.adds[adds_name]:
               if adds not in self.config['adds_rows']: continue
               for ra, rb in zip(sorted(self.config['adds_rows'][adds]),
                                 sorted(map(lambda r: (dt(r[2], r[3], r[4], r[5], r[6]), r[11], r[1], r[10]),
                                            collect_rows[adds]))):
                   self.assertEqual(ra, rb)
        self.assertEqual(self.config['dat_with_adds'], datsum_after)

    def test_60_reports(self) -> None:
        if not self.adds_applied:
            self.adds.apply_to_storage(self.stor)
            self.adds_applied = True
        check_result: Dict[str, Dict[str, int]] = {}
        limit_names_set = set()
        for acc in self.accounts.values():
            for lname in acc.limit.limit_names:
                limit_names_set.add(lname)
        limit_names = tuple(sorted(limit_names_set))
        reports = AccountsReport(limit_names, tuple(self.accounts.values()), self.stor)
        for limit in limit_names:
            result = reports.account_usage(self.start_ts, self.config['test_router'], limit)
            check_result[limit] = {}
            for account, usage in result.items():
                check_result[limit][account.name] = usage.dat
        for limit in limit_names:
            self.assertEqual(check_result[limit], self.config['account_usage'][limit])

    def test_61_reports(self) -> None:
        if not self.adds_applied:
            self.adds.apply_to_storage(self.stor)
            self.adds_applied = True
        limits_names_set = set()
        for acc in self.accounts.values():
            for lname in acc.limit.limit_names:
                limits_names_set.add((acc.limit.period(lname), lname))
        limit_names = tuple(x[1] for x in sorted(limits_names_set, key = lambda x: x[0]))
        reports = AccountsReport(limit_names, tuple(self.accounts.values()), self.stor)
        print()
        for limit in limit_names:
            result = reports.account_usage(self.start_ts, self.config['test_router'], limit)
            print(limit)
            for acc, usage in sorted(result.items(), key = lambda x: x[0].name):
                print(acc.name, bytes2units(usage.dat), acc.limit.limit(limit).amount_text)

if __name__ == '__main__':
    unittest.main()
