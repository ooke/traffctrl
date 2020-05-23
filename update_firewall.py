#!/usr/bin/env python3.8

import sys, time, os
import protocols as p
from storage import Storage
from filtering import Filtering
from reports import AccountsReport
from datetime import datetime as dt
from data_config import accounts, lnames
from typing import Dict

timing = time.monotonic()
def t() -> str: return "%ds" % int(time.monotonic() - timing)

if len(sys.argv) != 6:
    sys.stderr.write('Usage: %s <start_ts> <days> <router> <directory> <dbfile>\n' % sys.argv[0])
    sys.exit(1)

if sys.argv[1].lower() == 'now':
    start_ts = dt.now()
else: start_ts = dt.strptime(sys.argv[1], "%Y-%m-%d %H:%M")
days = int(sys.argv[2])
router = sys.argv[3]
directory = sys.argv[4]
db_file = sys.argv[5]
pid = os.getpid()

print(pid, t(), router, 'load data from file %s' % repr(db_file), flush = True)
storage = Storage(db_file, accounts, directory, False)

reports = AccountsReport(lnames, accounts, storage)
print(pid, t(), router, 'calculate account usage', flush = True)
account_usage: Dict[str, Dict[p.Account, p.Usage]] = {}
for limit_name in lnames:
    account_usage[limit_name] = reports.account_usage(start_ts, router, limit_name)

print(pid, t(), router, 'configure firewall', flush = True)
filtering = Filtering(lnames, accounts, account_usage)
filtering.filter(directory, storage.rest_adds)

print(pid, t(), router, 'firewall configured.')
