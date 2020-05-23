#!/usr/bin/env python3.8

import sys
import IPython # type: ignore
import protocols as p
from typing import Dict
from data_config import accounts, lnames
from storage import Storage
from reports import AccountsReport
from datetime import datetime as dt

if sys.argv[1].lower() == 'now':
    start_ts = dt.now()
else: start_ts = dt.strptime(sys.argv[1], "%Y-%m-%d %H:%M")
days = int(sys.argv[2])
router = sys.argv[3]
directory = sys.argv[4]
db_file = sys.argv[5]

storage = Storage(db_file, accounts, directory, False)
reports = AccountsReport(lnames, accounts, storage)
host_usage: Dict[str, Dict[p.Host, p.Usage]] = {}
for limit_name in lnames:
    host_usage[limit_name] = reports.host_usage(start_ts, router, limit_name)
IPython.embed()
