#!/usr/bin/env python3.8

import sys, time, os
import protocols as p
from storage import Storage
from reports import AccountsReport, HtmlReport
from datetime import datetime as dt
from typing import Dict
from data_config import accounts, lnames

timing = time.monotonic()
def t() -> str: return "%ds" % int(time.monotonic() - timing)

if len(sys.argv) != 7:
    sys.stderr.write('Usage: %s <start_ts> <days> <router> <directory> <output> <dbfile>\n' % sys.argv[0])
    sys.exit(1)

if sys.argv[1].lower() == 'now':
    start_ts = dt.now()
else: start_ts = dt.strptime(sys.argv[1], "%Y-%m-%d %H:%M")
days = int(sys.argv[2])
router = sys.argv[3]
directory = sys.argv[4]
outfile = sys.argv[5]
db_file = sys.argv[6]
pid = os.getpid()

print(pid, t(), router, 'load data from file %s' % repr(db_file), flush = True)
storage = Storage(db_file, accounts, directory, False)

reports = AccountsReport(lnames, accounts, storage)
print(pid, t(), router, 'calculate account usage', flush = True)
account_usage: Dict[str, Dict[p.Account, p.Usage]] = {}
for limit_name in lnames:
    account_usage[limit_name] = reports.account_usage(start_ts, router, limit_name)

print(pid, t(), router, 'calculate host usage', flush = True)
host_usage: Dict[str, Dict[p.Host, p.Usage]] = {}
for limit_name in lnames:
    host_usage[limit_name] = reports.host_usage(start_ts, router, limit_name)
print(pid, t(), router, 'calculate daily report', flush = True)
account_usage_daily = reports.account_usage_periodic(start_ts, router, days, period = 'day')
print(pid, t(), router, 'calculate hourly report', flush = True)
account_usage_hourly = reports.account_usage_periodic(start_ts, router, 31, period = 'hour')

print(pid, t(), router, 'generate html file to %s' % repr(outfile), flush = True)
header = '<a href="usage.html">nihonium</a> <a href="barium.html">barium</a> <a href="mikrotik.html">mikrotik</a> <a href="priv.html">priv</a>  <a href="/cgi-bin/adds_formular.py">additionals</a> <br/>'
footer = '<div class="bigblock"><h1>TELIA</h1><img src="telia_state.png" alt="Telia state"></div>'
with open(outfile + ".tmp", 'w') as fd:
    html_report = HtmlReport(router, lnames, accounts, account_usage, host_usage,
                             account_usage_daily, account_usage_hourly, storage.rest_adds,
                             header, footer, fd)
    html_report()
os.rename(outfile + ".tmp", outfile)

print(pid, t(), router, 'finish usage update', flush = True)
