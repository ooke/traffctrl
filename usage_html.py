#!/usr/bin/env python3.8

import sys, time, os
import protocols as p
from storage import Storage
from filtering import Filtering
from additionals import Additionals
from reports import AccountsReport, HtmlReport
from datetime import datetime as dt
from typing import Dict
from data_config import addsfile, accounts, lnames

timing = time.monotonic()
def t() -> str: return "%ds" % int(time.monotonic() - timing)

if len(sys.argv) != 7:
    sys.stderr.write('Usage: %s <start_ts> <days> <router> <directory> <output> <firewall>\n' % sys.argv[0])
    sys.exit(1)

if sys.argv[1].lower() == 'now':
    start_ts = dt.now()
else: start_ts = dt.strptime(sys.argv[1], "%Y-%m-%d %H:%M")
days = int(sys.argv[2])
router = sys.argv[3]
directory = sys.argv[4]
outfile = sys.argv[5]
firewall = (sys.argv[6].strip().lower() == 'on')
pid = os.getpid()

print(pid, t(), router, 'load data from directory %s' % repr(directory), flush = True)
storage = Storage(accounts, directory)
storage.load_data(start_ts, days)

print(pid, t(), router, 'load additionals from file %s' % repr(addsfile), flush = True)
additionals = Additionals(addsfile(directory))
rest_adds = additionals.apply_to_storage(storage)

reports = AccountsReport(lnames, accounts, storage)
print(pid, t(), router, 'calculate account usage', flush = True)
account_usage: Dict[str, Dict[p.Account, p.Usage]] = {}
for limit_name in lnames:
    account_usage[limit_name] = reports.account_usage(start_ts, router, limit_name)

child = os.fork()
if child == 0:
    mypid = os.getpid()
    print(pid, mypid, t(), router, 'calculate host usage', flush = True)
    host_usage: Dict[str, Dict[p.Host, p.Usage]] = {}
    for limit_name in lnames:
        host_usage[limit_name] = reports.host_usage(start_ts, router, limit_name)
    print(pid, mypid, t(), router, 'calculate daily report', flush = True)
    account_usage_daily = reports.account_usage_periodic(start_ts, router, days, period = 'day')
    print(pid, mypid, t(), router, 'calculate hourly report', flush = True)
    account_usage_hourly = reports.account_usage_periodic(start_ts, router, 31, period = 'hour')

    print(pid, mypid, t(), router, 'generate html file to %s' % repr(outfile), flush = True)
    header = '<a href="usage.html">nihonium</a> <a href="barium.html">barium</a> <a href="mikrotik.html">mikrotik</a> <a href="priv.html">priv</a>  <a href="/cgi-bin/adds_formular.py">additionals</a> <br/>'
    footer = '<div class="bigblock"><h1>TELIA</h1><img src="telia_state.png" alt="Telia state"></div>'
    with open(outfile + ".tmp", 'w') as fd:
        html_report = HtmlReport(router, lnames, accounts, account_usage, host_usage,
                                 account_usage_daily, account_usage_hourly, rest_adds,
                                 header, footer, fd)
        html_report()
    os.rename(outfile + ".tmp", outfile)

    print(pid, mypid, t(), router, 'file generated.')
    sys.exit(0)

if firewall:
    child = os.fork()
    if child == 0:
        mypid = os.getpid()
        print(pid, mypid, t(), router, 'configure firewall', flush = True)
        filtering = Filtering(lnames, accounts, account_usage)
        filtering.filter(directory, rest_adds)
        print(pid, mypid, t(), router, 'firewall configured.')
        sys.exit(0)

print(pid, t(), router, 'finish main process', flush = True)
