#!/usr/bin/env python3.8

import sys, time, os
import protocols as p
from storage import Storage
from accounts import Account
from hosts import Host
from limits import Limit, LimitSet
from filtering import Filtering
from marks import Mark
from additionals import Additionals
from reports import AccountsReport, HtmlReport
from datetime import datetime as dt, timedelta as td
from typing import *
from utils import *

if len(sys.argv) != 6:
    sys.stderr.write('Usage: %s <start_ts> <days> <router> <directory> <output>\n' % sys.argv[0])
    sys.exit(1)

sv_devs = (Host('ltn'),Host('bc'),Host('pi'),Host('klima'),Host('wolfram'),Host('erbium'),Host('downloads'),Host('chat'),Host('xenon'),Host('tantal'),Host('thorium'),Host('lithium'),Host('bohrium'),Host('barium'),Host('priv'),Host('nihonium'),Host('nswitch'),Host('idrac1'),Host('hpprinter'))
ok_devs = (Host('ok2mm'),Host('exadell'),Host('exatablet'),Host('neodym'),Host('thamium'))
mm_devs = (Host('mmipad'),Host('okiphone'))
ie_devs = (Host('irislt'),Host('mmiphone'),Host('irisphone'))
aa_devs = (Host('fafina'),Host('mmiphone4'),Host('afinaphone'),Host('afinalt'))
sc_devs = (Host('lutetium'),Host('switch'),Host('hafnium'),Host('pspsm'),Host('terbium'))
vo_devs = (Host('fvin'),Host('vinphone'))
bm_devs = (Host('bmainsw3'),Host('bmainsw4'),Host('bmainsw5'),Host('bmainsw6'))
hd_devs = (Host('canald'),Host('arisa'))
se_devs = (Host('irisipad'),Host('afinaipad'),Host('vindev'))

lnames = ('1 day', '3 days', '7 days', '14 days', '30 days', '90 days')
ok_limit = LimitSet(lnames).set('1 day',  15*GiB).set('3 days',  25*GiB).set('7 days',  50*GiB).set('14 days', 60*GiB).set('30 days', 120*GiB).set('90 days', 360*GiB)
mm_limit = LimitSet(lnames).set('1 day',  10*GiB).set('3 days',  25*GiB).set('7 days',  50*GiB).set('14 days', 60*GiB).set('30 days', 120*GiB).set('90 days', 360*GiB)
kk_limit = LimitSet(lnames).set('1 day',   4*GiB).set('3 days',  12*GiB).set('7 days',  18*GiB).set('14 days', 35*GiB).set('30 days',  55*GiB).set('90 days', 150*GiB)
hd_limit = LimitSet(lnames).set('1 day', 600*MiB).set('3 days',   1*GiB).set('7 days',   2*GiB).set('14 days',  4*GiB).set('30 days',   8*GiB).set('90 days',  24*GiB)
cc_limit = LimitSet(lnames).set('1 day',   8*GiB).set('3 days',  20*GiB).set('7 days',  30*GiB).set('14 days', 50*GiB).set('30 days',  70*GiB).set('90 days', 100*GiB)
bm_limit = LimitSet(lnames).set('1 day',  80*MiB).set('3 days', 250*MiB).set('7 days', 500*MiB).set('14 days',  1*GiB).set('30 days',   2*GiB).set('90 days',   6*GiB)

accounts = (
    Account('aa', 'Afina', aa_devs, kk_limit, Mark.M512KBIT, '#006BA6'),
    Account('bm', 'Mining', bm_devs, bm_limit, Mark.M128KBIT, '#483D3F'),
    Account('hd', 'Home', hd_devs, hd_limit, Mark.M64KBIT, '#FFBC42', no_hardlimit = True),
    Account('ie', 'Iris', ie_devs, kk_limit, Mark.M512KBIT, '#D81159'),
    Account('mm', 'Mama', mm_devs, mm_limit, Mark.M1MBIT, '#B2A4D4', no_hardlimit = True),
    Account('ok', 'Papa', ok_devs, ok_limit, Mark.M1MBIT, '#0496FF', no_hardlimit = True),
    Account('sc', 'Consoles', sc_devs, cc_limit, Mark.M512KBIT, '#86BBD8'),
    Account('se', 'School', se_devs, cc_limit, Mark.M1MBIT, '#F69FD6', no_hardlimit = True),
    Account('sv', 'Servers', sv_devs, cc_limit, Mark.M128KBIT, '#800000', ignore = True),
    Account('vo', 'Vincent', vo_devs, kk_limit, Mark.M512KBIT, '#97CC04'),
)

if sys.argv[1].lower() == 'now':
    start_ts = dt.now()
else: start_ts = dt.strptime(sys.argv[1], "%Y-%m-%d %H:%M")
days = int(sys.argv[2])
router = sys.argv[3]
directory = sys.argv[4]
outfile = sys.argv[5]
addsfile = os.path.join(directory, 'additional_contingent.dat')
pid = os.getpid()

print(pid, 'load data from directory %s' % repr(directory), flush = True)
storage = Storage(accounts, directory)
storage.load_data(start_ts, days)

print(pid, 'load additionals from file %s' % repr(addsfile), flush = True)
additionals = Additionals(addsfile)
additionals.apply_to_storage(storage)

print(pid, 'calculate account usage', flush = True)
reports = AccountsReport(lnames, accounts, storage)
account_usage: Dict[str, Dict[p.Account, p.Usage]] = {}
for limit_name in lnames:
    account_usage[limit_name] = reports.account_usage(start_ts, router, limit_name)

print(pid, 'calculate host usage', flush = True)
host_usage: Dict[str, Dict[p.Host, p.Usage]] = {}
for limit_name in lnames:
    host_usage[limit_name] = reports.host_usage(start_ts, router, limit_name)

print(pid, 'calculate daily report', flush = True)
account_usage_daily = reports.account_usage_periodic(start_ts, router, days, period = 'day')
print(pid, 'calculate hourly report', flush = True)
account_usage_hourly = reports.account_usage_periodic(start_ts, router, 31, period = 'hour')

print(pid, 'generate html file to %s' % repr(outfile), flush = True)
footer = '<div class="bigblock"><h1>TELIA</h1><img src="telia_state.png" alt="Telia state"></div>'
with open(outfile + ".tmp", 'w') as fd:
    html_report = HtmlReport(router, lnames, accounts, account_usage, host_usage,
                             account_usage_daily, account_usage_hourly,
                             footer, fd)
    html_report()
os.rename(outfile + ".tmp", outfile)

print(pid, 'configure firewall', flush = True)
filtering = Filtering(lnames, accounts, account_usage)
filtering.filter(directory)

print(pid, 'everything is done', flush = True)
