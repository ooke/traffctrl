#!/usr/bin/env python3.8

import sys, time, os
from storage import Storage
from additionals import Additionals
from datetime import datetime as dt
from data_config import addsfile, accounts

timing = time.monotonic()
def t() -> str: return "%ds" % int(time.monotonic() - timing)

if len(sys.argv) != 5:
    sys.stderr.write('Usage: %s <start_ts> <days> <directory> <dbfile>\n' % sys.argv[0])
    sys.exit(1)

if sys.argv[1].lower() == 'now':
    start_ts = dt.now()
else: start_ts = dt.strptime(sys.argv[1], "%Y-%m-%d %H:%M")
days = int(sys.argv[2])
directory = sys.argv[3]
db_file = sys.argv[4]
pid = os.getpid()

print(pid, t(), 'load data from file %s' % repr(db_file), flush = True)
storage = Storage(db_file, accounts, directory, True)

print(pid, t(), 'load data from directory %s' % repr(directory), flush = True)
storage.load_data(start_ts, days)

print(pid, t(), 'load additionals from file %s' % repr(addsfile(directory)), flush = True)
additionals = Additionals(addsfile(directory))
additionals.apply_to_storage(storage)

print(pid, t(), 'commit database', flush = True)
storage.commit()

print(pid, t(), 'finish update data process', flush = True)
