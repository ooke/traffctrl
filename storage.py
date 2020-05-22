import sqlite3, os, pickle, sys, protocols
from datetime import datetime, timedelta
from utils import ts2filter
from typing import Tuple, Optional, List, Callable, Any, Generator, Iterable, Union, cast

DataRow = protocols.DataRow
InputRow = Tuple[int, int, int, int, int, Optional[str]]

class Entry(protocols.Entry):
    def __init__(self) -> None: pass
    @property
    def row(self) -> DataRow: return self._row
    @row.setter
    def row(self, row: DataRow) -> None: self._row = row
    @property
    def id(self) -> int: return self._row[0]
    @property
    def host(self) -> str: return self._row[1]
    @property
    def year(self) -> int: return self._row[2]
    @property
    def month(self) -> int: return self._row[3]
    @property
    def day(self) -> int: return self._row[4]
    @property
    def hour(self) -> int: return self._row[5]
    @property
    def minute(self) -> int: return self._row[6]
    @property
    def dat_in(self) -> int: return self._row[7]
    @property
    def dat_out(self) -> int: return self._row[8]
    @property
    def dat_pkg(self) -> int: return self._row[9]
    @property
    def dat(self) -> int: return self._row[10]
    @property
    def router(self) -> str: return self._row[11]
    @property
    def ts(self) -> datetime:
        return datetime(self.year, self.month, self.day, self.hour, self.minute)

class Storage(protocols.Storage):
    def __init__(self, accounts: Tuple[protocols.Account, ...], data_path: str) -> None:
        self._data_path = data_path
        self._accounts = accounts
        for acc in self._accounts:
            acc._set_storage(self)
        self._conn = sqlite3.connect(':memory:')
        self._conn.isolation_level = None
        c = self._conn.cursor()
        c.execute('''CREATE TABLE data(id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT,
                                       year INTEGER, month INTEGER, day INTEGER, hour INTEGER, minute INTEGER,
                                       dat_in INTEGER, dat_out INTEGER, dat_pkg INTEGER,
                                       dat INTEGER, router TEXT) ''')
        self._cols = ('host', 'year', 'month', 'day', 'hour', 'minute', 'dat_in', 'dat_out', 'dat_pkg', 'dat', 'router')
        self._load_sql = '''INSERT INTO data(%s) VALUES(%s)''' \
            % (",".join(self._cols), ",".join(['?' for _ in self._cols]))

    def _read_data_file(self, fname: str) -> List[InputRow]:
        cache_file = fname + '.cache'
        if not os.path.exists(fname):
            return []
        if not os.path.exists(cache_file) or os.stat(fname).st_mtime >= os.stat(cache_file).st_mtime:
            result = []
            with open(fname) as fd:
                for line in fd:
                    line = line.replace('\0', '').strip()
                    if len(line) == 0: continue
                    data_router: Optional[str] = None
                    try:
                        data = line.split(' ')
                        hour, minute, data_in, data_out, data_pkg, data_router = \
                            int(data[0]), int(data[1]), int(data[2]), int(data[3]), int(data[4]), data[5]
                    except Exception as err1:
                        try:
                            data = line.split(' ')
                            hour, minute, data_in, data_out, data_pkg = \
                                int(data[0]), int(data[1]), int(data[2]), int(data[3]), int(data[4])
                        except Exception as err2:
                            sys.stderr.write("ERROR on parsing %s: %s -> %s\n" % (repr(fname), repr(err1), repr(err2)))
                            continue
                        data_router = None
                    if data_router ==  'mikrotik' and (data_in + data_out) < data_pkg:
                        # temporary to fix bug (2020-02-10)
                        data_in, data_pkg = data_pkg, data_in
                    result.append((hour, minute, data_in, data_out, data_pkg, data_router))
            with open(cache_file, 'wb') as fd_res:
                pickle.dump(result, fd_res)
            return result
        with open(cache_file, 'rb') as fd_res:
            return cast(List[InputRow], pickle.load(fd_res))

    def _load_one_day(self, start_ts: datetime, day_offset: int) -> None:
        td = timedelta(days = day_offset)
        ts = start_ts + td
        fname = 'day_%02d%02d%02d' % (ts.year, ts.month, ts.day)
        one_day_data = []
        for account in self._accounts:
            for host in account.hosts:
                for hostname in host.namelist:
                    today_file = os.path.join(self._data_path, hostname, fname)
                    for hour, minute, dat_in, dat_out, dat_pkg, router in self._read_data_file(today_file):
                        if datetime(ts.year, ts.month, ts.day, hour, minute) > start_ts: continue
                        one_day_data.append((hostname, ts.year, ts.month, ts.day, hour, minute,
                                             dat_in, dat_out, dat_pkg, dat_in + dat_out, router))
        c = self._conn.cursor()
        c.executemany(self._load_sql, one_day_data)

    def load_data(self, start_ts: datetime, days: int) -> None:
        for n in range(0, -days, -1):
            self._load_one_day(start_ts, n)
        c = self._conn.cursor()
        for col in ('host', 'year', 'month', 'day', 'hour', 'minute', 'router'):
            c.execute('''CREATE INDEX data_%s ON DATA(%s)''' % (col, col))

    def apply_mask(self, start_ts: datetime, cb: Callable[[DataRow], Optional[bool]],
                   flt: Optional[str] = None, direction: str = 'future',
                   args: Tuple[Any, ...] = tuple()) -> None:
        c = self._conn.cursor()
        if direction not in ('future', 'past'):
            raise RuntimeError('Direction %s uknown' % repr(direction))
        compare_sign = '>' if direction == 'future' else '<'
        sort_dir = 'ASC' if direction == 'future' else 'DESC'
        the_filter = ts2filter(start_ts, compare_sign, flt)
        the_order = 'year, month, day, hour, minute, router, host'
        the_cols = ','.join(self._cols)
        sql_text = '''SELECT id, %s FROM data WHERE %s ORDER BY %s %s''' % (the_cols, the_filter, the_order, sort_dir)
        for row in c.execute(sql_text):
            if cb(row, *args) == False:
                break

    def update_entries(self, column: str, changes: Iterable[Tuple[int, Union[str, int]]]) -> None:
        c = self._conn.cursor()
        for idval, data in changes:
            sql_text = 'UPDATE data SET %s = ? WHERE id = ?' % column
            c.execute(sql_text, (data, idval))
    
    def sum(self, start_ts: datetime, end_ts: datetime,
            flt: Optional[str] = None,
            reference_column: str = 'host') -> Generator[protocols.Usage, None, None]:
        c = self._conn.cursor()
        the_filter = '%s AND %s' % (ts2filter(start_ts, sign = '<'), ts2filter(end_ts, sign = '>'))
        if flt is not None:
            the_filter = '%s AND %s' % (the_filter, flt)
        sql_text = 'SELECT %s, SUM(dat_in), SUM(dat_out), SUM(dat_pkg), SUM(dat) FROM data WHERE %s' \
            % (reference_column, the_filter)
        c.execute(sql_text)
        for row in c.fetchall():
            yield protocols.Usage(*row)
        return None
