import sqlite3, os, sys, protocols, hashlib
from datetime import datetime, timedelta
from utils import ts2filter
from typing import Tuple, Optional, List, Callable, Any, Generator, Iterable, Union, Dict

DataRow = protocols.DataRow
InputRow = Tuple[int, int, int, int, int, Optional[str], str]

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
    def __init__(self, fname: str,
                 accounts: Tuple[protocols.Account, ...],
                 data_path: str,
                 create_db: bool) -> None:
        self._data_path = data_path
        self._accounts = accounts
        for acc in self._accounts:
            acc._set_storage(self)
        self._conn = sqlite3.connect(fname)
        c = self._conn.cursor()
        if create_db:
            try: c.execute('''CREATE TABLE data(id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT,
                                                year INTEGER, month INTEGER, day INTEGER, hour INTEGER, minute INTEGER,
                                                dat_in INTEGER, dat_out INTEGER, dat_pkg INTEGER,
                                                dat INTEGER, router TEXT, dat_id TEXT) ''')
            except sqlite3.OperationalError as err:
                if str(err) != 'table data already exists': raise err
            try: c.execute('''CREATE UNIQUE INDEX data_dat_id ON data(dat_id)''')
            except sqlite3.OperationalError as err:
                if str(err) != 'index data_dat_id already exists': raise err
            for col in ('host', 'year', 'month', 'day', 'hour', 'minute', 'router'):
                try: c.execute('''CREATE INDEX data_%s ON DATA(%s)''' % (col, col))
                except sqlite3.OperationalError as err:
                    if str(err) != 'index data_%s already exists' % col:
                        raise err
            try: c.execute('''CREATE TABLE files(name TEXT, mtime TEXT)''')
            except sqlite3.OperationalError as err:
                if str(err) != 'table files already exists': raise err
            try: c.execute('''CREATE UNIQUE INDEX files_name ON files(name)''')
            except sqlite3.OperationalError as err:
                if str(err) != 'index files_name already exists': raise err
            try: c.execute('''CREATE TABLE rest_adds(name TEXT, amount INTEGER)''')
            except sqlite3.OperationalError as err:
                if str(err) != 'table rest_adds already exists': raise err
            self._conn.commit()
        self._cols = ('host', 'year', 'month', 'day', 'hour', 'minute', 'dat_in', 'dat_out', 'dat_pkg', 'dat', 'router', 'dat_id')
        self._load_sql = '''INSERT INTO data(%s) VALUES(%s)''' \
            % (",".join(self._cols), ",".join(['?' for _ in self._cols]))
        self._known_ids = set(x[0] for x in c.execute('''SELECT dat_id FROM data''').fetchall())
        self._known_files = set((x[0], x[1]) for x in c.execute('''SELECT name, mtime FROM files''').fetchall())

    def _read_data_file(self, fname: str) -> List[InputRow]:
        result: List[InputRow] = []
        if not os.path.exists(fname): return result
        mtime = str(os.stat(fname).st_mtime)
        if (fname, mtime) in self._known_files: return result
        id_int = int.from_bytes(hashlib.shake_256(fname.encode('utf-8')).digest(11), 'big')
        with open(fname) as fd:
            for line in fd:
                id_int += 1
                line = line.replace('\0', '').strip()
                if len(line) == 0: continue
                data_router: Optional[str] = None
                try:
                    data = line.split(' ')
                    if len(data) == 7:
                        hour, minute, data_in, data_out, data_pkg, data_router, dat_id = \
                            int(data[0]), int(data[1]), int(data[2]), int(data[3]), int(data[4]), data[5], data[6]
                    elif len(data) == 6:
                        hour, minute, data_in, data_out, data_pkg, data_router = \
                            int(data[0]), int(data[1]), int(data[2]), int(data[3]), int(data[4]), data[5]
                        dat_id = id_int.to_bytes(11, 'big').hex()
                    elif len(data) == 5:
                        hour, minute, data_in, data_out, data_pkg = \
                            int(data[0]), int(data[1]), int(data[2]), int(data[3]), int(data[4])
                        dat_id = id_int.to_bytes(11, 'big').hex()
                    else: raise RuntimeError("wrong line format: %s" % repr(data))
                except Exception as err:
                    sys.stderr.write("ERROR on parsing %s: %s\n" % (repr(fname), repr(err)))
                    continue
                if data_router ==  'mikrotik' and (data_in + data_out) < data_pkg:
                    # temporary to fix bug (2020-02-10)
                    data_in, data_pkg = data_pkg, data_in
                if dat_id not in self._known_ids:
                    result.append((hour, minute, data_in, data_out, data_pkg, data_router, dat_id))
        c = self._conn.cursor()
        try: c.execute('INSERT INTO files(name, mtime) VALUES(?,?)', (fname, mtime))
        except sqlite3.IntegrityError as err:
            if str(err) != 'UNIQUE constraint failed: files.name': raise err
            c.execute('UPDATE files SET mtime = ? WHERE name = ?', (mtime, fname))
        return result

    def _load_one_day(self, start_ts: datetime, day_offset: int) -> None:
        td = timedelta(days = day_offset)
        ts = start_ts + td
        fname = 'day_%02d%02d%02d' % (ts.year, ts.month, ts.day)
        one_day_data = []
        for account in self._accounts:
            for host in account.hosts:
                for hostname in host.namelist:
                    today_file = os.path.join(self._data_path, hostname, fname)
                    for hour, minute, dat_in, dat_out, dat_pkg, router, dat_id in self._read_data_file(today_file):
                        if datetime(ts.year, ts.month, ts.day, hour, minute) > start_ts: continue
                        one_day_data.append((hostname, ts.year, ts.month, ts.day, hour, minute,
                                             dat_in, dat_out, dat_pkg, dat_in + dat_out, router, dat_id))
        c = self._conn.cursor()
        c.executemany(self._load_sql, one_day_data)

    def load_data(self, start_ts: datetime, days: int) -> None:
        for n in range(0, -days, -1):
            self._load_one_day(start_ts, n)

    def reset_dat(self) -> None:
        c = self._conn.cursor()
        c.execute('UPDATE data SET dat = dat_in + dat_out')

    def commit(self) -> None:
        self._conn.commit()

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

    @property
    def rest_adds(self) -> Dict[str, int]:
        result: Dict[str, int] = {}
        c = self._conn.cursor()
        for name, amount in c.execute('''SELECT * FROM rest_adds''').fetchall():
            result[name] = amount
        return result

    @rest_adds.setter
    def rest_adds(self, rest_adds: Dict[str, int]) -> None:
        c = self._conn.cursor()
        c.execute('''DELETE FROM rest_adds''')
        data = [(k,v) for k,v in rest_adds.items()]
        c.executemany('''INSERT INTO rest_adds(name, amount) VALUES (?,?)''', data)
    
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
