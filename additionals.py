import re, protocols
from typing import Tuple, Dict, List, Optional, Generator, cast
from utils import units2bytes, Units, ts2filter
from datetime import datetime as dt, timedelta as td
from storage import Entry

class Additionals(protocols.Additionals):
    def __init__(self, adds_path: str) -> None:
        self._adds: List[protocols.AddsEntry] = []
        self._end_ts: Dict[protocols.AddsEntry, dt] = {}
        self._entry = Entry()
        self._min_ts = dt.now()
        routerset = set()
        with open(adds_path) as fd:
            for line in fd:
                line = line.strip()
                ma_boost = re.match(r'^([0-9]+)-([0-9]+)-([0-9]+)\s+([0-9]+):([0-9]+)\s+boost\s+([a-z0-9]+)\s+([0-9]+)\s*h(.*)$', line)
                ma_cont = re.match(r'^([0-9]+)-([0-9]+)-([0-9]+)\s+([0-9]+):([0-9]+)\s+data\s+([a-z0-9]+)\s+([a-z0-9]+)\s+([0-9.]+)\s*([kmgtpezyKMGTPEZY])i[bB](.*)$', line)
                if ma_boost: ma_gen = ma_boost
                elif ma_cont: ma_gen = ma_cont
                else: continue
                year, month, day = int(ma_gen.group(1)), int(ma_gen.group(2)), int(ma_gen.group(3))
                hour, minute, router = int(ma_gen.group(4)), int(ma_gen.group(5)), ma_gen.group(6)
                routerset.add(router)
                ts = dt(year, month, day, hour, minute)
                if self._min_ts > ts:
                    self._min_ts = ts
                comment: Optional[str]
                if ma_boost:
                    numhours = int(ma_boost.group(7))
                    comment_str = ma_gen.group(8).strip()
                    comment = (comment_str if len(comment_str) > 0 else None)
                    self._adds.append(protocols.AddsEntry(ts, 'boost', router, None, td(hours = numhours), comment))
                elif ma_cont:
                    host = ma_cont.group(7)
                    amount = units2bytes(float(ma_cont.group(8)), Units[ma_cont.group(9).upper()])
                    comment_str = ma_gen.group(10).strip()
                    comment = (comment_str if len(comment_str) > 0 else None)
                    self._adds.append(protocols.AddsEntry(ts, 'data', router, host, int(amount), comment))
        self._routers: Tuple[str, ...] = tuple(routerset)

    @property
    def routers(self) -> Tuple[str, ...]:
        return self._routers
    
    def __len__(self) -> int:
        return len(self._adds)
    
    def __getitem__(self, router: str) -> Generator[protocols.AddsEntry, None, None]:
        for adds in self._adds:
            if router == adds.router:
                yield adds

    def _apply_adds(self, row: protocols.DataRow) -> Optional[bool]:
        e = self._entry; e.row = row
        curr_dat = e.dat_in + e.dat_out
        for add in self._adds:
            if add.atype == 'boost':
                if self._collect_rows is not None and add not in self._collect_rows:
                    self._collect_rows[add] = []
                if curr_dat > 0 and add.router == e.router \
                   and e.ts >= add.ts and e.ts <= self._end_ts[add]:
                    curr_dat = 0
                    if self._collect_rows is not None:
                        self._collect_rows[add].append(row)
        for add in self._adds:
            if add.atype == 'data':
                if self._collect_rows is not None and add not in self._collect_rows:
                    self._collect_rows[add] = []
                if curr_dat > 0 and add.router == e.router \
                   and add.host == e.host and self._unused_data[add] > 0 and e.ts >= add.ts:
                    amount = min(self._unused_data[add], curr_dat)
                    self._unused_data[add] -= amount
                    curr_dat -= amount
                    if self._collect_rows is not None:
                        self._collect_rows[add].append(row)
        if curr_dat != e.dat:
            self._entryids.append((e.id, curr_dat))
        return True

    def apply_to_storage(self, store: protocols.Storage,
                         collect_rows: Optional[Dict[protocols.AddsEntry, List[protocols.DataRow]]] = None) -> None:
        store.reset_dat()
        self._entryids: List[Tuple[int, int]] = []
        self._collect_rows = collect_rows
        self._unused_data: Dict[protocols.AddsEntry, int] = {}
        flt = set()
        for add in self._adds:
            self._current_add = add
            if add.atype == 'boost':
                self._end_ts[add] = add.ts + cast(td, add.amount)
                flt.add("(router = '%s' AND %s AND %s)" % (add.router, ts2filter(add.ts, '>'), ts2filter(self._end_ts[add], '<')))
            elif add.atype == 'data':
                self._unused_data[add] = cast(int, add.amount)
                flt.add("(router = '%s' AND host = '%s' AND %s)" % (add.router, add.host, ts2filter(add.ts, '>')))
        store.apply_mask(self._min_ts, self._apply_adds, flt = " OR ".join(flt))
        store.update_entries('dat', self._entryids)
        rest_adds: Dict[str, int] = {}
        for add, amount in self._unused_data.items():
            if add.atype != 'data': continue
            if add.host is None: continue
            rest_adds[add.host] = rest_adds.get(add.host, 0) + amount
        store.rest_adds = rest_adds
