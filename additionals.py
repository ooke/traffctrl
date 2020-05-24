import re, protocols
from typing import Tuple, Dict, List, Optional, Generator, cast
from utils import units2bytes, Units
from datetime import datetime as dt, timedelta as td
from storage import Entry

class Additionals(protocols.Additionals):
    def __init__(self, adds_path: str) -> None:
        self._adds: Dict[str, List[protocols.AddsEntry]] = {}
        self._end_ts: Dict[protocols.AddsEntry, dt] = {}
        self._entry = Entry()
        self._min_ts = dt.now()
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
                ts = dt(year, month, day, hour, minute)
                if self._min_ts > ts:
                    self._min_ts = ts
                comment: Optional[str]
                if ma_boost:
                    numhours = int(ma_boost.group(7))
                    if router not in self._adds: self._adds[router] = []
                    comment_str = ma_gen.group(8).strip()
                    comment = (comment_str if len(comment_str) > 0 else None)
                    self._adds[router].append(protocols.AddsEntry(ts, 'boost', router, None, td(hours = numhours), comment))
                    continue
                if ma_cont:
                    host = ma_cont.group(7)
                    amount = units2bytes(float(ma_cont.group(8)), Units[ma_cont.group(9).upper()])
                    if router not in self._adds: self._adds[router] = []
                    comment_str = ma_gen.group(10).strip()
                    comment = (comment_str if len(comment_str) > 0 else None)
                    self._adds[router].append(protocols.AddsEntry(ts, 'data', router, host, int(amount), comment))
                    continue

    @property
    def routers(self) -> Tuple[str, ...]:
        return tuple(self._adds.keys())
    
    def __len__(self) -> int:
        return len(self._adds)
    
    def __getitem__(self, router: str) -> Generator[protocols.AddsEntry, None, None]:
        for adds in self._adds[router]:
            yield adds

    def _apply_adds(self, row: protocols.DataRow) -> bool:
        e = self._entry; e.row = row
        for router, add_data in self._adds.items():
            if router != e.router: continue
            for add in add_data:
                if add.atype == 'boost':
                    if e.ts < add.ts or e.ts > self._end_ts[add] or e.dat == 0:
                        continue
                    self._entryids.append((e.id, 0))
                    if self._collect_rows is not None:
                        self._collect_rows.setdefault(add, []).append(row)
            for add in add_data:
                if add.atype == 'data':
                    if e.ts < add.ts or self._unused_data[add] == 0 or e.dat == 0:
                        continue
                    amount = min(self._unused_data[add], e.dat)
                    self._unused_data[add] -= amount
                    self._entryids.append((e.id, e.dat - amount))
                    if self._collect_rows is not None:
                        self._collect_rows.setdefault(add, []).append(row)
        return True

    def apply_to_storage(self, store: protocols.Storage,
                         collect_rows: Optional[Dict[protocols.AddsEntry, List[protocols.DataRow]]] = None) -> None:
        store.reset_dat()
        self._entryids: List[Tuple[int, int]] = []
        self._collect_rows = collect_rows
        self._unused_data: Dict[protocols.AddsEntry, int] = {}
        for router, add_data in self._adds.items():
            for add in add_data:
                self._current_add = add
                if add.atype == 'boost':
                    self._end_ts[add] = add.ts + cast(td, add.amount)
                elif add.atype == 'data':
                    self._unused_data[add] = cast(int, add.amount)
                else: raise RuntimeError('Unknown add type.')
        store.apply_mask(self._min_ts, self._apply_adds)
        store.update_entries('dat', self._entryids)
