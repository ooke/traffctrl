import re, itertools, sys, protocols
from typing import *
from utils import *
from datetime import datetime as dt, timedelta as td
from storage import Entry
from itertools import repeat

class Additionals(protocols.Additionals):
    def __init__(self, adds_path: str) -> None:
        self._adds: Dict[str, List[protocols.AddsEntry]] = {}
        self._entry = Entry()
        with open(adds_path) as fd:
            for line in fd:
                line = line.strip()
                ma_boost = re.match(r'^([0-9]+)-([0-9]+)-([0-9]+)\s+([0-9]+):([0-9]+)\s+boost\s+([a-z0-9]+)\s+([0-9]+)\s*h$', line)
                ma_cont = re.match(r'^([0-9]+)-([0-9]+)-([0-9]+)\s+([0-9]+):([0-9]+)\s+data\s+([a-z0-9]+)\s+([a-z0-9]+)\s+([0-9.]+)\s*([kmgtKMGT])i[bB]$', line)
                if ma_boost: ma_gen = ma_boost
                elif ma_cont: ma_gen = ma_cont
                else: continue
                year, month, day = int(ma_gen.group(1)), int(ma_gen.group(2)), int(ma_gen.group(3))
                hour, minute, router = int(ma_gen.group(4)), int(ma_gen.group(5)), ma_gen.group(6)
                if ma_boost:
                    numhours = int(ma_boost.group(7))
                    if router not in self._adds: self._adds[router] = []
                    self._adds[router].append((dt(year, month, day, hour, minute), 'boost', router, None, td(hours = numhours)))
                    continue
                if ma_cont:
                    host = ma_cont.group(7)
                    amount = units2bytes(float(ma_cont.group(8)), Units[ma_cont.group(9).upper()])
                    if router not in self._adds: self._adds[router] = []
                    self._adds[router].append((dt(year, month, day, hour, minute), 'data', router, host, int(amount)))
                    continue

    @property
    def routers(self) -> Tuple[str, ...]:
        return tuple(self._adds.keys())
    
    def __len__(self) -> int:
        return len(self._adds)
    
    def __getitem__(self, router: str) -> Generator[protocols.AddsEntry, None, None]:
        for adds in self._adds[router]:
            yield adds

    def _apply_boost_entries(self, row: protocols.DataRow, ets: dt, entries: List[int],
                             collect_rows: Optional[List[protocols.DataRow]] = None) -> bool:
        e = self._entry; e.row = row
        if e.ts > ets: return False
        if collect_rows is not None:
            collect_rows.append(row)
        entries.append(e.id)
        return True

    def _apply_boost(self, store: protocols.Storage, ts: dt,
                     router: str,
                     duration: td,
                     entries: List[int],
                     collect_rows: Optional[List[protocols.DataRow]] = None) -> None:
        bts, ets = ts, (ts + duration)
        store.apply_mask(bts, lambda row: self._apply_boost_entries(row, ets, entries, collect_rows),
                         flt = "router = '%s'" % router)
    
    def _apply_data_entries(self, row: protocols.DataRow, ts: dt,
                            data: Dict[str, int],
                            entries: List[Tuple[int,int]],
                            collect_rows: Optional[List[protocols.DataRow]] = None) -> bool:
        if data['add'] == 0: return False
        e = self._entry; e.row = row
        amount = min(data['add'], e.dat)
        data['add'] -= amount
        if collect_rows is not None:
            collect_rows.append(row)
        entries.append((e.id, e.dat - amount))
        data['amount'] += amount
        return True

    def _apply_data(self, store: protocols.Storage, ts: dt,
                    router: str, host: str, amount: int,
                    entries: List[Tuple[int, int]],
                    collect_rows: Optional[List[protocols.DataRow]] = None) -> int:
        data = {'add': amount, 'amount': 0}
        store.apply_mask(ts, lambda row: self._apply_data_entries(row, ts, data, entries, collect_rows),
                         flt = "host = '%s' AND router = '%s'" % (host, router))
        return data['add']

    def apply_to_storage(self, store: protocols.Storage,
    collect_rows: Optional[Dict[protocols.AddsEntry, List[protocols.DataRow]]] = None) -> Dict[str, int]:
        boost_entries: List[int] = []
        for router, add_data in self._adds.items():
            for adds in add_data:
                add_ts, add_type, _, _, add_duration = adds
                if add_type == 'boost':
                    if not isinstance(add_duration, td):
                        raise RuntimeError('No timedelta defined')
                    crows = None
                    if collect_rows is not None:
                        crows = collect_rows.setdefault(adds, [])
                    self._apply_boost(store, add_ts, router, add_duration, boost_entries, crows)
        store.update_entries('dat', zip(boost_entries, repeat(0)))
        del boost_entries
        data_entries: List[Tuple[int, int]] = []
        rest_adds: Dict[str, int] = {}
        for router, add_data in self._adds.items():
            for adds in add_data:
                add_ts, add_type, _, add_host, add_amount = adds
                if add_type == 'data':
                    if add_host is None: raise RuntimeError('No host defined')
                    if not isinstance(add_amount, int):
                        raise RuntimeError('No amount defined')
                    crows = None
                    if collect_rows is not None:
                        crows = collect_rows.setdefault(adds, [])
                    res = self._apply_data(store, add_ts, router, add_host, add_amount, data_entries, crows)
                    if add_host not in rest_adds:
                        rest_adds[add_host] = res
                    else: rest_adds[add_host] += res
                    store.update_entries('dat', data_entries)
        del data_entries
        return rest_adds
