import re
from datetime import datetime
from typing import *
from enum import Enum

class Units(Enum):
    B: int = 1
    KiB: int = 1024
    MiB: int = 1048576
    GiB: int = 1073741824
    TiB: int = 1099511627776
    PiB: int = 1125899906842624
    EiB: int = 1152921504606846976
    ZiB: int = 1180591620717411303424
    YiB: int = 1208925819614629174706176
    K: int = 1024
    M: int = 1048576
    G: int = 1073741824
    T: int = 1099511627776
    P: int = 1125899906842624
    E: int = 1152921504606846976
    Z: int = 1180591620717411303424
    Y: int = 1208925819614629174706176
KiB = K = Units.KiB.value
MiB = M = Units.MiB.value
GiB = G = Units.GiB.value
TiB = T = Units.TiB.value
PiB = P = Units.PiB.value
EiB = E = Units.EiB.value
ZiB = Z = Units.ZiB.value
YiB = Y = Units.YiB.value
    
def bytes2units(num: Union[int,float], delimiter: str = ' ') -> str:
    num = float(num)
    for x in ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'):
        if num < 1024.0:
            return "%s%s%s" % (("%3.1f" % num).rstrip('0').rstrip('.'), delimiter, x)
        num /= 1024.0
    return "%s%sYiB" % (("%-3.1f" % num).rstrip('0').rstrip('.'), delimiter)

AMOUNT_RE = re.compile(r'\s*^([+-]?[0-9.]+)\s*([kKmMgGtTpPeEzZyY]i[bB]|[bB])?\s*$')
def units2bytes(amount: Union[str, int, float], units: Optional[Units] = None) -> int:
    if isinstance(amount, str) and units is None:
        ma_data = AMOUNT_RE.match(amount)
        if not ma_data: raise RuntimeError('Data %s can not be converted to bytes.' % repr(amount))
        the_amount = float(ma_data.group(1))
        if ma_data.group(2) == '': the_units = 'B'
        else: the_units = ma_data.group(2)
        return int(the_amount * Units[the_units].value)
    elif isinstance(amount, (int, float)) and units is not None:
        return int(amount * units.value)
    raise RuntimeError('Invalid arguments combination.')

def ts2filter(ts: datetime, sign: str = '>', flt: Optional[str] = None, equal: bool = True) -> str:
    if sign not in ('<', '>'):
        raise RuntimeError('Sign can only be one of < or >, not %s' % repr(sign))
    the_equal = '=' if equal else ''
    the_filter = '''(year {s} {y} OR (year = {y} AND (month {s} {m} OR (month = {m} AND (day {s} {d} OR (day = {d} AND (hour {s} {H} OR (hour = {H} AND minute {s}{e} {M}))))))))'''\
        .format(y = ts.year, m = ts.month, d = ts.day, H = ts.hour, M = ts.minute, s = sign, e = the_equal)
    if flt is not None:
        the_filter = '%s AND (%s)' % (the_filter, flt)
    return the_filter
    
