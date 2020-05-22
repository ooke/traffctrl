import protocols
from typing import Union, Tuple, cast

class Host(protocols.Host):
    def __init__(self, name: str, ips: Union[Tuple[str, ...], None] = None):
        self._name = name
        if ips is not None:
            self._ips = ips
        else: self._ips = cast(Tuple[str], tuple())

    @property
    def name(self) -> str: return self._name
    @property
    def ips(self) -> Tuple[str, ...]: return self._ips
    @property
    def namelist(self) -> Tuple[str, ...]:
        return (self._name,) + self._ips
