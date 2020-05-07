from enum import Enum, unique

@unique
class Mark(Enum):
    M64KBIT = 5
    M128KBIT = 4
    M256KBIT = 3
    M512KBIT = 2
    M1MBIT = 1
    M3MBIT = 6
    M5MBIT = 7
    M1GBIT = 12
