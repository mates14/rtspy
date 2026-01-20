from enum import Enum, auto, IntEnum

class CentralState:
    PERIOD_MASK = 0x00F
    DAY = 0x000
    EVENING = 0x001
    DUSK = 0x002
    NIGHT = 0x003
    DAWN = 0x004
    MORNING = 0x005
    UNKNOWN = 13

    ONOFF_MASK = 0x030
    ON = 0x000
    STANDBY = 0x010
    SOFT_OFF = 0x020
# set when it is a real off state blocking all domes
    HARD_OFF = 0x030


class ConnectionState(IntEnum):
    """States for network connections (matching RTS2 C++ enum)."""
    UNKNOWN = 0
    RESOLVING_DEVICE = 1
    CONNECTING = 2
    INPROGRESS = 3
    CONNECTED = 4
    BROKEN = 5
    DELETE = 6
    AUTH_PENDING = 7
    AUTH_OK = 8
    AUTH_FAILED = 9

class DeviceType:
    UNKNOWN = 0
    SERVERD = 1
    MOUNT = 2
    CCD = 3
    DOME = 4
    WEATHER = 5
    ROTATOR = 6
    PHOT = 7
    PLAN = 8
    GRB = 9
    FOCUS = 10
    MIRROR = 11
    CUPOLA = 12
    FW = 13
    AUGERSH = 14
    SENSOR = 15
    CAT = 16
    AXIS = 18
    EXECUTOR = 20
    IMGPROC = 21
    SELECTOR = 22
    HTTPD = 23
    INDI = 24
    LOGD = 25
    SCRIPTOR = 26
    BB = 27
    REDIS = 28
    THRIFT = 29

DevTypes = {
    0: "unknown",
    1: "serverd",
    2: "mount",
    3: "ccd",
    4: "dome",
    5: "weather",
    6: "rotator",
    7: "phot",
    8: "plan",
    9: "grb",
    10: "focus",
    11: "mirror",
    12: "cupola",
    13: "fw",
    14: "augersh",
    15: "sensor",
    16: "cat",
    18: "axis",
    20: "executor",
    21: "imgproc",
    22: "selector",
    23: "httpd",
    24: "indi",
    25: "logd",
    26: "scriptor",
    27: "bb",
    28: "redis",
    29: "thrift"
}


