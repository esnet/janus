from enum import IntEnum


class Constants:
    NET = "network"
    HOST = "host"
    QOS = "qos"
    VOL = "volume"
    NET_BRIDGE = "bridge"
    NET_NONE = "none"
    NET_HOST = "host"

class State(IntEnum):
    UNKNOWN = 0
    INITIALIZED = 1
    STARTED = 2
    STOPPED = 3
    MIXED = 4
    STALE = 5


class EPType(IntEnum):
    UNKNOWN = 0,
    PORTAINER = 1
    KUBERNETES = 2
    DOCKER = 3
