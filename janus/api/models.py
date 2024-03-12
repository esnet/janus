from pydantic import BaseModel, root_validator, create_model
from typing import List, Optional, Union


class QoSProfileSettings(BaseModel):
    delay: Optional[str] = None
    loss: Optional[str] = None
    rate: Optional[str] = None
    corrupt: Optional[str] = None
    reordering: Optional[str] = None
    limit: Optional[str] = None
    dport: Optional[str] = None
    ip: Optional[str] = None

class QoS_Controller(BaseModel):
    name: str
    settings: QoSProfileSettings

class QoS_Agent(BaseModel):
    interface: Optional[str] = None
    container: Optional[str] = None
    delay: Optional[str] = None
    loss: Optional[str] = None
    rate: Optional[str] = None
    corrupt: Optional[str] = None
    reordering: Optional[str] = None
    limit: Optional[str] = None
    dport: Optional[str] = None
    ip: Optional[str] = None

    @root_validator(pre=True)
    def validate_qos_additional(cls, values):
        if ("interface" not in values) and ("container" not in values):
            raise ValueError("Either interface name or container id must be given!")

        return values

    @root_validator(pre=True)
    def validate_qos(cls, values):
        if ("interface" in values) and ("container" in values):
            raise ValueError("Only one of interface and container can be set")

        return values


class ContainerProfileSettings(BaseModel):
    privileged: bool = False
    systemd: bool = False
    pull_image: bool = False
    cpu: int = 0
    memory: int = 0
    affinity: str = "network"
    cpu_set: Optional[str] = None
    mgmt_net: Optional[Union[dict, str]] = None
    data_net: Optional[Union[dict, str]] = None
    internal_port: Optional[str] = None
    ctrl_port_range: Optional[List[int]] = None
    data_port_range: Optional[List[int]] = None
    serv_port_range: Optional[List[int]] = None
    features: Optional[List[str]] = None
    volumes: Optional[List[str]] = None
    environment: Optional[List[str]] = None
    qos: Optional[str] = None
    tools: Optional[dict] = dict()
    arguments: Optional[str] = None
    post_starts: Optional[List[str]] = None


class ContainerProfile(BaseModel):
    name: str
    settings: ContainerProfileSettings


class NetworkProfileSettings(BaseModel):
    driver: str
    mode: Optional[str] = None
    enable_ipv6: bool = False
    ipam: Optional[dict] = None
    options: Optional[dict] = None


class NetworkProfile(BaseModel):
    name: str
    settings: NetworkProfileSettings


class VolumeProfileSettings(BaseModel):
    type: str
    driver: Optional[str] = None
    source: Optional[str] = None
    target: Optional[str] = None

    @root_validator(pre=True)
    def validate_type(cls, values):
        if (values.get('type') == "bind") and (not values.get('source') or not values.get('target')):
            raise ValueError("Source and Target are required for bind mounts")
        return values


class VolumeProfile(BaseModel):
    name: str
    settings: VolumeProfileSettings


class SessionConstraints(BaseModel):
    cpu: Optional[float] = None
    memory: Optional[int] = None
    nodeName: Optional[str] = None
    nodeCount: Optional[int] = None
    time: Optional[int] = None


class SessionRequest(BaseModel):
    node: dict
    image: str
    profile: ContainerProfile
    constraints: SessionConstraints
    arguments: Optional[str]
    remove_container: Optional[bool]
    kwargs: Optional[dict]


class Node(BaseModel):
    id: Union[int, str]
    name: str


class Network(object):
    def __init__(self, net, node=None):
        self.name = None
        self.ipv4 = None
        self.ipv6 = None
        self.node = node
        if isinstance(net, list):
            raise Exception("List of networks not supported")
        if isinstance(net, dict):
            self.name = net.get("name", None)
            self.ipv4 = net.get("ipv4_addr", None)
            self.ipv6 = net.get("ipv6_addr", None)
        elif isinstance(net, str):
            self.name = net
    @property
    def key(self):
        return f"{self.node}-{self.name}" if self.node else self.name

    def is_host(self):
        if self.name and self.name == "host":
            return True
        return False


class ServiceRecord(object):
    def __init__(self, srec_dict):
        pass
