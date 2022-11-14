from pydantic import BaseModel, validator, root_validator
from typing import List, Optional, Union
from enum import Enum


class QoS_Controller(BaseModel):
    delay: Optional[str] = None
    loss: Optional[str] = None
    rate: Optional[str] = None
    corrupt: Optional[str] = None
    reordering: Optional[str] = None
    limit: Optional[str] = None
    dport: Optional[str] = None
    ip: Optional[str] = None


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


class Profile(BaseModel):
    privileged: bool = False
    systemd: bool = False
    pull_image: bool = False
    cpu: int = 0
    memory: int = 0
    affinity: str = "network"
    cpu_set: Optional[str] = None
    mgmt_net: Union[dict, str] = None
    data_net: Optional[Union[dict, str]] = None
    internal_port: Optional[str] = None
    ctrl_port_range: Optional[List[int]] = None
    data_port_range: Optional[List[int]] = None
    serv_port_range: Optional[List[int]] = None
    features: Optional[List[str]] = None
    volumes: Optional[List[str]] = None
    environment: Optional[List[str]] = None
    tools: Optional[dict] = dict()
