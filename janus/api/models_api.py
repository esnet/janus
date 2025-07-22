from pydantic import BaseModel
from typing import List, Optional


class AddEndpointRequest(BaseModel):
    type: int
    name: str
    url: str
    edge_type: Optional[int] = None
    public_url: Optional[str] = None


class SessionRequest(BaseModel):
    instances: List[dict | str]
    image: str
    profile: str
    constraints: Optional[dict] = dict()
    arguments: Optional[str] = None
    remove_container: Optional[bool] = False
    kwargs: Optional[dict] = dict()
    overrides: Optional[dict] = dict()


class ProfileRequest(BaseModel):
    name: Optional[str] = None
    settings: dict


class ExecRequest(BaseModel):
    Cmd: List[str]
    node: str
    container: str
    start: Optional[bool] = False
    attach: Optional[bool] = True
    tty: Optional[bool] = False


class AuthRequest(BaseModel):
    users: List[str]
    groups: List[str]
