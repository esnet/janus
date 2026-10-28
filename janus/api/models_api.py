from pydantic import BaseModel, Field, RootModel
from typing import List, Optional, Union, Any, Dict


class AddEndpointRequest(BaseModel):
    type: int = Field(..., description="Endpoint type identifier (e.g. Docker, Slurm)")
    name: str = Field(..., description="Name of the endpoint node")
    url: str = Field(..., description="Base URL of the endpoint API")
    edge_type: Optional[int] = Field(
        None, description="Optional Edge Type classification"
    )
    public_url: Optional[str] = Field(
        None, description="Optional public URL if operating behind NAT"
    )


class SessionRequest(BaseModel):
    name: Optional[str] = Field(None, description="Optional session name")
    instances: List[Union[dict, str]] = Field(
        ..., description="List of instances or node names to deploy"
    )
    image: str = Field(..., description="Docker image to run")
    profile: str = Field(..., description="Host profile defining deployment parameters")
    constraints: Optional[dict] = Field(
        default_factory=dict, description="Resource and scheduling constraints"
    )
    arguments: Optional[str] = Field(
        None, description="Command line arguments for the container"
    )
    remove_container: Optional[bool] = Field(
        False, description="Automatically remove container on exit"
    )
    kwargs: Optional[dict] = Field(
        default_factory=dict, description="Additional docker kwargs"
    )
    overrides: Optional[dict] = Field(
        default_factory=dict, description="Profile overrides specific to this session"
    )
    entrypoint: Optional[str] = Field(
        None,
        description="Override the container entrypoint (e.g. '/bin/bash' for interactive setup)",
    )
    dns: Optional[List[str]] = Field(
        None,
        description="List of DNS server IPs to configure in the container",
    )


# Helper for list of requests
class SessionRequestList(RootModel):
    root: Union[SessionRequest, List[SessionRequest]]


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


class AuthBulkRequest(BaseModel):
    resource: str = Field(..., description="Resource type")
    identifiers: List[Union[str, int]] = Field(
        ..., description="List of resource names or IDs"
    )
    users: List[str] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)
    remove: Optional[bool] = Field(
        False, description="If true, remove these users/groups instead of adding them"
    )


# Query Models for Flask-OpenAPI3
class ActiveQuery(BaseModel):
    fields: Optional[str] = Field(
        None, description="Comma separated list of fields to return"
    )
    force: Optional[bool] = Field(False, description="Force deletion of session")


class LogQuery(BaseModel):
    timestamps: Optional[bool] = Field(False, description="Include timestamps in logs")
    stderr: Optional[bool] = Field(True, description="Include stderr in logs")
    stdout: Optional[bool] = Field(True, description="Include stdout in logs")
    since: Optional[int] = Field(0, description="Return logs since this timestamp")
    tail: Optional[int] = Field(
        100, description="Number of lines to return from the end of the log"
    )


class InterfaceQuery(BaseModel):
    interface: Optional[str] = Field(None, description="Network interface name")
    container: Optional[str] = Field(None, description="Container ID")


class TuneRequest(BaseModel):
    config: dict


class NodeQuery(BaseModel):
    refresh: Optional[bool] = Field(False, description="Refresh nodes from backends")
    fields: Optional[str] = Field(
        None, description="Comma separated list of fields to return"
    )


class ProfileQuery(BaseModel):
    refresh: Optional[bool] = Field(False, description="Refresh profiles from files")
    reset: Optional[bool] = Field(False, description="Reset database tables")
    fields: Optional[str] = Field(
        None, description="Comma separated list of fields to return"
    )


class ImageQuery(BaseModel):
    fields: Optional[str] = Field(
        None, description="Comma separated list of fields to return"
    )


class AuthQuery(BaseModel):
    fields: Optional[str] = Field(
        None, description="Comma separated list of fields to return"
    )


# Path Models for Flask-OpenAPI3
class ActivePath(BaseModel):
    aid: Optional[int] = Field(None, description="Active session ID")


class LogPath(BaseModel):
    aid: int = Field(..., description="Active session ID")
    nname: str = Field(..., description="Node name")


class NodePath(BaseModel):
    node: Optional[str] = Field(None, description="Node name")
    id: Optional[int] = Field(None, description="Node ID")


class ImagePath(BaseModel):
    name: Optional[str] = Field(None, description="Image name")


class ProfileResourcePath(BaseModel):
    resource: str = Field(..., description="Resource type (e.g., host, net, vol, qos)")


class ProfileFullByPath(BaseModel):
    resource: str = Field(..., description="Resource type")
    rname: str = Field(..., description="Profile name")


class AuthPath(BaseModel):
    resource: str = Field(..., description="Resource type")
    rid: Optional[int] = Field(None, description="Auth ID")
    rname: Optional[str] = Field(None, description="Auth name")


class GenericDictResponse(RootModel[Dict[str, Any]]):
    pass


class GenericListResponse(RootModel[List[Any]]):
    pass


class GenericResponse(RootModel[Any]):
    pass


class NodeResponse(BaseModel):
    id: Union[int, str] = Field(..., description="Unique identifier for the node")
    name: str = Field(..., description="Name of the node")
    model_config = {"extra": "allow"}


class NodeListResponse(RootModel[List[NodeResponse]]):
    pass


class SessionResponse(BaseModel):
    id: Optional[int] = Field(None, description="Session identifier")
    name: Optional[str] = Field(None, description="Name of the session")
    services: Optional[Dict[str, Any]] = Field(
        None, description="Services running in this session"
    )
    model_config = {"extra": "allow"}


class SessionListResponse(RootModel[List[SessionResponse]]):
    pass


class ProfileResponse(BaseModel):
    name: str = Field(..., description="Name of the profile")
    settings: Dict[str, Any] = Field(
        ..., description="Configuration settings for the profile"
    )
    is_system: Optional[bool] = Field(
        None, description="Whether this is a built-in system profile"
    )
    on_disk: Optional[bool] = Field(
        None, description="Whether this profile is saved to disk"
    )
    is_modified: Optional[bool] = Field(
        None, description="Whether this profile has unsaved modifications"
    )
    model_config = {"extra": "allow"}


class ProfileListResponse(RootModel[List[ProfileResponse]]):
    pass


class ImageResponse(BaseModel):
    name: str = Field(..., description="Name and tag of the image")
    model_config = {"extra": "allow"}


class ImageListResponse(RootModel[List[ImageResponse]]):
    pass


class AuthInfoResponse(BaseModel):
    users: Optional[List[str]] = Field(None, description="List of authorized users")
    groups: Optional[List[str]] = Field(None, description="List of authorized groups")
    model_config = {"extra": "allow"}


class SessionCreateResponse(RootModel[Dict[str, Dict[str, int]]]):
    pass


class AuthBulkResponse(BaseModel):
    resource: str = Field(..., description="Resource type updated")
    results: List[Dict[str, Any]] = Field(
        ..., description="List of individual update results"
    )


class ExecResponse(BaseModel):
    id: Optional[str] = Field(None, description="Execution ID")
    model_config = {"extra": "allow"}
