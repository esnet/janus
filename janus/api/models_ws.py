from pydantic import BaseModel, Field
from typing import List, Optional, Union
from janus.api.constants import EPType, WSType, WS_MIN, WS_MAX


class WSExecStream(BaseModel):
    type: int = Field(None, eq=WSType.EXEC_STREAM)
    node: str
    node_id: Union[str,int]
    container: str
    exec_id: Optional[Union[str,int]]

class EdgeAgentRegister(BaseModel):
    type: int = Field(None, eq=WSType.AGENT_REGISTER)
    jwt: str
    name: str
    edge_type: int = Field(None, ge=WS_MIN, le=WS_MAX)
    public_url: str
