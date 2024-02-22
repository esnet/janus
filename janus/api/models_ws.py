from pydantic import BaseModel
from typing import List, Optional, Union


class WSExecStream(BaseModel):
    node: str
    node_id: Union[str,int]
    container: str
    exec_id: Optional[Union[str,int]]
