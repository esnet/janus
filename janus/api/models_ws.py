from pydantic import BaseModel
from typing import List, Optional, Union


class WSExecStream(BaseModel):
    node: str
    container: str
