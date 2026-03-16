from typing import Any, Dict, List, Optional
from uuid import uuid4
from pydantic import BaseModel, Field, field_validator

class QueryRequest(BaseModel):
    query: str = Field(..., description="Natural-language question from the user.")
    include_debug: bool = Field(
        default=False,
        description="Whether to include debug metadata in the API response.",
    )
    session_id: str = Field(default_factory=lambda: str(uuid4()))

    @field_validator("session_id", mode="before")
    @classmethod
    def normalize_session_id(cls, value: object) -> str:
        if value is None:
            return str(uuid4())
        session_id = str(value).strip()
        if not session_id or session_id.lower() in {"string", "null", "none"}:
            return str(uuid4())
        return session_id


class QueryResponse(BaseModel):
    session_id: str
    success: bool
    answer: str
    attempts: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    rows: Optional[List[Dict[str, Any]]] = None
    sql: Optional[str] = None
