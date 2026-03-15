from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(..., description="Natural-language question from the user.")
    include_debug: bool = Field(
        default=False,
        description="Whether to include debug metadata in the API response.",
    )


class QueryResponse(BaseModel):
    success: bool
    answer: str
    attempts: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    rows: Optional[List[Dict[str, Any]]] = None
    sql: Optional[str] = None
