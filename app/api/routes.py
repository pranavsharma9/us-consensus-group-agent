import logging

from fastapi import APIRouter, HTTPException, Request

from app.graph.workflow import QueryWorkflow
from app.schemas.query import QueryRequest, QueryResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def query_endpoint(request: Request, payload: QueryRequest) -> QueryResponse:
    workflow: QueryWorkflow | None = getattr(request.app.state, "workflow", None)
    if workflow is None:
        raise HTTPException(status_code=500, detail="Workflow is not initialized.")

    try:
        result = workflow.invoke(
            session_id=payload.session_id,
            user_query=payload.query,
            include_debug=payload.include_debug,
        )
    except Exception as exc:
        logger.exception("Workflow invocation failed: %s", exc)
        raise HTTPException(status_code=500, detail="Workflow execution failed.") from exc

    success = result.get("status") == "success"

    sql_calls: list = result.get("sql", [])
    sql_display = "\n---\n".join(sql_calls) if isinstance(sql_calls, list) else sql_calls or ""

    metadata: dict = {
        "status": result.get("status"),
        "error_message": result.get("error_message"),
        "agent_turns": result.get("attempt", 1),
    }
    if payload.include_debug:
        metadata["sql_steps"] = sql_calls
        metadata["tool_results"] = result.get("rows", [])

    return QueryResponse(
        session_id=payload.session_id,
        success=success,
        answer=result.get("final_answer", ""),
        attempts=result.get("attempt", 1),
        metadata=metadata,
        rows=None,
        sql=sql_display if payload.include_debug else None,
    )
