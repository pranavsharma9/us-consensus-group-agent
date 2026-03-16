from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler

import logging

from app.api.routes import router as query_router
from app.core.config import get_settings, limiter
from app.graph.workflow import QueryWorkflow
from app.services.agent_context import AgentContext
from app.services.few_shot_retriever import FewShotRetriever
settings = get_settings()
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    json_path = Path(__file__).resolve().parent / "prompts" / "few_shots.json"
    context_path = Path(__file__).resolve().parent.parent / "context.json"
    retriever = FewShotRetriever(settings=settings, json_path=json_path)
    retriever.build()
    agent_context = AgentContext(settings=settings, storage_path=context_path)
    app.state.workflow = QueryWorkflow(
        settings=settings,
        few_shot_retriever=retriever,
        agent_context=agent_context,
    )
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Snowflake NL Q&A Backend", version="0.1.0", lifespan=lifespan)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)
    app.include_router(query_router)
    return app


app = create_app()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="localhost", port=8000)