import json
import logging
from pathlib import Path
from typing import Any

from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings

from app.core.config import Settings

logger = logging.getLogger(__name__)


class FewShotRetriever:
    """FAISS-backed retriever for dynamic few-shot examples."""

    def __init__(self, settings: Settings, json_path: Path) -> None:
        self._settings = settings
        self._json_path = json_path
        self._vectorstore: FAISS | None = None
        self._embeddings = OpenAIEmbeddings(
            model=settings.openai_embedding_model,
            api_key=settings.openai_api_key,
        )

    def build(self) -> None:
        examples = self._read_examples()
        if not examples:
            logger.warning("Few-shot index not built: no examples from %s", self._json_path)
            return

        docs = [Document(page_content=text) for text in examples]
        self._vectorstore = FAISS.from_documents(docs, self._embeddings)
        logger.info("Few-shot index built with %d examples", len(examples))

    def retrieve(self, query: str, k: int | None = None) -> list[str]:
        if not query.strip() or self._vectorstore is None:
            return []
        top_k = k or self._settings.few_shot_top_k
        docs = self._vectorstore.similarity_search(query, k=top_k)
        return [d.page_content for d in docs]

    def _read_examples(self) -> list[str]:
        if not self._json_path.exists():
            logger.warning("Few-shot file missing: %s", self._json_path)
            return []

        try:
            payload = json.loads(self._json_path.read_text(encoding="utf-8") or "[]")
        except Exception as exc:
            logger.warning("Could not parse few-shot file %s: %s", self._json_path, exc)
            return []

        rows = payload.get("examples", []) if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            return []

        examples: list[str] = []
        for row in rows:
            text = self._to_text(row)
            if text:
                examples.append(text)
        return examples

    @staticmethod
    def _to_text(row: Any) -> str:
        if isinstance(row, str):
            return row.strip()
        if not isinstance(row, dict):
            return ""

        q = str(row.get("question") or row.get("query") or "").strip()
        r = str(row.get("reasoning") or row.get("steps") or "").strip()
        s = str(row.get("sql") or "").strip()
        a = str(row.get("answer") or "").strip()

        parts = []
        if q:
            parts.append(f"Question: {q}")
        if r:
            parts.append(f"Reasoning: {r}")
        if s:
            parts.append(f"SQL: {s}")
        if a:
            parts.append(f"Answer: {a}")

        if parts:
            return "\n".join(parts)
        return json.dumps(row, ensure_ascii=True)
