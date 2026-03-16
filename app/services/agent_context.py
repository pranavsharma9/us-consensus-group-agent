import json
from pathlib import Path
from typing import Literal

from app.core.config import Settings

class AgentContext:
    def __init__(self, settings: Settings, storage_path: Path | None = None) -> None:
        self._settings = settings
        self.max_context_window = settings.max_context_window
        default_storage_path = Path(__file__).resolve().parents[2] / "context.json"
        self._storage_path = storage_path or default_storage_path
        self.context_window: dict[str, list[dict[str, str]]] = {}
        self._load_from_file()

    def add_context(
        self, session_id: str, role: Literal["user", "assistant", "system"], content: str
    ) -> None:
        if session_id not in self.context_window:
            self.context_window[session_id] = []
        self.context_window[session_id].append({"role": role, "content": content})
        if len(self.context_window[session_id]) > self.max_context_window:
            self.context_window[session_id].pop(0)

    def get_context(self, session_id: str) -> list[dict[str, str]]:
        return self.context_window.get(session_id, [])

    def persist(self, session_id: str | None = None) -> None:
        try:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = self._read_file_payload()
            if session_id is None:
                payload = {
                    sid: turns[-self.max_context_window :]
                    for sid, turns in self.context_window.items()
                    if turns
                }
            else:
                session_turns = self.context_window.get(session_id, [])
                if session_turns:
                    payload[session_id] = session_turns[-self.max_context_window :]
                elif session_id in payload:
                    del payload[session_id]

            with self._storage_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True, indent=2)
        except Exception:
            return

    def _load_from_file(self) -> None:
        payload = self._read_file_payload()

        for session_id, turns in payload.items():
            if not isinstance(session_id, str) or not isinstance(turns, list):
                continue
            all_data: list[dict[str, str]] = []

            for data in turns:
                if not isinstance(data, dict):
                    continue
                role = str(data.get("role", "")).strip()
                content = str(data.get("content", ""))
                if role in {"user", "assistant", "system"}:
                    all_data.append({"role": role, "content": content})
                    
            if all_data:
                self.context_window[session_id] = all_data[-self.max_context_window :]

    def _read_file_payload(self) -> dict:
        if not self._storage_path.exists():
            return {}
        try:
            with self._storage_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}