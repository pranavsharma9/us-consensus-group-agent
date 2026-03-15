from app.core.config import Settings
from collections import defaultdict
from typing import Literal

class AgentContext:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.context_window=defaultdict(list)
        self.max_context_window=settings.max_context_window

    def add_context(self, sessionId: str, role: Literal["user", "assistant", "system"], content: str) -> None:
        self.context_window[sessionId].append({"role": role, "content": content})
        if len(self.context_window[sessionId]) > self.max_context_window:
            self.context_window[sessionId].pop(0)

    def get_context(self, sessionId: str) -> list[dict[str, str]]:
        return self.context_window[sessionId]