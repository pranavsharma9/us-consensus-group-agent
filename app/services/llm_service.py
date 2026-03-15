from langchain_openai import ChatOpenAI

from app.core.config import Settings


class LLMService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def get_llm(self, temperature: float = 0.0) -> ChatOpenAI:
        return ChatOpenAI(
            model=self._settings.openai_model,
            api_key=self._settings.openai_api_key,
            temperature=temperature,
        )
