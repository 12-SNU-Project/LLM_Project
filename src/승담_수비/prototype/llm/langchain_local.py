from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class LocalLLMConfig:
    provider: str = "ollama"
    model: str = "qwen2.5:7b"
    base_url: str = "http://localhost:11434"
    temperature: float = 0.0
    timeout: int = 120


class LangChainLocalLLM:
    def __init__(self, config: Optional[LocalLLMConfig] = None) -> None:
        self.config = config or LocalLLMConfig()

    def invoke_text(self, system_prompt: str, user_prompt: str) -> str:
        chat_model = self._create_chat_model()
        messages = self._build_messages(system_prompt, user_prompt)
        response = chat_model.invoke(messages)
        content = getattr(response, "content", response)
        if isinstance(content, list):
            return "\n".join(str(item) for item in content)
        return str(content)

    def invoke_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        raw = self.invoke_text(system_prompt, user_prompt)
        parsed = self._extract_json_object(raw)
        if not isinstance(parsed, dict):
            raise ValueError("LLM JSON output is not an object")
        return parsed

    def _create_chat_model(self) -> Any:
        if self.config.provider != "ollama":
            raise RuntimeError(f"Unsupported local LLM provider: {self.config.provider}")
        try:
            from langchain_ollama import ChatOllama
        except ImportError as exc:
            raise RuntimeError(
                "langchain_ollama 패키지가 필요합니다. "
                "로컬 LLM 사용 전 langchain-core/langchain-ollama를 설치하십시오."
            ) from exc
        return ChatOllama(
            model=self.config.model,
            base_url=self.config.base_url,
            temperature=self.config.temperature,
            timeout=self.config.timeout,
        )

    @staticmethod
    def _build_messages(system_prompt: str, user_prompt: str) -> Any:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
        except ImportError as exc:
            raise RuntimeError(
                "langchain_core 패키지가 필요합니다. "
                "로컬 LLM 사용 전 langchain-core를 설치하십시오."
            ) from exc
        return [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]

    @staticmethod
    def _extract_json_object(raw: str) -> Any:
        raw = raw.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end < 0 or end <= start:
            raise ValueError("JSON object not found in LLM output")
        return json.loads(raw[start:end + 1])
