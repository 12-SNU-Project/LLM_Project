from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class LocalLLMConfig:
    provider: str = "ollama"
    model: str = "qwen3:8b"
    base_url: str = "http://127.0.0.1:11434"
    temperature: float = 0.0
    timeout: int = 120
    num_gpu: Optional[int] = 1
    num_thread: Optional[int] = None
    keep_alive: Optional[int] = 300


@dataclass
class LocalEmbeddingConfig:
    provider: str = "ollama"
    model: str = "qwen3-embedding:8b"
    base_url: str = "http://127.0.0.1:11434"
    timeout: int = 120
    keep_alive: Optional[int] = 300
    num_gpu: Optional[int] = 1
    num_thread: Optional[int] = None


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
            num_gpu=self.config.num_gpu,
            num_thread=self.config.num_thread,
            keep_alive=self.config.keep_alive,
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

    @staticmethod
    def runtime_available() -> bool:
        return all(
            importlib.util.find_spec(module) is not None
            for module in ("langchain_core", "langchain_ollama")
        )


class LangChainLocalEmbedding:
    def __init__(self, config: Optional[LocalEmbeddingConfig] = None) -> None:
        self.config = config or LocalEmbeddingConfig()
        self._embedding_model: Optional[Any] = None

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        embedding_model = self._create_embedding_model()
        return [list(vector) for vector in embedding_model.embed_documents(texts)]

    def embed_query(self, text: str) -> List[float]:
        embedding_model = self._create_embedding_model()
        return list(embedding_model.embed_query(text))

    def to_chroma_embedding_function(self) -> Any:
        # Chroma keeps the embedding boundary separate from the LLM path.
        config_payload = {
            "provider": self.config.provider,
            "model": self.config.model,
            "base_url": self.config.base_url,
            "timeout": self.config.timeout,
            "keep_alive": self.config.keep_alive,
            "num_gpu": self.config.num_gpu,
            "num_thread": self.config.num_thread,
        }
        function_name = self._chroma_function_name(config_payload)

        class _EmbeddingFunction:
            def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
                self._config = dict(config or config_payload)
                self._embedding = LangChainLocalEmbedding(LocalEmbeddingConfig(**self._config))

            def __call__(self, input: List[str]) -> List[List[float]]:
                return self._embedding.embed_documents(list(input))

            def embed_query(self, input: Any) -> List[float]:
                if isinstance(input, list):
                    if not input:
                        raise ValueError("query input is empty")
                    input = input[0]
                return self._embedding.embed_query(str(input))

            @staticmethod
            def name() -> str:
                return function_name

            @staticmethod
            def build_from_config(config: Dict[str, Any]) -> "_EmbeddingFunction":
                return _EmbeddingFunction(
                    {
                        "provider": str(config.get("provider") or config_payload["provider"]),
                        "model": str(config.get("model") or config_payload["model"]),
                        "base_url": str(config.get("base_url") or config_payload["base_url"]),
                        "timeout": int(config.get("timeout") or config_payload["timeout"]),
                        "keep_alive": (
                            int(config["keep_alive"])
                            if config.get("keep_alive") is not None
                            else config_payload["keep_alive"]
                        ),
                        "num_gpu": (
                            int(config["num_gpu"])
                            if config.get("num_gpu") is not None
                            else config_payload["num_gpu"]
                        ),
                        "num_thread": (
                            int(config["num_thread"])
                            if config.get("num_thread") is not None
                            else config_payload["num_thread"]
                        ),
                    }
                )

            def get_config(self) -> Dict[str, Any]:
                return dict(self._config)

            @staticmethod
            def default_space() -> str:
                return "cosine"

            @staticmethod
            def supported_spaces() -> List[str]:
                return ["cosine", "l2", "ip"]

        return _EmbeddingFunction()

    @staticmethod
    def _chroma_function_name(config: Dict[str, Any]) -> str:
        raw_name = f"ohsd_{config['provider']}_{config['model']}"
        sanitized = "".join(char if char.isalnum() else "_" for char in raw_name)
        return sanitized.strip("_").lower()

    def _create_embedding_model(self) -> Any:
        if self._embedding_model is not None:
            return self._embedding_model
        if self.config.provider != "ollama":
            raise RuntimeError(f"Unsupported local embedding provider: {self.config.provider}")
        try:
            from langchain_ollama import OllamaEmbeddings
        except ImportError as exc:
            raise RuntimeError(
                "langchain_ollama 패키지가 필요합니다. "
                "임베딩 사용 전 langchain-core/langchain-ollama를 설치하십시오."
            ) from exc
        self._embedding_model = OllamaEmbeddings(
            model=self.config.model,
            base_url=self.config.base_url,
            sync_client_kwargs={"timeout": float(self.config.timeout)},
            keep_alive=self.config.keep_alive,
            num_gpu=self.config.num_gpu,
            num_thread=self.config.num_thread,
        )
        return self._embedding_model

    @staticmethod
    def runtime_available() -> bool:
        return importlib.util.find_spec("langchain_ollama") is not None
