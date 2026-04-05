from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from ..llm.langchain_local import (
        LangChainLocalEmbedding,
        LangChainLocalLLM,
        LocalEmbeddingConfig,
        LocalLLMConfig,
    )
    from ..query.interpreter import QueryInterpreter
    from ..query.langchain_interpreter import LangChainQueryInterpreter
    from ..retrieval.chroma_metadata import ChromaMetadataBuilder
    from ..retrieval.chroma_store import ChromaVectorStore
    from ..retrieval.fusion import InMemoryVectorStore
    from ..support.answering.composer import LangChainAnswerComposer
except ImportError:
    from llm.langchain_local import (
        LangChainLocalEmbedding,
        LangChainLocalLLM,
        LocalEmbeddingConfig,
        LocalLLMConfig,
    )
    from query.interpreter import QueryInterpreter
    from query.langchain_interpreter import LangChainQueryInterpreter
    from retrieval.chroma_metadata import ChromaMetadataBuilder
    from retrieval.chroma_store import ChromaVectorStore
    from retrieval.fusion import InMemoryVectorStore
    from support.answering.composer import LangChainAnswerComposer


@dataclass
class PrototypeRuntimeConfig:
    intent_model: str = "qwen3:8b"
    answer_model: str = "qwen3:8b"
    embedding_model: str = "qwen3-embedding:8b"
    ollama_base_url: str = "http://localhost:11434"
    chroma_collection_name: str = "audit_chunks"
    chroma_persist_directory: Optional[str] = None
    prefer_langchain: bool = True
    prefer_chroma: bool = True
    allow_fallback: bool = True
    reset_chroma_collection: bool = False


@dataclass
class PrototypeRuntimeComponents:
    query_interpreter: Any
    answer_composer: LangChainAnswerComposer
    vector_store: Any
    metadata_builder: ChromaMetadataBuilder
    runtime_report: Dict[str, Any] = field(default_factory=dict)


class PrototypeRuntimeFactory:
    """Build the preferred LangChain/Ollama/Chroma path with safe fallbacks."""

    def __init__(self, config: Optional[PrototypeRuntimeConfig] = None) -> None:
        self.config = config or PrototypeRuntimeConfig()

    def build(
        self,
        parse_results: Optional[Iterable[Any]] = None,
        repo_root: Optional[Path] = None,
    ) -> PrototypeRuntimeComponents:
        metadata_builder = ChromaMetadataBuilder()
        documents = (
            [
                document
                for result in parse_results
                for document in metadata_builder.build_documents(result)
            ]
            if parse_results is not None
            else []
        )
        runtime_report: Dict[str, Any] = {
            "preferred_intent_model": self.config.intent_model,
            "preferred_answer_model": self.config.answer_model,
            "preferred_embedding_model": self.config.embedding_model,
            "prefer_langchain": self.config.prefer_langchain,
            "prefer_chroma": self.config.prefer_chroma,
            "vector_document_count": len(documents),
            "fallbacks": [],
        }

        query_interpreter = self._build_query_interpreter(runtime_report)
        answer_composer = self._build_answer_composer(runtime_report)
        vector_store = self._build_vector_store(
            documents,
            runtime_report,
            repo_root=repo_root,
            ingest_documents=bool(documents),
        )

        return PrototypeRuntimeComponents(
            query_interpreter=query_interpreter,
            answer_composer=answer_composer,
            vector_store=vector_store,
            metadata_builder=metadata_builder,
            runtime_report=runtime_report,
        )

    def _build_query_interpreter(self, runtime_report: Dict[str, Any]) -> Any:
        if not self.config.prefer_langchain:
            runtime_report["query_interpreter_backend"] = "heuristic"
            runtime_report["fallbacks"].append("query_interpreter=heuristic_by_config")
            return QueryInterpreter()

        intent_llm = LangChainLocalLLM(
            LocalLLMConfig(
                model=self.config.intent_model,
                base_url=self.config.ollama_base_url,
            )
        )
        runtime_report["query_interpreter_backend"] = "langchain_ollama"
        runtime_report["query_interpreter_runtime_available"] = LangChainLocalLLM.runtime_available()
        return LangChainQueryInterpreter(intent_llm)

    def _build_answer_composer(self, runtime_report: Dict[str, Any]) -> LangChainAnswerComposer:
        answer_llm = LangChainLocalLLM(
            LocalLLMConfig(
                model=self.config.answer_model,
                base_url=self.config.ollama_base_url,
            )
        )
        runtime_report["answer_backend"] = "langchain_ollama"
        runtime_report["answer_runtime_available"] = LangChainLocalLLM.runtime_available()
        return LangChainAnswerComposer(answer_llm)

    def _build_vector_store(
        self,
        documents: List[Any],
        runtime_report: Dict[str, Any],
        repo_root: Optional[Path] = None,
        ingest_documents: bool = True,
    ) -> Any:
        if not self.config.prefer_chroma:
            runtime_report["vector_backend"] = "in_memory"
            runtime_report["embedding_backend"] = "disabled_by_config"
            runtime_report["fallbacks"].append("vector_store=in_memory_by_config")
            return InMemoryVectorStore(documents)

        persist_directory = self._resolve_chroma_directory(repo_root)
        try:
            embedding_model = LangChainLocalEmbedding(
                LocalEmbeddingConfig(
                    model=self.config.embedding_model,
                    base_url=self.config.ollama_base_url,
                )
            )
            vector_store = ChromaVectorStore(
                collection_name=self.config.chroma_collection_name,
                persist_directory=str(persist_directory),
                embedding_model=embedding_model,
                reset_collection=self.config.reset_chroma_collection,
            )
            # Validate the collection at startup so service-mode failures are
            # reported before the first user query.
            _ = vector_store.collection
            # Keep ingestion separate from service-time retrieval loading.
            if ingest_documents:
                vector_store.upsert_documents(documents)
            runtime_report["vector_backend"] = "chroma"
            runtime_report["embedding_backend"] = "ollama_embeddings"
            runtime_report["embedding_runtime_available"] = LangChainLocalEmbedding.runtime_available()
            runtime_report["chroma_persist_directory"] = str(persist_directory)
            return vector_store
        except Exception as exc:
            runtime_report["vector_backend"] = "in_memory"
            runtime_report["embedding_backend"] = "fallback"
            runtime_report["embedding_runtime_available"] = LangChainLocalEmbedding.runtime_available()
            runtime_report["fallbacks"].append(f"vector_store={type(exc).__name__}")
            runtime_report["vector_fallback_reason"] = repr(exc)
            if not self.config.allow_fallback:
                raise
            return InMemoryVectorStore(documents)

    def _resolve_chroma_directory(self, repo_root: Optional[Path]) -> Path:
        if self.config.chroma_persist_directory:
            return Path(self.config.chroma_persist_directory)
        root = repo_root or Path.cwd()
        return root / ".runtime" / "chroma" / "audit_chunks"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self.config)
