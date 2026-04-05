from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from ..llm.langchain_local import LangChainLocalEmbedding
except ImportError:
    from llm.langchain_local import LangChainLocalEmbedding

from .schema import ChromaChunkDocument, VectorSearchHit


@dataclass
class ChromaStoreConfig:
    collection_name: str = "audit_chunks"
    persist_directory: Optional[str] = None
    reset_collection: bool = False


class ChromaVectorStore:
    def __init__(
        self,
        collection: Optional[Any] = None,
        collection_name: str = "audit_chunks",
        persist_directory: Optional[str] = None,
        embedding_model: Optional[LangChainLocalEmbedding] = None,
        reset_collection: bool = False,
    ) -> None:
        self._collection = collection
        self.collection_name = collection_name
        self.persist_directory = persist_directory
        self.embedding_model = embedding_model
        self.reset_collection = reset_collection

    @property
    def collection(self) -> Any:
        if self._collection is None:
            self._collection = self._create_default_collection()
        return self._collection

    def upsert_documents(self, documents: Iterable[ChromaChunkDocument]) -> None:
        docs = list(documents)
        if not docs:
            return
        self.collection.upsert(
            ids=[doc.document_id for doc in docs],
            documents=[doc.text for doc in docs],
            metadatas=[doc.to_chroma_record()["metadata"] for doc in docs],
        )

    def query(self, query_text: str, top_k: int = 8, where: Optional[Dict[str, Any]] = None) -> List[VectorSearchHit]:
        result = self.collection.query(
            query_texts=[query_text],
            n_results=top_k,
            where=where or None,
        )
        ids = result.get("ids", [[]])[0]
        documents = result.get("documents", [[]])[0]
        metadatas = result.get("metadatas", [[]])[0]
        distances = result.get("distances", [[]])[0] if result.get("distances") else [0.0] * len(ids)

        hits: List[VectorSearchHit] = []
        for doc_id, document, metadata, distance in zip(ids, documents, metadatas, distances):
            similarity = 1.0 / (1.0 + float(distance))
            hits.append(
                VectorSearchHit(
                    document_id=str(doc_id),
                    text=str(document),
                    metadata=dict(metadata or {}),
                    similarity_score=similarity,
                )
            )
        return hits

    def _create_default_collection(self) -> Any:
        self._disable_telemetry()
        try:
            import chromadb
        except ImportError as exc:
            raise RuntimeError(
                "chromadb 패키지가 필요합니다. "
                "ChromaVectorStore 사용 전 chromadb를 설치하십시오."
            ) from exc

        settings = self._build_client_settings()
        if self.persist_directory:
            persist_path = Path(self.persist_directory)
            persist_path.mkdir(parents=True, exist_ok=True)
            client = self._create_client(
                chromadb,
                path=str(persist_path),
                settings=settings,
            )
        else:
            client = self._create_client(chromadb, settings=settings)

        if self.reset_collection:
            try:
                client.delete_collection(name=self.collection_name)
            except Exception:
                pass

        embedding_function = None
        if self.embedding_model is not None:
            embedding_function = self.embedding_model.to_chroma_embedding_function()

        return client.get_or_create_collection(
            name=self.collection_name,
            embedding_function=embedding_function,
        )

    def _build_client_settings(self) -> Optional[Any]:
        try:
            from chromadb.config import Settings
        except Exception:
            return None

        # This project runs Chroma as a local embedded component, so
        # anonymized telemetry only adds noise in offline environments.
        return Settings(anonymized_telemetry=False)

    @staticmethod
    def _disable_telemetry() -> None:
        # Keep local/offline runs quiet and deterministic even when the
        # installed Chroma/PostHog versions disagree on telemetry hooks.
        os.environ.setdefault("ANONYMIZED_TELEMETRY", "FALSE")
        os.environ.setdefault("CHROMA_ANONYMIZED_TELEMETRY", "FALSE")
        os.environ.setdefault("POSTHOG_DISABLED", "1")
        os.environ.setdefault("DISABLE_POSTHOG", "1")

        try:
            import posthog

            posthog.disabled = True
            if hasattr(posthog, "capture"):
                posthog.capture = lambda *args, **kwargs: None
        except Exception:
            pass

    def _create_client(
        self,
        chromadb: Any,
        *,
        path: Optional[str] = None,
        settings: Optional[Any] = None,
    ) -> Any:
        if path:
            try:
                return chromadb.PersistentClient(path=path, settings=settings)
            except TypeError:
                return chromadb.PersistentClient(path=path)

        try:
            return chromadb.Client(settings=settings)
        except TypeError:
            return chromadb.Client()
