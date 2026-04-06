from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

try:
    from ..query.schema import QueryInterpretation
except ImportError:
    from query.schema import QueryInterpretation


@dataclass
class ChromaChunkDocument:
    document_id: str
    text: str
    metadata: Dict[str, Any]

    def to_chroma_record(self) -> Dict[str, Any]:
        metadata = dict(self.metadata)
        topic_tags = metadata.get("topic_tags")
        if isinstance(topic_tags, list):
            metadata["topic_tags_json"] = json.dumps(topic_tags, ensure_ascii=False)
            metadata["topic_tags"] = ",".join(topic_tags)
        metadata = self._sanitize_metadata(metadata)
        return {
            "id": self.document_id,
            "document": self.text,
            "metadata": metadata,
        }

    @staticmethod
    def _sanitize_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
        clean: Dict[str, Any] = {}
        for key, value in metadata.items():
            # Chroma metadata only accepts scalar primitives, so we drop None
            # and coerce unexpected values to strings before upsert.
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                clean[key] = value
                continue
            clean[key] = str(value)
        return clean


@dataclass
class VectorSearchHit:
    document_id: str
    text: str
    metadata: Dict[str, Any]
    similarity_score: float
    rerank_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EvidenceBundle:
    interpretation: QueryInterpretation
    applied_vector_filter: Dict[str, Any]
    sql_results: List[Dict[str, Any]] = field(default_factory=list)
    vector_hits: List[VectorSearchHit] = field(default_factory=list)
    table_contexts: List[Dict[str, Any]] = field(default_factory=list)
    evidence_requirements: List[str] = field(default_factory=list)
    evidence_gaps: List[str] = field(default_factory=list)
    citations: List[Dict[str, Any]] = field(default_factory=list)
    retrieval_summary: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["interpretation"] = self.interpretation.to_dict()
        return payload
