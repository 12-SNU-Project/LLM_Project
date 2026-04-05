from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, List, Optional, Protocol

try:
    from ..query.schema import QueryIntent, QueryInterpretation
except ImportError:
    from query.schema import QueryIntent, QueryInterpretation

from .chroma_metadata import ChromaMetadataBuilder
from .schema import EvidenceBundle, VectorSearchHit


TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣]+")


class VectorStoreProtocol(Protocol):
    def query(self, query_text: str, top_k: int = 8, where: Optional[Dict[str, Any]] = None) -> List[VectorSearchHit]:
        ...


class InMemoryVectorStore:
    def __init__(self, documents: Iterable[Any]) -> None:
        self.documents = list(documents)

    def query(self, query_text: str, top_k: int = 8, where: Optional[Dict[str, Any]] = None) -> List[VectorSearchHit]:
        hits: List[VectorSearchHit] = []
        for document in self.documents:
            metadata = dict(getattr(document, "metadata", {}))
            if where and not self._matches_where(metadata, where):
                continue
            similarity = self._similarity(query_text, getattr(document, "text", ""))
            hits.append(
                VectorSearchHit(
                    document_id=getattr(document, "document_id"),
                    text=getattr(document, "text", ""),
                    metadata=metadata,
                    similarity_score=similarity,
                )
            )
        hits.sort(key=lambda item: item.similarity_score, reverse=True)
        return hits[:top_k]

    @classmethod
    def _tokenize(cls, text: str) -> List[str]:
        return [match.group(0).lower() for match in TOKEN_RE.finditer(text or "")]

    @classmethod
    def _similarity(cls, query_text: str, document_text: str) -> float:
        query_tokens = set(cls._tokenize(query_text))
        document_tokens = set(cls._tokenize(document_text))
        if not query_tokens or not document_tokens:
            return 0.0
        overlap = len(query_tokens & document_tokens)
        return overlap / math.sqrt(len(query_tokens) * len(document_tokens))

    def _matches_where(self, metadata: Dict[str, Any], where: Dict[str, Any]) -> bool:
        if not where:
            return True
        if "$and" in where:
            return all(self._matches_where(metadata, clause) for clause in where["$and"])
        if "$or" in where:
            return any(self._matches_where(metadata, clause) for clause in where["$or"])

        for key, expected in where.items():
            actual = metadata.get(key)
            if isinstance(expected, dict):
                for op, operand in expected.items():
                    if op == "$eq" and actual != operand:
                        return False
                    if op == "$gte" and (actual is None or actual < operand):
                        return False
                    if op == "$lte" and (actual is None or actual > operand):
                        return False
                    if op == "$in" and actual not in operand:
                        return False
            else:
                if actual != expected:
                    return False
        return True


class RetrievalFusionEngine:
    def __init__(self, metadata_builder: Optional[ChromaMetadataBuilder] = None) -> None:
        self.metadata_builder = metadata_builder or ChromaMetadataBuilder()

    def retrieve(
        self,
        question: str,
        interpretation: QueryInterpretation,
        vector_store: VectorStoreProtocol,
        sql_rows: Optional[List[Dict[str, Any]]] = None,
        top_k: int = 8,
    ) -> EvidenceBundle:
        sql_rows = self._prepare_sql_rows(sql_rows or [], interpretation)
        if not interpretation.need_vdb:
            return EvidenceBundle(
                interpretation=interpretation,
                applied_vector_filter={},
                sql_results=sql_rows,
                vector_hits=[],
                citations=self._collect_citations(sql_rows, []),
            )
        where = self.metadata_builder.build_where(interpretation, sql_rows)
        vector_hits = vector_store.query(question, top_k=max(top_k * 2, top_k), where=where)
        if not vector_hits:
            fallback_where = self._build_fallback_where(interpretation)
            if fallback_where != where:
                where = fallback_where
                vector_hits = vector_store.query(question, top_k=max(top_k * 2, top_k), where=where)
        reranked = self._rerank_hits(vector_hits, interpretation, sql_rows)
        deduped = self._dedupe_hits(reranked)[:top_k]
        citations = self._collect_citations(sql_rows, deduped)
        return EvidenceBundle(
            interpretation=interpretation,
            applied_vector_filter=where,
            sql_results=sql_rows,
            vector_hits=deduped,
            citations=citations,
        )

    def build_generation_context(
        self,
        bundle: EvidenceBundle,
        max_sql_rows: int = 10,
        max_chunks: int = 6,
    ) -> Dict[str, Any]:
        return {
            "interpretation": bundle.interpretation.to_dict(),
            "retrieval_summary": bundle.retrieval_summary,
            "sql_results": bundle.sql_results[:max_sql_rows],
            "text_chunks": [
                {
                    "chunk_id": hit.document_id,
                    "text": hit.text,
                    "section_type": hit.metadata.get("section_type"),
                    "section_title": hit.metadata.get("section_title"),
                    "fiscal_year": hit.metadata.get("fiscal_year"),
                    "page_start": hit.metadata.get("page_start"),
                    "page_end": hit.metadata.get("page_end"),
                    "near_table_id": hit.metadata.get("near_table_id"),
                    "score": round(hit.rerank_score or hit.similarity_score, 4),
                }
                for hit in bundle.vector_hits[:max_chunks]
            ],
            "citations": bundle.citations,
        }

    def _build_fallback_where(self, interpretation: QueryInterpretation) -> Dict[str, Any]:
        fallback = QueryInterpretation(
            raw_question=interpretation.raw_question,
            intent=interpretation.intent,
            metric_candidates=[],
            year=interpretation.year,
            year_range=interpretation.year_range,
            year_window=interpretation.year_window,
            section_candidates=interpretation.section_candidates,
            need_sql=interpretation.need_sql,
            need_vdb=interpretation.need_vdb,
            comparison_mode=interpretation.comparison_mode,
            limit=interpretation.limit,
            notes=list(interpretation.notes),
        )
        return self.metadata_builder.build_where(fallback, [])

    def _prepare_sql_rows(
        self,
        sql_rows: List[Dict[str, Any]],
        interpretation: QueryInterpretation,
    ) -> List[Dict[str, Any]]:
        seen = set()
        deduped: List[Dict[str, Any]] = []
        for row in sql_rows:
            key = row.get("value_id") or (row.get("table_id"), row.get("row_id"), row.get("column_key"), row.get("fiscal_year"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)

        reverse = interpretation.intent != QueryIntent.TREND_COMPARE
        deduped.sort(
            key=lambda row: (
                row.get("fiscal_year") is None,
                row.get("fiscal_year") or 0,
                row.get("table_id") or "",
                row.get("row_index") or 0,
                row.get("col_index") or 0,
            ),
            reverse=reverse,
        )
        return deduped

    def _rerank_hits(
        self,
        hits: List[VectorSearchHit],
        interpretation: QueryInterpretation,
        sql_rows: List[Dict[str, Any]],
    ) -> List[VectorSearchHit]:
        sql_years = {row.get("fiscal_year") for row in sql_rows if row.get("fiscal_year") is not None}
        sql_table_ids = {row.get("table_id") for row in sql_rows if row.get("table_id")}
        topic_candidates = set(interpretation.metric_candidates)

        reranked: List[VectorSearchHit] = []
        for hit in hits:
            score = hit.similarity_score
            metadata = hit.metadata
            if interpretation.year is not None and metadata.get("fiscal_year") == interpretation.year:
                score += 0.15
            elif sql_years and metadata.get("fiscal_year") in sql_years:
                score += 0.10

            primary_topic = metadata.get("primary_topic_tag")
            if primary_topic in topic_candidates:
                score += 0.15
            if interpretation.section_candidates and metadata.get("section_type") in interpretation.section_candidates:
                score += 0.10
            if metadata.get("near_table_id") in sql_table_ids:
                score += 0.10

            hit.rerank_score = score
            reranked.append(hit)

        reranked.sort(key=lambda item: item.rerank_score, reverse=True)
        return reranked

    @staticmethod
    def _dedupe_hits(hits: List[VectorSearchHit]) -> List[VectorSearchHit]:
        seen = set()
        deduped: List[VectorSearchHit] = []
        for hit in hits:
            if hit.document_id in seen:
                continue
            seen.add(hit.document_id)
            deduped.append(hit)
        return deduped

    @staticmethod
    def _collect_citations(sql_rows: List[Dict[str, Any]], vector_hits: List[VectorSearchHit]) -> List[Dict[str, Any]]:
        citations: List[Dict[str, Any]] = []
        for row in sql_rows[:8]:
            citations.append(
                {
                    "kind": "sql_metric",
                    "filing_id": row.get("filing_id"),
                    "fiscal_year": row.get("fiscal_year"),
                    "table_id": row.get("table_id"),
                    "row_id": row.get("row_id"),
                    "column_key": row.get("column_key"),
                }
            )
        for hit in vector_hits[:8]:
            citations.append(
                {
                    "kind": "text_chunk",
                    "chunk_id": hit.document_id,
                    "filing_id": hit.metadata.get("filing_id"),
                    "fiscal_year": hit.metadata.get("fiscal_year"),
                    "page_start": hit.metadata.get("page_start"),
                    "page_end": hit.metadata.get("page_end"),
                    "near_table_id": hit.metadata.get("near_table_id"),
                }
            )
        return citations
