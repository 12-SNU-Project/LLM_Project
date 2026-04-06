from __future__ import annotations

from dataclasses import replace
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    from ..query.schema import QueryIntent
except ImportError:
    from query.schema import QueryIntent

from .schema import EvidenceBundle, VectorSearchHit


class EvidenceOrganizer:
    """Trim and reorder retrieval outputs before final answer generation."""

    def __init__(self, max_sql_rows: int = 12, max_vector_hits: int = 6) -> None:
        self.max_sql_rows = max_sql_rows
        self.max_vector_hits = max_vector_hits

    def organize(self, bundle: EvidenceBundle) -> EvidenceBundle:
        sql_rows = self._organize_sql_rows(bundle.sql_results, bundle.interpretation)
        vector_hits = self._organize_vector_hits(bundle.vector_hits, bundle.interpretation, sql_rows)
        citations = self._collect_citations(sql_rows, vector_hits)
        summary = {
            "sql_before": len(bundle.sql_results),
            "sql_after": len(sql_rows),
            "vector_before": len(bundle.vector_hits),
            "vector_after": len(vector_hits),
        }
        return replace(
            bundle,
            sql_results=sql_rows,
            vector_hits=vector_hits,
            citations=citations,
            retrieval_summary=summary,
        )

    def _organize_sql_rows(
        self,
        sql_rows: List[Dict[str, Any]],
        interpretation: Any,
    ) -> List[Dict[str, Any]]:
        seen: Set[Tuple[Any, ...]] = set()
        deduped: List[Dict[str, Any]] = []
        for row in sql_rows:
            key = (
                row.get("value_id"),
                row.get("table_id"),
                row.get("row_id"),
                row.get("column_key"),
                row.get("fiscal_year"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)

        if interpretation.intent in {
            QueryIntent.TABLE_CELL_LOOKUP,
            QueryIntent.COMPARISON_LIST_LOOKUP,
        }:
            return deduped[: self.max_sql_rows]

        reverse = interpretation.intent != QueryIntent.TREND_COMPARE
        deduped.sort(key=lambda row: self._sql_sort_key(row, interpretation), reverse=reverse)
        max_sql_rows = self.max_sql_rows
        if interpretation.intent == QueryIntent.TREND_COMPARE:
            metric_count = max(1, len(getattr(interpretation, "metric_candidates", []) or []))
            max_sql_rows = max(self.max_sql_rows, interpretation.limit * metric_count)
        return deduped[: max_sql_rows]

    def _sql_sort_key(self, row: Dict[str, Any], interpretation: Any) -> Tuple[Any, ...]:
        fiscal_year = row.get("fiscal_year") or 0
        exact_year = interpretation.year is not None and row.get("fiscal_year") == interpretation.year
        is_financial_table = row.get("table_role") == "financial_table"
        is_primary_value = bool(row.get("is_primary_value"))
        return (
            int(exact_year),
            int(is_financial_table),
            int(is_primary_value),
            fiscal_year,
            row.get("table_id") or "",
            row.get("row_index") or 0,
            row.get("col_index") or 0,
        )

    def _organize_vector_hits(
        self,
        hits: List[VectorSearchHit],
        interpretation: Any,
        sql_rows: List[Dict[str, Any]],
    ) -> List[VectorSearchHit]:
        sql_table_ids = {row.get("table_id") for row in sql_rows if row.get("table_id")}
        anchored_hits = [hit for hit in hits if self._is_anchor_match(hit, interpretation, sql_table_ids)]
        candidate_hits = anchored_hits or hits
        candidate_hits.sort(key=lambda hit: self._hit_sort_key(hit, interpretation, sql_table_ids), reverse=True)

        deduped: List[VectorSearchHit] = []
        seen = set()
        for hit in candidate_hits:
            if hit.document_id in seen:
                continue
            seen.add(hit.document_id)
            deduped.append(hit)
            if len(deduped) >= self.max_vector_hits:
                break
        return deduped

    def _is_anchor_match(
        self,
        hit: VectorSearchHit,
        interpretation: Any,
        sql_table_ids: Set[str],
    ) -> bool:
        metadata = hit.metadata
        topic_tags = metadata.get("topic_tags") or []
        same_year = (
            interpretation.year is None
            or metadata.get("fiscal_year") == interpretation.year
            or (
                interpretation.year_range is not None
                and metadata.get("fiscal_year") is not None
                and interpretation.year_range[0] <= metadata.get("fiscal_year") <= interpretation.year_range[1]
            )
        )
        topic_match = not interpretation.metric_candidates or any(
            topic in interpretation.metric_candidates for topic in [metadata.get("primary_topic_tag"), *topic_tags]
        )
        section_match = not interpretation.section_candidates or metadata.get("section_type") in interpretation.section_candidates
        table_match = bool(sql_table_ids) and metadata.get("near_table_id") in sql_table_ids

        if interpretation.intent == QueryIntent.TEXT_EXPLANATION:
            return same_year and section_match
        if interpretation.intent == QueryIntent.METRIC_WITH_EXPLANATION:
            return same_year and (topic_match or table_match or section_match)
        return same_year or topic_match or section_match or table_match

    def _hit_sort_key(
        self,
        hit: VectorSearchHit,
        interpretation: Any,
        sql_table_ids: Set[str],
    ) -> Tuple[Any, ...]:
        metadata = hit.metadata
        topic_tags = metadata.get("topic_tags") or []
        return (
            int(metadata.get("near_table_id") in sql_table_ids),
            int(interpretation.year is not None and metadata.get("fiscal_year") == interpretation.year),
            int(any(topic in interpretation.metric_candidates for topic in [metadata.get("primary_topic_tag"), *topic_tags])),
            int(metadata.get("section_type") in interpretation.section_candidates),
            hit.rerank_score or hit.similarity_score,
        )

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
