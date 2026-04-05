from __future__ import annotations
import re
from typing import Any, Dict, Iterable, List, Optional

try:
    from ..query.catalog import METRIC_DEFINITIONS
    from ..query.schema import QueryInterpretation
except ImportError:
    from query.catalog import METRIC_DEFINITIONS
    from query.schema import QueryInterpretation

from .schema import ChromaChunkDocument


TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣]+")


class ChromaMetadataBuilder:
    def build_documents(self, parse_result: Any) -> List[ChromaChunkDocument]:
        block_by_id = {block.block_id: block for block in getattr(parse_result, "blocks", [])}
        table_by_id = {table.table_id: table for table in getattr(parse_result, "tables", [])}
        documents: List[ChromaChunkDocument] = []

        for chunk in getattr(parse_result, "text_chunks", []):
            start_block = block_by_id.get(chunk.start_block_id)
            end_block = block_by_id.get(chunk.end_block_id)
            near_table = table_by_id.get(chunk.near_table_id) if getattr(chunk, "near_table_id", None) else None
            topic_tags = self._extract_topic_tags(chunk=chunk, near_table=near_table)
            primary_topic_tag = topic_tags[0] if topic_tags else (chunk.topic_hint or chunk.section_type or "general")
            # Keep Chroma metadata focused on retrieval filters and citations.
            metadata = {
                "chunk_id": chunk.chunk_id,
                "filing_id": chunk.filing_id,
                "fiscal_year": chunk.fiscal_year,
                "section_type": chunk.section_type or "unknown",
                "section_title": chunk.section_title or "",
                "near_table_id": chunk.near_table_id,
                "topic_hint": chunk.topic_hint or "",
                "primary_topic_tag": primary_topic_tag,
                "topic_tags": topic_tags,
                "page_start": getattr(chunk, "page_start", None) or getattr(start_block, "page_index", None),
                "page_end": getattr(chunk, "page_end", None) or getattr(end_block, "page_index", None),
            }
            documents.append(
                ChromaChunkDocument(
                    document_id=chunk.chunk_id,
                    text=chunk.text,
                    metadata=metadata,
                )
            )
        return documents

    def build_where(self, interpretation: QueryInterpretation, sql_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        clauses: List[Dict[str, Any]] = []

        target_year = interpretation.year
        if target_year is None and sql_rows:
            years = {row.get("fiscal_year") for row in sql_rows if row.get("fiscal_year") is not None}
            if len(years) == 1:
                target_year = next(iter(years))

        if target_year is not None:
            clauses.append({"fiscal_year": target_year})
        elif interpretation.year_range is not None:
            start_year, end_year = interpretation.year_range
            clauses.append({"fiscal_year": {"$gte": start_year}})
            clauses.append({"fiscal_year": {"$lte": end_year}})

        if interpretation.section_candidates:
            if len(interpretation.section_candidates) == 1:
                clauses.append({"section_type": interpretation.section_candidates[0]})
            else:
                clauses.append(
                    {
                        "$or": [
                            {"section_type": section_type}
                            for section_type in interpretation.section_candidates
                        ]
                    }
                )

        topic_candidates = self._collect_topic_candidates(interpretation, sql_rows or [])
        if topic_candidates:
            if len(topic_candidates) == 1:
                topic = topic_candidates[0]
                clauses.append(
                    {
                        "$or": [
                            {"primary_topic_tag": topic},
                            {"topic_hint": topic},
                        ]
                    }
                )
            else:
                clauses.append(
                    {
                        "$or": [
                            {"primary_topic_tag": topic}
                            for topic in topic_candidates
                        ]
                        + [
                            {"topic_hint": topic}
                            for topic in topic_candidates
                        ]
                    }
                )

        if not clauses:
            return {}
        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return [match.group(0).lower() for match in TOKEN_RE.finditer(text or "")]

    def _extract_topic_tags(self, chunk: Any, near_table: Any = None) -> List[str]:
        haystack = " ".join(
            part
            for part in (
                getattr(chunk, "section_title", None),
                getattr(chunk, "topic_hint", None),
                getattr(chunk, "text", None),
                getattr(near_table, "title", None),
            )
            if part
        )
        compact = "".join(self._tokenize(haystack))
        tags: List[str] = []

        if getattr(chunk, "topic_hint", None):
            tags.append(str(chunk.topic_hint))
        if getattr(chunk, "section_type", None) and chunk.section_type not in {"cover", "note_section"}:
            tags.append(str(chunk.section_type))

        for metric_id, definition in METRIC_DEFINITIONS.items():
            if any(alias.replace(" ", "").lower() in compact for alias in definition.row_label_aliases + definition.aliases):
                if metric_id not in tags:
                    tags.append(metric_id)
                for topic_tag in definition.topic_tags:
                    if topic_tag not in tags:
                        tags.append(topic_tag)

        return tags[:8]

    def _collect_topic_candidates(
        self,
        interpretation: QueryInterpretation,
        sql_rows: Iterable[Dict[str, Any]],
    ) -> List[str]:
        topics: List[str] = []
        for metric_id in interpretation.metric_candidates:
            if metric_id not in topics:
                topics.append(metric_id)
        for row in sql_rows:
            normalized_label = str(row.get("normalized_label") or row.get("raw_label") or "")
            compact = "".join(self._tokenize(normalized_label))
            for metric_id, definition in METRIC_DEFINITIONS.items():
                if any(alias.replace(" ", "").lower() in compact for alias in definition.row_label_aliases):
                    if metric_id not in topics:
                        topics.append(metric_id)
        return topics
