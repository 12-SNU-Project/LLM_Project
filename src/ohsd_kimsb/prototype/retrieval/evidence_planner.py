from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set

try:
    from ..query.schema import QueryIntent, QueryInterpretation
except ImportError:
    from query.schema import QueryIntent, QueryInterpretation

from .schema import EvidenceBundle


class EvidenceDimension(str, Enum):
    NUMERIC = "numeric"
    NARRATIVE = "narrative"
    STRUCTURE = "structure"
    UNIT = "unit"
    FOOTNOTE = "footnote"


@dataclass
class EvidenceAssessment:
    required_dimensions: List[str] = field(default_factory=list)
    satisfied_dimensions: List[str] = field(default_factory=list)
    missing_dimensions: List[str] = field(default_factory=list)
    table_expansion_dimensions: List[str] = field(default_factory=list)
    candidate_table_ids: List[str] = field(default_factory=list)
    semantic_table_types: List[str] = field(default_factory=list)
    table_title_terms: List[str] = field(default_factory=list)
    filing_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class EvidenceExpansionPlanner:
    """Assess evidence sufficiency and plan generic table-context expansion."""

    INTENT_REQUIREMENTS = {
        QueryIntent.METRIC_LOOKUP: {EvidenceDimension.NUMERIC, EvidenceDimension.UNIT},
        QueryIntent.TEXT_EXPLANATION: {EvidenceDimension.NARRATIVE},
        QueryIntent.METRIC_WITH_EXPLANATION: {
            EvidenceDimension.NUMERIC,
            EvidenceDimension.NARRATIVE,
            EvidenceDimension.UNIT,
        },
        QueryIntent.TREND_COMPARE: {EvidenceDimension.NUMERIC, EvidenceDimension.UNIT},
        QueryIntent.TABLE_CELL_LOOKUP: {EvidenceDimension.NUMERIC, EvidenceDimension.UNIT},
        QueryIntent.COMPARISON_LIST_LOOKUP: {EvidenceDimension.NUMERIC, EvidenceDimension.UNIT},
    }
    TABLE_DIMENSIONS = {
        EvidenceDimension.STRUCTURE,
        EvidenceDimension.UNIT,
        EvidenceDimension.FOOTNOTE,
    }
    DIMENSION_ORDER = [
        EvidenceDimension.NUMERIC,
        EvidenceDimension.NARRATIVE,
        EvidenceDimension.STRUCTURE,
        EvidenceDimension.UNIT,
        EvidenceDimension.FOOTNOTE,
    ]
    FOOTNOTE_MARKER_RE = re.compile(r"\(\*\d+\)|\[\*\d+\]|\b(?:각주|주석)\b")

    def assess(self, bundle: EvidenceBundle) -> EvidenceAssessment:
        required = self._infer_requirements(bundle.interpretation, bundle.sql_results)
        satisfied = self._infer_satisfied_dimensions(bundle)
        missing = [dimension for dimension in self.DIMENSION_ORDER if dimension in required and dimension not in satisfied]
        candidate_table_ids = self._collect_candidate_table_ids(bundle)
        semantic_table_types = self._collect_semantic_table_types(bundle.sql_results)
        table_expansion_dimensions = [
            dimension.value for dimension in missing if dimension in self.TABLE_DIMENSIONS
        ]
        if (
            EvidenceDimension.NARRATIVE in missing
            and (candidate_table_ids or semantic_table_types)
            and EvidenceDimension.STRUCTURE.value not in table_expansion_dimensions
        ):
            table_expansion_dimensions.append(EvidenceDimension.STRUCTURE.value)
        return EvidenceAssessment(
            required_dimensions=[dimension.value for dimension in self.DIMENSION_ORDER if dimension in required],
            satisfied_dimensions=[dimension.value for dimension in self.DIMENSION_ORDER if dimension in satisfied],
            missing_dimensions=[dimension.value for dimension in missing],
            table_expansion_dimensions=table_expansion_dimensions,
            candidate_table_ids=candidate_table_ids,
            semantic_table_types=semantic_table_types,
            table_title_terms=self._dedupe_strs(bundle.interpretation.table_title_terms),
            filing_id=self._resolve_filing_id(bundle),
        )

    @classmethod
    def requires_table_contexts(cls, assessment: EvidenceAssessment) -> bool:
        if not assessment.table_expansion_dimensions:
            return False
        return bool(assessment.candidate_table_ids or (assessment.filing_id and assessment.semantic_table_types))

    def _infer_requirements(
        self,
        interpretation: QueryInterpretation,
        sql_rows: List[Dict[str, Any]],
    ) -> Set[EvidenceDimension]:
        required = set(self.INTENT_REQUIREMENTS.get(interpretation.intent, set()))
        if self._needs_structural_context(interpretation):
            required.add(EvidenceDimension.STRUCTURE)
        if self._needs_footnote_context(interpretation, sql_rows):
            required.add(EvidenceDimension.FOOTNOTE)
        return required

    @staticmethod
    def _needs_structural_context(interpretation: QueryInterpretation) -> bool:
        if interpretation.intent == QueryIntent.TABLE_CELL_LOOKUP:
            return bool(
                interpretation.table_title_terms
                or interpretation.row_label_terms
                or len(interpretation.column_terms) > 1
            )
        if interpretation.intent == QueryIntent.TEXT_EXPLANATION:
            return bool(
                interpretation.table_title_terms
                or interpretation.row_label_terms
                or interpretation.column_terms
            )
        return False

    def _needs_footnote_context(
        self,
        interpretation: QueryInterpretation,
        sql_rows: List[Dict[str, Any]],
    ) -> bool:
        if self.FOOTNOTE_MARKER_RE.search(interpretation.raw_question or ""):
            return True
        for row in sql_rows:
            for value in (row.get("raw_label"), row.get("value_raw"), row.get("table_title")):
                if self.FOOTNOTE_MARKER_RE.search(str(value or "")):
                    return True
        return False

    @staticmethod
    def _infer_satisfied_dimensions(bundle: EvidenceBundle) -> Set[EvidenceDimension]:
        satisfied: Set[EvidenceDimension] = set()
        if bundle.sql_results:
            satisfied.add(EvidenceDimension.NUMERIC)
        if bundle.vector_hits:
            satisfied.add(EvidenceDimension.NARRATIVE)
        if any(str(row.get("unit") or row.get("table_unit") or "").strip() for row in bundle.sql_results):
            satisfied.add(EvidenceDimension.UNIT)
        if any(str(context.get("table_unit") or "").strip() for context in bundle.table_contexts):
            satisfied.add(EvidenceDimension.UNIT)
        if any(str(context.get("table_markdown") or "").strip() for context in bundle.table_contexts):
            satisfied.add(EvidenceDimension.STRUCTURE)
        if any(str(context.get("footnotes") or "").strip() for context in bundle.table_contexts):
            satisfied.add(EvidenceDimension.FOOTNOTE)
        return satisfied

    @staticmethod
    def _collect_candidate_table_ids(bundle: EvidenceBundle) -> List[str]:
        candidates: List[str] = []
        for table_id in EvidenceExpansionPlanner._iter_table_ids(bundle):
            if table_id not in candidates:
                candidates.append(table_id)
        return candidates

    @staticmethod
    def _iter_table_ids(bundle: EvidenceBundle) -> Iterable[str]:
        for row in bundle.sql_results:
            table_id = str(row.get("table_id") or "").strip()
            if table_id:
                yield table_id
        for hit in bundle.vector_hits:
            table_id = str(hit.metadata.get("near_table_id") or "").strip()
            if table_id:
                yield table_id
        for context in bundle.table_contexts:
            table_id = str(context.get("table_id") or "").strip()
            if table_id:
                yield table_id

    @staticmethod
    def _collect_semantic_table_types(sql_rows: List[Dict[str, Any]]) -> List[str]:
        semantic_types: List[str] = []
        for row in sql_rows:
            semantic_type = str(row.get("semantic_table_type") or "").strip()
            if semantic_type and semantic_type not in semantic_types:
                semantic_types.append(semantic_type)
        return semantic_types

    @staticmethod
    def _resolve_filing_id(bundle: EvidenceBundle) -> Optional[str]:
        for row in bundle.sql_results:
            filing_id = str(row.get("filing_id") or "").strip()
            if filing_id:
                return filing_id
        for hit in bundle.vector_hits:
            filing_id = str(hit.metadata.get("filing_id") or "").strip()
            if filing_id:
                return filing_id
        for context in bundle.table_contexts:
            filing_id = str(context.get("filing_id") or "").strip()
            if filing_id:
                return filing_id
        return None

    @staticmethod
    def _dedupe_strs(values: Iterable[Any]) -> List[str]:
        deduped: List[str] = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in deduped:
                deduped.append(text)
        return deduped
