from __future__ import annotations

import re
from dataclasses import replace

from .catalog import DEFAULT_EXPLANATION_SECTIONS
from .schema import QueryIntent, QueryInterpretation


class QueryRoutingPolicy:
    """Finalize LLM/rule output into deterministic retrieval routing."""

    MAX_LIMIT = 20
    DEFAULT_LIMIT = 10
    DEFAULT_TREND_WINDOW = 3
    YEAR_TOKEN_RE = re.compile(r"^(19\d{2}|20\d{2})$")

    def apply(self, interpretation: QueryInterpretation) -> QueryInterpretation:
        normalized = replace(
            interpretation,
            metric_candidates=list(interpretation.metric_candidates),
            row_label_filters=list(interpretation.row_label_filters),
            row_label_terms=list(interpretation.row_label_terms),
            column_terms=list(interpretation.column_terms),
            table_title_terms=list(interpretation.table_title_terms),
            section_candidates=list(interpretation.section_candidates),
            notes=list(interpretation.notes),
        )

        normalized.limit = max(1, min(normalized.limit or self.DEFAULT_LIMIT, self.MAX_LIMIT))
        if normalized.year_range is not None:
            normalized.year_range = tuple(sorted(normalized.year_range))
        normalized = self._sanitize_year_like_anchors(normalized)

        had_section_anchor = bool(normalized.section_candidates)

        # Keep the final route stable even if the upstream LLM drifts.
        if normalized.intent == QueryIntent.METRIC_LOOKUP:
            normalized.need_sql = True
            normalized.need_vdb = False
        elif normalized.intent == QueryIntent.TEXT_EXPLANATION:
            structure_lookup = "table_structure_lookup" in normalized.notes
            metric_drift = "metric_candidates_empty" in normalized.notes
            if normalized.table_title_terms and (structure_lookup or metric_drift):
                # 표 제목이 명확한 구조 질문은 일반 metric 추론보다 표 구조 조회를 우선한다.
                normalized.metric_candidates = []
            normalized.need_sql = structure_lookup or metric_drift
            normalized.need_vdb = True
        elif normalized.intent == QueryIntent.METRIC_WITH_EXPLANATION:
            normalized.need_sql = True
            normalized.need_vdb = True
        elif normalized.intent == QueryIntent.TREND_COMPARE:
            normalized.need_sql = True
            normalized.need_vdb = False
        elif normalized.intent == QueryIntent.TABLE_CELL_LOOKUP:
            normalized.need_sql = True
            normalized.need_vdb = False
        elif normalized.intent == QueryIntent.COMPARISON_LIST_LOOKUP:
            normalized.need_sql = True
            normalized.need_vdb = False

        if normalized.intent in {QueryIntent.TEXT_EXPLANATION, QueryIntent.METRIC_WITH_EXPLANATION}:
            if not normalized.section_candidates:
                normalized.section_candidates = list(DEFAULT_EXPLANATION_SECTIONS)

        # Broad qualitative questions without a concrete section anchor should
        # not drift into VDB-only answers; ask for a tighter metric/section.
        if normalized.intent == QueryIntent.TEXT_EXPLANATION:
            has_table_anchor = bool(normalized.table_title_terms)
            if not normalized.metric_candidates and not had_section_anchor and not has_table_anchor:
                normalized.need_sql = False
                normalized.need_vdb = False
                normalized.clarification_needed = True
                normalized.clarification_reason = normalized.clarification_reason or "section_or_metric_required"
                normalized.notes.append("clarification_required=section_or_metric_required")

        if normalized.intent == QueryIntent.TREND_COMPARE:
            if normalized.year_window is None and normalized.year_range is None:
                normalized.year_window = self.DEFAULT_TREND_WINDOW
                normalized.notes.append(f"default_year_window={self.DEFAULT_TREND_WINDOW}")

        if normalized.intent == QueryIntent.TABLE_CELL_LOOKUP:
            # 행/열/표 제목 중 최소 하나는 있어야 셀 단위 조회가 성립한다.
            if not (
                normalized.row_label_terms
                or normalized.column_terms
                or normalized.table_title_terms
                or normalized.row_label_filters
            ):
                normalized.need_sql = False
                normalized.need_vdb = False
                normalized.clarification_needed = True
                normalized.clarification_reason = "cell_anchor_required"
                normalized.notes.append("clarification_required=cell_anchor_required")

        if normalized.intent == QueryIntent.COMPARISON_LIST_LOOKUP:
            has_comparison_target = bool(normalized.column_terms or normalized.table_title_terms or normalized.entity_scope)
            if normalized.comparison_operator is None or normalized.threshold_value is None or not has_comparison_target:
                normalized.need_sql = False
                normalized.need_vdb = False
                normalized.clarification_needed = True
                normalized.clarification_reason = "comparison_condition_required"
                normalized.notes.append("clarification_required=comparison_condition_required")

        if normalized.intent in {
            QueryIntent.METRIC_LOOKUP,
            QueryIntent.METRIC_WITH_EXPLANATION,
            QueryIntent.TREND_COMPARE,
        } and not normalized.metric_candidates:
            normalized.need_sql = False
            normalized.need_vdb = False
            normalized.clarification_needed = True
            normalized.clarification_reason = "metric_required"
            normalized.notes.append("clarification_required=metric_required")

        if normalized.confidence is None:
            normalized.confidence = self._infer_confidence(normalized)
        else:
            normalized.confidence = max(0.0, min(float(normalized.confidence), 1.0))

        return normalized

    def _sanitize_year_like_anchors(self, interpretation: QueryInterpretation) -> QueryInterpretation:
        year_tokens = set()
        if interpretation.year is not None:
            year_tokens.add(str(interpretation.year))
        if interpretation.year_range is not None:
            year_tokens.update(str(year) for year in interpretation.year_range)

        def _clean(values: list[str]) -> list[str]:
            cleaned: list[str] = []
            for value in values:
                token = str(value).strip()
                if self.YEAR_TOKEN_RE.match(token) and token in year_tokens:
                    continue  # 연도는 표의 행/열 anchor가 아니라 연도 조건으로만 남긴다.
                cleaned.append(value)
            return cleaned

        original_filters = list(interpretation.row_label_filters)
        original_terms = list(interpretation.row_label_terms)
        cleaned_filters = _clean(original_filters)
        cleaned_terms = _clean(original_terms)
        if cleaned_filters == original_filters and cleaned_terms == original_terms:
            return interpretation

        notes = list(interpretation.notes)
        notes.append("year_anchor_sanitized")
        return replace(
            interpretation,
            row_label_filters=cleaned_filters,
            row_label_terms=cleaned_terms,
            notes=notes,
        )

    @staticmethod
    def _infer_confidence(interpretation: QueryInterpretation) -> float:
        if interpretation.clarification_needed:
            return 0.25

        signal_count = sum(
            bool(signal)
            for signal in (
                interpretation.metric_candidates,
                interpretation.section_candidates,
                interpretation.row_label_terms,
                interpretation.column_terms,
                interpretation.entity_scope,
                interpretation.threshold_value is not None,
                interpretation.year is not None or interpretation.year_range is not None,
            )
        )
        confidence = 0.55 + (0.1 * signal_count)
        if interpretation.intent == QueryIntent.TREND_COMPARE:
            confidence += 0.05
        if interpretation.intent == QueryIntent.METRIC_WITH_EXPLANATION:
            confidence += 0.05
        return max(0.0, min(confidence, 0.95))
