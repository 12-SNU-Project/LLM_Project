from __future__ import annotations

from dataclasses import replace

from .catalog import DEFAULT_EXPLANATION_SECTIONS
from .schema import QueryIntent, QueryInterpretation


class QueryRoutingPolicy:
    """Finalize LLM/rule output into deterministic retrieval routing."""

    MAX_LIMIT = 20
    DEFAULT_LIMIT = 10
    DEFAULT_TREND_WINDOW = 3

    def apply(self, interpretation: QueryInterpretation) -> QueryInterpretation:
        normalized = replace(
            interpretation,
            metric_candidates=list(interpretation.metric_candidates),
            section_candidates=list(interpretation.section_candidates),
            notes=list(interpretation.notes),
        )

        normalized.limit = max(1, min(normalized.limit or self.DEFAULT_LIMIT, self.MAX_LIMIT))
        if normalized.year_range is not None:
            normalized.year_range = tuple(sorted(normalized.year_range))

        # Keep the final route stable even if the upstream LLM drifts.
        if normalized.intent == QueryIntent.METRIC_LOOKUP:
            normalized.need_sql = True
            normalized.need_vdb = False
        elif normalized.intent == QueryIntent.TEXT_EXPLANATION:
            normalized.need_sql = False
            normalized.need_vdb = True
        elif normalized.intent == QueryIntent.METRIC_WITH_EXPLANATION:
            normalized.need_sql = True
            normalized.need_vdb = True
        elif normalized.intent == QueryIntent.TREND_COMPARE:
            normalized.need_sql = True
            normalized.need_vdb = False

        if normalized.intent in {QueryIntent.TEXT_EXPLANATION, QueryIntent.METRIC_WITH_EXPLANATION}:
            if not normalized.section_candidates:
                normalized.section_candidates = list(DEFAULT_EXPLANATION_SECTIONS)

        if normalized.intent == QueryIntent.TREND_COMPARE:
            if normalized.year_window is None and normalized.year_range is None:
                normalized.year_window = self.DEFAULT_TREND_WINDOW
                normalized.notes.append(f"default_year_window={self.DEFAULT_TREND_WINDOW}")

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

    @staticmethod
    def _infer_confidence(interpretation: QueryInterpretation) -> float:
        if interpretation.clarification_needed:
            return 0.25

        signal_count = sum(
            bool(signal)
            for signal in (
                interpretation.metric_candidates,
                interpretation.section_candidates,
                interpretation.year is not None or interpretation.year_range is not None,
            )
        )
        confidence = 0.55 + (0.1 * signal_count)
        if interpretation.intent == QueryIntent.TREND_COMPARE:
            confidence += 0.05
        if interpretation.intent == QueryIntent.METRIC_WITH_EXPLANATION:
            confidence += 0.05
        return max(0.0, min(confidence, 0.95))
