from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional

from .catalog import (
    DEFAULT_EXPLANATION_SECTIONS,
    METRIC_ALIAS_TO_ID,
    METRIC_DEFINITIONS,
    SECTION_ALIAS_TO_GROUP,
    SECTION_GROUPS,
    compact_token,
)
from .policy import QueryRoutingPolicy
from .schema import INTERPRETATION_JSON_SCHEMA, QueryIntent, QueryInterpretation


YEAR_RE = re.compile(r"(19\d{2}|20\d{2})")
YEAR_WINDOW_RE = re.compile(r"(?:최근|최신)\s*(\d+)\s*개년")
ENTITY_NAME_RE = re.compile(
    r"([A-Z][A-Za-z0-9&.,\-]*"
    r"(?:\s+[A-Z][A-Za-z0-9&.,\-]*)*"
    r"\s+(?:Inc\.|Ltd\.|LLC\.?|Corp\.|GmbH|ApS|Kft|OOO|SAS|Co\., Ltd\.|Co\. Ltd\.|Co\.)"
    r"\s*(?:\([A-Z0-9]{2,8}\))?)"
)
EXPLANATION_KEYWORDS = ("설명", "의견", "이유", "배경", "근거", "내용", "주석", "관련")
TREND_KEYWORDS = ("추이", "흐름", "비교", "변화", "증가", "감소", "최근")


class QueryInterpreter:
    def __init__(self, routing_policy: Optional[QueryRoutingPolicy] = None) -> None:
        self.routing_policy = routing_policy or QueryRoutingPolicy()

    def build_llm_instruction(self) -> str:
        return (
            "사용자 질문을 구조화된 질의 해석 JSON으로 변환하세요. "
            "intent는 metric_lookup, text_explanation, metric_with_explanation, trend_compare 중 하나만 허용됩니다. "
            "SQL을 직접 만들지 말고, metric_candidates / row_label_filters / year / section_candidates / "
            "need_sql / need_vdb / clarification_needed 만 채우세요. "
            "RDB는 숫자/연도/추이 조회, VDB는 설명 문단/감사의견/주석 조회에 사용됩니다. "
            "질문이 너무 추상적이면 clarification_needed=true 와 clarification_reason을 채우세요.\n"
            f"{json.dumps(INTERPRETATION_JSON_SCHEMA, ensure_ascii=False, indent=2)}"
        )

    def parse_llm_output(self, raw_question: str, payload: str | Dict[str, Any]) -> QueryInterpretation:
        if isinstance(payload, str):
            payload = json.loads(payload)
        interpretation = QueryInterpretation.from_dict(payload, raw_question=raw_question)
        interpretation.metric_candidates = self._canonicalize_metrics(interpretation.metric_candidates)
        interpretation.row_label_filters = self._canonicalize_row_label_filters(interpretation.row_label_filters)
        interpretation.section_candidates = self._canonicalize_sections(interpretation.section_candidates)
        return interpretation

    def interpret(
        self,
        question: str,
        llm_output: Optional[str | Dict[str, Any]] = None,
    ) -> QueryInterpretation:
        fallback = self._interpret_with_rules(question)
        if llm_output is None:
            return self.routing_policy.apply(fallback)

        structured = self.parse_llm_output(question, llm_output)
        return self.routing_policy.apply(self._merge(fallback, structured))

    def _merge(
        self,
        fallback: QueryInterpretation,
        structured: QueryInterpretation,
    ) -> QueryInterpretation:
        return QueryInterpretation(
            raw_question=structured.raw_question or fallback.raw_question,
            intent=structured.intent,
            metric_candidates=structured.metric_candidates or fallback.metric_candidates,
            row_label_filters=structured.row_label_filters or fallback.row_label_filters,
            year=structured.year if structured.year is not None else fallback.year,
            year_range=structured.year_range or fallback.year_range,
            year_window=structured.year_window if structured.year_window is not None else fallback.year_window,
            section_candidates=structured.section_candidates or fallback.section_candidates,
            need_sql=structured.need_sql if structured.need_sql is not None else fallback.need_sql,
            need_vdb=structured.need_vdb if structured.need_vdb is not None else fallback.need_vdb,
            comparison_mode=structured.comparison_mode or fallback.comparison_mode,
            limit=structured.limit or fallback.limit,
            confidence=structured.confidence if structured.confidence is not None else fallback.confidence,
            clarification_needed=structured.clarification_needed or fallback.clarification_needed,
            clarification_reason=structured.clarification_reason or fallback.clarification_reason,
            notes=fallback.notes + structured.notes,
        )

    def _interpret_with_rules(self, question: str) -> QueryInterpretation:
        compact_question = compact_token(question)
        metrics = self._detect_metrics(compact_question)
        row_label_filters = self._detect_row_label_filters(question)
        sections = self._detect_sections(compact_question)
        years = [int(match.group(1)) for match in YEAR_RE.finditer(question)]
        year = years[0] if len(years) == 1 else None
        year_range = (min(years), max(years)) if len(years) >= 2 else None
        year_window = self._extract_year_window(question)
        comparison_mode = "year_over_year" if "전년" in compact_question or "비교" in compact_question else None
        asks_explanation = any(keyword in compact_question for keyword in map(compact_token, EXPLANATION_KEYWORDS))
        asks_trend = any(keyword in compact_question for keyword in map(compact_token, TREND_KEYWORDS)) and (
            year_window is not None or year_range is not None or "추이" in question or "비교" in question
        )

        if asks_trend and metrics:
            intent = QueryIntent.TREND_COMPARE
            need_sql = True
            need_vdb = asks_explanation
        elif metrics and asks_explanation:
            intent = QueryIntent.METRIC_WITH_EXPLANATION
            need_sql = True
            need_vdb = True
        elif sections or asks_explanation:
            intent = QueryIntent.TEXT_EXPLANATION
            need_sql = False
            need_vdb = True
        else:
            intent = QueryIntent.METRIC_LOOKUP
            need_sql = bool(metrics)
            need_vdb = False

        if intent in {QueryIntent.METRIC_WITH_EXPLANATION, QueryIntent.TEXT_EXPLANATION} and not sections:
            sections = list(DEFAULT_EXPLANATION_SECTIONS)

        notes: List[str] = []
        if not metrics and intent in {
            QueryIntent.METRIC_LOOKUP,
            QueryIntent.METRIC_WITH_EXPLANATION,
            QueryIntent.TREND_COMPARE,
        }:
            notes.append("metric_candidates_empty")
        if row_label_filters:
            notes.append("row_label_filter_detected")

        return QueryInterpretation(
            raw_question=question,
            intent=intent,
            metric_candidates=metrics,
            row_label_filters=row_label_filters,
            year=year,
            year_range=year_range,
            year_window=year_window,
            section_candidates=sections,
            need_sql=need_sql,
            need_vdb=need_vdb,
            comparison_mode=comparison_mode,
            limit=10,
            notes=notes,
        )

    @staticmethod
    def _extract_year_window(question: str) -> Optional[int]:
        match = YEAR_WINDOW_RE.search(question)
        return int(match.group(1)) if match else None

    def _detect_metrics(self, compact_question: str) -> List[str]:
        matches: List[str] = []
        for alias, metric_id in sorted(METRIC_ALIAS_TO_ID.items(), key=lambda item: len(item[0]), reverse=True):
            if alias and alias in compact_question and metric_id not in matches:
                matches.append(metric_id)
        return matches

    def _detect_sections(self, compact_question: str) -> List[str]:
        matches: List[str] = []
        for alias, group in sorted(SECTION_ALIAS_TO_GROUP.items(), key=lambda item: len(item[0]), reverse=True):
            if alias and alias in compact_question:
                for section_type in SECTION_GROUPS[group]:
                    if section_type not in matches:
                        matches.append(section_type)
        return matches

    @staticmethod
    def _detect_row_label_filters(question: str) -> List[str]:
        matches: List[str] = []

        for match in re.finditer(r"[\"']([^\"']{3,120})[\"']", question):
            candidate = match.group(1).strip()
            if candidate and candidate not in matches:
                matches.append(candidate)

        for match in ENTITY_NAME_RE.finditer(question):
            candidate = re.sub(r"\s+", " ", match.group(1)).strip(" ,")
            if candidate and candidate not in matches:
                matches.append(candidate)

        return matches

    def _canonicalize_metrics(self, metrics: Iterable[str]) -> List[str]:
        result: List[str] = []
        for metric in metrics:
            canonical = METRIC_ALIAS_TO_ID.get(compact_token(metric), metric)
            if canonical in METRIC_DEFINITIONS and canonical not in result:
                result.append(canonical)
        return result

    @staticmethod
    def _canonicalize_row_label_filters(filters: Iterable[str]) -> List[str]:
        result: List[str] = []
        for value in filters:
            normalized = re.sub(r"\s+", " ", (value or "")).strip()
            if normalized and normalized not in result:
                result.append(normalized)
        return result

    def _canonicalize_sections(self, sections: Iterable[str]) -> List[str]:
        result: List[str] = []
        for section in sections:
            compact = compact_token(section)
            group = SECTION_ALIAS_TO_GROUP.get(compact)
            if group:
                for candidate in SECTION_GROUPS[group]:
                    if candidate not in result:
                        result.append(candidate)
                continue
            if section in SECTION_GROUPS:
                for candidate in SECTION_GROUPS[section]:
                    if candidate not in result:
                        result.append(candidate)
                continue
            if section not in result:
                result.append(section)
        return result
