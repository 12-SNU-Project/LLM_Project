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
EXPLANATION_KEYWORDS = ("설명", "의견", "원인", "배경", "근거", "내용", "주석", "관련")
TREND_KEYWORDS = ("추이", "흐름", "비교", "변화", "증가", "감소", "최근")


class QueryInterpreter:
    def __init__(self, routing_policy: Optional[QueryRoutingPolicy] = None) -> None:
        self.routing_policy = routing_policy or QueryRoutingPolicy()

    def build_llm_instruction(self) -> str:
        return (
            "사용자 질문을 구조화된 질의 해석 JSON으로 변환하라. "
            "지원 intent는 metric_lookup, text_explanation, metric_with_explanation, trend_compare 네 가지뿐이다. "
            "SQL은 직접 생성하지 말고, 아래 JSON schema에 맞춰 intent와 슬롯만 채워라. "
            "RDB는 수치/연도/추이 조회에 쓰고, VDB는 설명 문단/감사의견/주석 설명에 쓴다. "
            "질문이 너무 추상적이거나 metric이 없으면 clarification_needed=true와 clarification_reason을 채워라.\n"
            f"{json.dumps(INTERPRETATION_JSON_SCHEMA, ensure_ascii=False, indent=2)}"
        )

    def parse_llm_output(self, raw_question: str, payload: str | Dict[str, Any]) -> QueryInterpretation:
        if isinstance(payload, str):
            payload = json.loads(payload)
        interpretation = QueryInterpretation.from_dict(payload, raw_question=raw_question)
        interpretation.metric_candidates = self._canonicalize_metrics(interpretation.metric_candidates)
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
        if not metrics and intent in {QueryIntent.METRIC_LOOKUP, QueryIntent.METRIC_WITH_EXPLANATION, QueryIntent.TREND_COMPARE}:
            notes.append("metric_candidates_empty")

        return QueryInterpretation(
            raw_question=question,
            intent=intent,
            metric_candidates=metrics,
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

    def _canonicalize_metrics(self, metrics: Iterable[str]) -> List[str]:
        result: List[str] = []
        for metric in metrics:
            canonical = METRIC_ALIAS_TO_ID.get(compact_token(metric), metric)
            if canonical in METRIC_DEFINITIONS and canonical not in result:
                result.append(canonical)
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
