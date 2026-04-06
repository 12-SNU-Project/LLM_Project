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
    r"((?:[A-Z][A-Za-z0-9&.,\-]*|\([A-Za-z0-9&.,\-]+\))"
    r"(?:\s+(?:[A-Z][A-Za-z0-9&.,\-]*|\([A-Za-z0-9&.,\-]+\)))*"
    r"\s+(?:Inc\.?|Ltd\.?|LLC\.?|Corp\.?|GmbH|ApS|Kft|OOO|SAS|Co\.,?\s*Ltd\.?|Co\.?\s*Ltd\.?|Co\.?)"
    r"\s*(?:\([A-Z0-9]{2,8}\))?)"
)
EXPLANATION_KEYWORDS = ("설명", "근거", "이유", "배경", "무엇", "의견", "주석")
TREND_KEYWORDS = ("추이", "흐름", "비교", "변화", "증가", "감소", "최근")
STRUCTURE_KEYWORDS = ("어떤 종류", "무슨 종류", "포함", "구성", "항목", "종류", "무엇이")
LIST_KEYWORDS = ("리스트업", "목록", "나열", "정리", "리스트")
PERIOD_PATTERNS = (
    ("당기말", ("당기말",)),
    ("전기말", ("전기말",)),
    ("당기초", ("당기초",)),
    ("전기초", ("전기초",)),
    ("당기", ("당기",)),
    ("전기", ("전기",)),
)
ROW_LABEL_CANDIDATES = (
    "기초장부가액",
    "기말장부가액",
    "취득",
    "처분",
    "상각",
    "손상",
    "감가상각",
    "할인율",
    "미래임금상승률",
    "채권 등",
    "채무 등",
)
ROW_LABEL_ALIASES = {
    # 사용자는 표의 원문 표현보다 짧게 묻는 경우가 많아서 자주 쓰는 축약어만 별도로 맞춰준다.
    "임금상승률": "미래임금상승률",
    "미래임금상승률": "미래임금상승률",
}
COLUMN_ALIASES = {
    "채권": "채권 등",
    "채무": "채무 등",
    "매출": "매출 등",
    "매입": "매입 등",
    "지분": "지분율",
}
COLUMN_CANDIDATES = (
    "지분율",
    "개발비",
    "산업재산권",
    "영업권",
    "회원권",
    "기타무형자산",
    "매출액",
    "매출 등",
    "매입 등",
    "채권 등",
    "채무 등",
)
TABLE_TITLE_CANDIDATES = (
    "무형자산",
    "유형자산",
    "보험수리적 가정",
    "순확정급여부채",
    "특수관계자",
    "종속기업",
    "관계기업",
    "공동기업",
    "요약 재무정보",
)
COMPARISON_PATTERNS = (
    ("lt", ("미만", "보다 적은", "보다 작은", "작은")),
    ("lte", ("이하", "이하인")),
    ("gt", ("초과", "보다 큰", "보다 많은", "큰")),
    ("gte", ("이상", "이상인", "넘는")),
)
THRESHOLD_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%?")
ENTITY_SCOPE_MAP = {
    "종속기업": "subsidiary",
    "관계기업": "associate",
    "공동기업": "joint_venture",
}


class QueryInterpreter:
    def __init__(self, routing_policy: Optional[QueryRoutingPolicy] = None) -> None:
        self.routing_policy = routing_policy or QueryRoutingPolicy()

    def build_llm_instruction(self) -> str:
        return (
            "사용자 질문을 구조화된 질의 해석 JSON으로 변환하세요. "
            "intent는 metric_lookup, text_explanation, metric_with_explanation, trend_compare, "
            "table_cell_lookup, comparison_list_lookup 중 하나만 사용합니다. "
            "SQL은 직접 만들지 말고 metric_candidates / row_label_filters / row_label_terms / "
            "column_terms / table_title_terms / year / period / comparison_operator / "
            "threshold_value / entity_scope / section_candidates / need_sql / need_vdb / "
            "clarification_needed 만 채우세요. "
            "RDB는 숫자/연도/표 셀 조회, VDB는 설명 문단/감사의견/주석 조회에 사용합니다. "
            "질문이 너무 추상적이면 clarification_needed=true 와 clarification_reason을 채우세요.\n"
            f"{json.dumps(INTERPRETATION_JSON_SCHEMA, ensure_ascii=False, indent=2)}"
        )

    def parse_llm_output(self, raw_question: str, payload: str | Dict[str, Any]) -> QueryInterpretation:
        if isinstance(payload, str):
            payload = json.loads(payload)
        interpretation = QueryInterpretation.from_dict(payload, raw_question=raw_question)
        interpretation.metric_candidates = self._canonicalize_metrics(interpretation.metric_candidates)
        interpretation.row_label_filters = self._canonicalize_row_label_filters(interpretation.row_label_filters)
        interpretation.row_label_terms = self._canonicalize_terms(interpretation.row_label_terms)
        interpretation.column_terms = self._canonicalize_terms(interpretation.column_terms)
        interpretation.table_title_terms = self._canonicalize_terms(interpretation.table_title_terms)
        interpretation.section_candidates = self._canonicalize_sections(interpretation.section_candidates)
        interpretation.entity_scope = self._canonicalize_entity_scope(interpretation.entity_scope)
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
        def _merge_unique(*groups: Iterable[str]) -> List[str]:
            merged: List[str] = []
            for group in groups:
                for item in group:
                    if item and item not in merged:
                        merged.append(item)
            return merged

        return QueryInterpretation(
            raw_question=structured.raw_question or fallback.raw_question,
            intent=structured.intent,
            metric_candidates=_merge_unique(fallback.metric_candidates, structured.metric_candidates),
            row_label_filters=_merge_unique(fallback.row_label_filters, structured.row_label_filters),
            row_label_terms=_merge_unique(fallback.row_label_terms, structured.row_label_terms),
            column_terms=_merge_unique(fallback.column_terms, structured.column_terms),
            table_title_terms=_merge_unique(fallback.table_title_terms, structured.table_title_terms),
            year=structured.year if structured.year is not None else fallback.year,
            year_range=structured.year_range or fallback.year_range,
            year_window=structured.year_window if structured.year_window is not None else fallback.year_window,
            period=structured.period or fallback.period,
            comparison_operator=structured.comparison_operator or fallback.comparison_operator,
            threshold_value=(
                structured.threshold_value if structured.threshold_value is not None else fallback.threshold_value
            ),
            entity_scope=structured.entity_scope or fallback.entity_scope,
            section_candidates=_merge_unique(fallback.section_candidates, structured.section_candidates),
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
        row_label_terms = self._detect_keyword_terms(question, ROW_LABEL_CANDIDATES)
        row_label_terms = self._expand_row_label_aliases(question, row_label_terms)
        column_terms = self._detect_keyword_terms(question, COLUMN_CANDIDATES)
        column_terms = self._expand_column_aliases(question, column_terms)
        table_title_terms = self._detect_keyword_terms(question, TABLE_TITLE_CANDIDATES)
        sections = self._detect_sections(compact_question)
        years = [int(match.group(1)) for match in YEAR_RE.finditer(question)]
        year = years[0] if len(years) == 1 else None
        year_range = (min(years), max(years)) if len(years) >= 2 else None
        year_window = self._extract_year_window(question)
        period = self._detect_period(question)
        comparison_mode = "year_over_year" if "전년" in question or "비교" in question else None
        comparison_operator = self._detect_comparison_operator(question)
        threshold_value = self._detect_threshold_value(question, years)
        entity_scope = self._detect_entity_scope(question)
        asks_explanation = any(keyword in question for keyword in EXPLANATION_KEYWORDS)
        asks_trend = any(keyword in question for keyword in TREND_KEYWORDS) and (
            year_window is not None or year_range is not None or "추이" in question or "비교" in question
        )
        asks_structure = any(keyword in question for keyword in STRUCTURE_KEYWORDS)
        asks_list = any(keyword in question for keyword in LIST_KEYWORDS)
        has_table_cell_anchor = bool(
            row_label_filters or row_label_terms or column_terms or table_title_terms
        )  # 회사명/행/열/표 제목 축이 보이면 셀 질의로 본다.
        has_comparison_anchor = bool(
            comparison_operator and threshold_value is not None and (column_terms or table_title_terms or entity_scope)
        )

        if entity_scope:
            scope_term = {
                "subsidiary": "종속기업",
                "associate": "관계기업",
                "joint_venture": "공동기업",
            }.get(entity_scope)
            if scope_term and scope_term not in table_title_terms:
                table_title_terms.append(scope_term)

        if has_comparison_anchor and (asks_list or entity_scope):
            intent = QueryIntent.COMPARISON_LIST_LOOKUP
            need_sql = True
            need_vdb = False
        elif asks_structure and table_title_terms:
            intent = QueryIntent.TEXT_EXPLANATION
            need_sql = True   # 표 구조 설명은 RDB의 행/열 축도 함께 본다.
            need_vdb = True
        elif has_table_cell_anchor and not metrics:
            intent = QueryIntent.TABLE_CELL_LOOKUP
            need_sql = True
            need_vdb = False
        elif asks_trend and metrics:
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
        if has_table_cell_anchor:
            notes.append("table_cell_anchor_detected")
        if asks_structure and table_title_terms:
            notes.append("table_structure_lookup")
        if has_comparison_anchor:
            notes.append("comparison_list_lookup")

        return QueryInterpretation(
            raw_question=question,
            intent=intent,
            metric_candidates=metrics,
            row_label_filters=row_label_filters,
            row_label_terms=row_label_terms,
            column_terms=column_terms,
            table_title_terms=table_title_terms,
            year=year,
            year_range=year_range,
            year_window=year_window,
            period=period,
            comparison_operator=comparison_operator,
            threshold_value=threshold_value,
            entity_scope=entity_scope,
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
        if match:
            return int(match.group(1))
        for pattern in (r"(?:지난|최근)\s*(\d+)\s*년(?:간)?", r"(\d+)\s*개년"):
            extra_match = re.search(pattern, question)
            if extra_match:
                return int(extra_match.group(1))
        return None

    @staticmethod
    def _detect_keyword_terms(question: str, candidates: Iterable[str]) -> List[str]:
        matches: List[str] = []
        for candidate in candidates:
            if candidate in question and candidate not in matches:
                matches.append(candidate)
        return matches

    @staticmethod
    def _expand_row_label_aliases(question: str, detected_terms: List[str]) -> List[str]:
        matches = list(detected_terms)
        for alias, canonical in ROW_LABEL_ALIASES.items():
            if alias in question and canonical not in matches:
                matches.append(canonical)
        return matches

    @staticmethod
    def _expand_column_aliases(question: str, detected_terms: List[str]) -> List[str]:
        matches = list(detected_terms)
        for alias, canonical in COLUMN_ALIASES.items():
            if alias in question and canonical not in matches:
                matches.append(canonical)
        return matches

    @staticmethod
    def _detect_period(question: str) -> Optional[str]:
        for canonical, variants in PERIOD_PATTERNS:
            if any(variant in question for variant in variants):
                return canonical
        return None

    @staticmethod
    def _detect_comparison_operator(question: str) -> Optional[str]:
        for canonical, variants in COMPARISON_PATTERNS:
            if any(variant in question for variant in variants):
                return canonical
        return None

    @staticmethod
    def _detect_threshold_value(question: str, year_values: Iterable[int]) -> Optional[float]:
        year_set = {int(year) for year in year_values}
        candidates: List[float] = []
        for match in THRESHOLD_RE.finditer(question):
            token = match.group(1)
            try:
                numeric = float(token)
            except ValueError:
                continue
            if int(numeric) in year_set and numeric >= 1900:
                continue  # 연도 값은 비교 임계값 후보에서 제외한다.
            candidates.append(numeric)
        return candidates[0] if candidates else None

    @staticmethod
    def _detect_entity_scope(question: str) -> Optional[str]:
        for keyword, entity_scope in ENTITY_SCOPE_MAP.items():
            if keyword in question:
                return entity_scope
        return None

    def _detect_metrics(self, compact_question: str) -> List[str]:
        matches: List[str] = []
        occupied_spans: List[tuple[int, int]] = []
        for alias, metric_id in sorted(METRIC_ALIAS_TO_ID.items(), key=lambda item: len(item[0]), reverse=True):
            if not alias or metric_id in matches:
                continue
            start = compact_question.find(alias)
            while start >= 0:
                span = (start, start + len(alias))
                if not any(start_idx <= span[0] and span[1] <= end_idx for start_idx, end_idx in occupied_spans):
                    matches.append(metric_id)
                    occupied_spans.append(span)
                    break
                start = compact_question.find(alias, start + 1)
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

    @staticmethod
    def _canonicalize_terms(values: Iterable[str]) -> List[str]:
        result: List[str] = []
        for value in values:
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

    @staticmethod
    def _canonicalize_entity_scope(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        normalized = str(value).strip()
        return normalized or None
