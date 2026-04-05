from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Sequence


COMPACT_RE = re.compile(r"[^0-9A-Za-z가-힣]+")


def compact_token(value: str) -> str:
    return COMPACT_RE.sub("", value or "").lower()


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    aliases: Sequence[str]
    row_label_aliases: Sequence[str]
    statement_types: Sequence[str]
    topic_tags: Sequence[str]


def _metric(
    metric_id: str,
    aliases: Sequence[str],
    row_label_aliases: Sequence[str],
    statement_types: Sequence[str],
    topic_tags: Sequence[str] | None = None,
) -> MetricDefinition:
    return MetricDefinition(
        metric_id=metric_id,
        aliases=tuple(aliases),
        row_label_aliases=tuple(row_label_aliases),
        statement_types=tuple(statement_types),
        topic_tags=tuple(topic_tags or (metric_id,)),
    )


METRIC_DEFINITIONS: Dict[str, MetricDefinition] = {
    "revenue": _metric(
        "revenue",
        aliases=("매출", "매출액", "영업수익", "revenue", "sales"),
        row_label_aliases=("매출액", "매출", "영업수익", "수익"),
        statement_types=("income_statement", "statement_of_comprehensive_income"),
    ),
    "cost_of_sales": _metric(
        "cost_of_sales",
        aliases=("매출원가", "cost of sales", "cost_of_sales"),
        row_label_aliases=("매출원가",),
        statement_types=("income_statement", "statement_of_comprehensive_income"),
    ),
    "gross_profit": _metric(
        "gross_profit",
        aliases=("매출총이익", "gross profit", "gross_profit"),
        row_label_aliases=("매출총이익",),
        statement_types=("income_statement", "statement_of_comprehensive_income"),
    ),
    "operating_income": _metric(
        "operating_income",
        aliases=("영업이익", "영업손실", "operating income", "operating profit"),
        row_label_aliases=("영업이익(손실)", "영업이익", "영업손실"),
        statement_types=("income_statement", "statement_of_comprehensive_income"),
    ),
    "net_income": _metric(
        "net_income",
        aliases=("당기순이익", "순이익", "net income", "profit"),
        row_label_aliases=("당기순이익", "당기순이익(손실)", "순이익"),
        statement_types=("income_statement", "statement_of_comprehensive_income"),
    ),
    "total_assets": _metric(
        "total_assets",
        aliases=("자산총계", "총자산", "total assets"),
        row_label_aliases=("자산총계", "자산총액", "총자산"),
        statement_types=("statement_of_financial_position",),
    ),
    "total_liabilities": _metric(
        "total_liabilities",
        aliases=("부채총계", "총부채", "total liabilities"),
        row_label_aliases=("부채총계", "총부채"),
        statement_types=("statement_of_financial_position",),
    ),
    "total_equity": _metric(
        "total_equity",
        aliases=("자본총계", "총자본", "equity", "total equity"),
        row_label_aliases=("자본총계", "총자본"),
        statement_types=("statement_of_financial_position",),
    ),
    "cash_and_cash_equivalents": _metric(
        "cash_and_cash_equivalents",
        aliases=("현금", "현금성자산", "현금및현금성자산", "cash"),
        row_label_aliases=("현금및현금성자산", "현금성자산"),
        statement_types=("statement_of_financial_position", "cash_flow_statement"),
        topic_tags=("cash", "liquidity"),
    ),
    "accounts_receivable": _metric(
        "accounts_receivable",
        aliases=("매출채권", "외상매출금", "accounts receivable"),
        row_label_aliases=("매출채권", "외상매출금"),
        statement_types=("statement_of_financial_position",),
        topic_tags=("receivable",),
    ),
    "inventory": _metric(
        "inventory",
        aliases=("재고", "재고자산", "inventory"),
        row_label_aliases=("재고자산", "재고"),
        statement_types=("statement_of_financial_position",),
    ),
}


METRIC_ALIAS_TO_ID: Dict[str, str] = {}
for metric_id, definition in METRIC_DEFINITIONS.items():
    for alias in (metric_id, *definition.aliases):
        METRIC_ALIAS_TO_ID[compact_token(alias)] = metric_id


SECTION_GROUPS: Dict[str, List[str]] = {
    "audit_opinion": ["audit_opinion", "audit_opinion_basis"],
    "key_audit_matters": ["key_audit_matters"],
    "internal_control": [
        "internal_control_opinion",
        "internal_control_audit_report",
        "internal_control_audit_opinion",
        "internal_control_audit_basis",
    ],
    "contingent_liabilities_and_commitments": ["contingent_liabilities_and_commitments"],
    "subsequent_events": ["subsequent_events"],
    "notes": ["notes", "note_section"],
}


SECTION_ALIAS_TO_GROUP: Dict[str, str] = {
    compact_token("감사의견"): "audit_opinion",
    compact_token("핵심감사사항"): "key_audit_matters",
    compact_token("내부회계관리제도"): "internal_control",
    compact_token("내부통제"): "internal_control",
    compact_token("우발부채"): "contingent_liabilities_and_commitments",
    compact_token("약정사항"): "contingent_liabilities_and_commitments",
    compact_token("보고기간후사건"): "subsequent_events",
    compact_token("주석"): "notes",
    compact_token("회계정책"): "notes",
    compact_token("회계처리방침"): "notes",
}


DEFAULT_EXPLANATION_SECTIONS = ["notes", "note_section"]
