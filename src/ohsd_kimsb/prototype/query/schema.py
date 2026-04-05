from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class QueryIntent(str, Enum):
    METRIC_LOOKUP = "metric_lookup"
    TEXT_EXPLANATION = "text_explanation"
    METRIC_WITH_EXPLANATION = "metric_with_explanation"
    TREND_COMPARE = "trend_compare"
    TABLE_CELL_LOOKUP = "table_cell_lookup"
    COMPARISON_LIST_LOOKUP = "comparison_list_lookup"


@dataclass
class QueryInterpretation:
    """Structured query intent plus app-side routing decisions."""

    raw_question: str
    intent: QueryIntent
    metric_candidates: List[str] = field(default_factory=list)
    row_label_filters: List[str] = field(default_factory=list)
    row_label_terms: List[str] = field(default_factory=list)
    column_terms: List[str] = field(default_factory=list)
    table_title_terms: List[str] = field(default_factory=list)
    year: Optional[int] = None
    year_range: Optional[Tuple[int, int]] = None
    year_window: Optional[int] = None
    period: Optional[str] = None
    comparison_operator: Optional[str] = None
    threshold_value: Optional[float] = None
    entity_scope: Optional[str] = None
    section_candidates: List[str] = field(default_factory=list)
    need_sql: bool = False
    need_vdb: bool = False
    comparison_mode: Optional[str] = None
    limit: int = 10
    confidence: Optional[float] = None
    clarification_needed: bool = False
    clarification_reason: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["intent"] = self.intent.value
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any], raw_question: str = "") -> "QueryInterpretation":
        year_range = payload.get("year_range")
        if year_range:
            year_range = (int(year_range[0]), int(year_range[1]))
        return cls(
            raw_question=raw_question or str(payload.get("raw_question", "")),
            intent=QueryIntent(str(payload["intent"])),
            metric_candidates=[str(item) for item in payload.get("metric_candidates", [])],
            row_label_filters=[str(item) for item in payload.get("row_label_filters", [])],
            row_label_terms=[str(item) for item in payload.get("row_label_terms", [])],
            column_terms=[str(item) for item in payload.get("column_terms", [])],
            table_title_terms=[str(item) for item in payload.get("table_title_terms", [])],
            year=int(payload["year"]) if payload.get("year") is not None else None,
            year_range=year_range,
            year_window=int(payload["year_window"]) if payload.get("year_window") is not None else None,
            period=payload.get("period"),
            comparison_operator=payload.get("comparison_operator"),
            threshold_value=float(payload["threshold_value"]) if payload.get("threshold_value") is not None else None,
            entity_scope=payload.get("entity_scope"),
            section_candidates=[str(item) for item in payload.get("section_candidates", [])],
            need_sql=bool(payload.get("need_sql", False)),
            need_vdb=bool(payload.get("need_vdb", False)),
            comparison_mode=payload.get("comparison_mode"),
            limit=int(payload.get("limit", 10)),
            confidence=float(payload["confidence"]) if payload.get("confidence") is not None else None,
            clarification_needed=bool(payload.get("clarification_needed", False)),
            clarification_reason=payload.get("clarification_reason"),
            notes=[str(item) for item in payload.get("notes", [])],
        )


@dataclass
class SQLQueryPlan:
    template_name: str
    sql: str
    params: List[Any]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


INTERPRETATION_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intent": {
            "type": "string",
            "enum": [intent.value for intent in QueryIntent],
        },
        "metric_candidates": {
            "type": "array",
            "items": {"type": "string"},
        },
        "row_label_filters": {
            "type": "array",
            "items": {"type": "string"},
        },
        "row_label_terms": {
            "type": "array",
            "items": {"type": "string"},
        },
        "column_terms": {
            "type": "array",
            "items": {"type": "string"},
        },
        "table_title_terms": {
            "type": "array",
            "items": {"type": "string"},
        },
        "year": {
            "type": ["integer", "null"],
        },
        "year_range": {
            "type": ["array", "null"],
            "items": {"type": "integer"},
            "minItems": 2,
            "maxItems": 2,
        },
        "year_window": {
            "type": ["integer", "null"],
        },
        "period": {
            "type": ["string", "null"],
        },
        "comparison_operator": {
            "type": ["string", "null"],
        },
        "threshold_value": {
            "type": ["number", "null"],
        },
        "entity_scope": {
            "type": ["string", "null"],
        },
        "section_candidates": {
            "type": "array",
            "items": {"type": "string"},
        },
        "need_sql": {"type": "boolean"},
        "need_vdb": {"type": "boolean"},
        "comparison_mode": {"type": ["string", "null"]},
        "limit": {"type": "integer"},
        "confidence": {"type": ["number", "null"]},
        "clarification_needed": {"type": "boolean"},
        "clarification_reason": {"type": ["string", "null"]},
        "notes": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["intent", "metric_candidates", "need_sql", "need_vdb"],
}
