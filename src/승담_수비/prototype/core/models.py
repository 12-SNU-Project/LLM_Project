from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DocumentMeta:
    """감사보고서 문서 전체 메타데이터."""

    filing_id: str
    company_name: str
    fiscal_year: Optional[int] = None
    report_type: str = "감사보고서"
    auditor_name: Optional[str] = None
    auditor_report_date: Optional[str] = None
    source_file: Optional[str] = None
    source_encoding: Optional[str] = None
    parser_backend: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Block:
    """DOM에서 분해된 기본 블록 단위."""

    block_id: str
    block_type: str  # cover | section_heading | paragraph | table | footnote | page_break
    text: str
    html_fragment: str
    dom_path: str
    order_index: int
    prev_block_id: Optional[str] = None
    next_block_id: Optional[str] = None
    section_id: Optional[str] = None
    section_type: Optional[str] = None
    section_title: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Section:
    """블록 묶음으로 구성되는 섹션 단위."""
    
    section_id: str
    filing_id: str
    section_type: str
    section_title: str
    start_block_id: str
    end_block_id: Optional[str] = None
    order_index: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    parent_section_id: Optional[str] = None
    section_level: int = 0

@dataclass
class TableCell:
    """정규화된 논리 grid의 셀."""

    cell_id: str
    row_index: int
    col_index: int
    text: str
    rowspan: int
    colspan: int
    is_header: bool
    source_html: str
    header_path: List[str] = field(default_factory=list)
    row_id: Optional[str] = None
    row_depth: int = 0
    parent_row_id: Optional[str] = None
    is_section_header: bool = False

    @property
    def column_header_path(self) -> str:
        return " > ".join(part for part in self.header_path if part)


@dataclass
class TableRow:
    """RDB 적재 기준의 표 행 객체."""

    row_id: str
    table_id: str
    row_index: int
    raw_label: str
    normalized_label: str
    row_depth: int
    parent_row_id: Optional[str]
    is_section_header: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableValue:
    """RDB 적재 기준의 표 값 객체."""

    value_id: str
    table_id: str
    row_id: str
    col_index: int
    period: Optional[str]
    value_raw: str
    value_numeric: Optional[float]
    unit: Optional[str]
    column_header_path: str
    is_primary_value: bool = False
    note_reference_candidates: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedTable:
    """rowspan/colspan 해체 후의 intermediate table object."""

    table_id: str
    filing_id: str
    source_block_id: str
    statement_type: Optional[str] = None
    table_role: str = "unknown_table"  # cover_table | financial_table | internal_control_table | unknown_table
    title: Optional[str] = None
    unit: Optional[str] = None
    year_candidates: List[int] = field(default_factory=list)
    context_before: Optional[str] = None
    context_after: Optional[str] = None
    section_id: Optional[str] = None
    section_type: Optional[str] = None
    section_title: Optional[str] = None
    html_fragment: Optional[str] = None
    cells: List[TableCell] = field(default_factory=list)
    rows: List[TableRow] = field(default_factory=list)
    values: List[TableValue] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TextChunk:
    """VDB 적재용 텍스트 청크."""

    chunk_id: str
    filing_id: str
    fiscal_year: Optional[int]
    section_type: Optional[str]
    section_title: Optional[str]
    auditor_name: Optional[str]
    near_table_id: Optional[str]
    topic_hint: Optional[str]
    text: str
    start_block_id: str
    end_block_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FilingParseResult:
    """단일 보고서 파싱 결과 전체."""

    meta: DocumentMeta
    blocks: List[Block]
    sections: List[Section]
    tables: List[NormalizedTable]
    text_chunks: List[TextChunk]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# parser.py 상단 근처

CONSERVATIVE_RULES = [
    # 상위 섹션
    {
        "pattern": r"독립된감사인의감사보고서",
        "section_type": "independent_auditor_report",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"\(첨부\)재무제표",
        "section_type": "attached_financial_statements",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"^주석$",
        "section_type": "notes",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"내부회계관리제도(감사또는검토의견|검토의견)",
        "section_type": "internal_control_opinion",
        "level": "top",
        "priority": 60,
        "parent_types": None,
    },
    {
        "pattern": r"^외부감사실시내용$",
        "section_type": "external_audit_activity",
        "level": "top",
        "priority": 60,
        "parent_types": None,
    },

    # 보수적 모드에서도 확실한 하위 섹션은 허용
    {
        "pattern": r"^\d+(\.\d+)*보고기간후사건$",
        "section_type": "subsequent_events",
        "level": "sub",
        "priority": 90,
        "parent_types": {"notes", "attached_financial_statements", "cover", "other"},
    },
    {
        "pattern": r"^\d+\.\s*감사대상업무$",
        "section_type": "external_audit_target_work",
        "level": "sub",
        "priority": 95,
        "parent_types": {"external_audit_activity"},
    },
    {
        "pattern": r"^\d+\.\s*감사참여자구분별인원수및감사시간$",
        "section_type": "external_audit_hours",
        "level": "sub",
        "priority": 95,
        "parent_types": {"external_audit_activity"},
    },
]

AGGRESSIVE_RULES = CONSERVATIVE_RULES + [
    {
        "pattern": r"^감사의견$",
        "section_type": "audit_opinion",
        "level": "sub",
        "priority": 100,
        "parent_types": {"independent_auditor_report"},
    },
    {
        "pattern": r"^핵심감사사항$",
        "section_type": "key_audit_matters",
        "level": "sub",
        "priority": 100,
        "parent_types": {"independent_auditor_report"},
    },
    {
        "pattern": r"내부회계관리제도에대한감사의견",
        "section_type": "internal_control_audit_opinion",
        "level": "sub",
        "priority": 120,
        "parent_types": {"internal_control_opinion"},
    },
    {
        "pattern": r"내부회계관리제도감사의견근거",
        "section_type": "internal_control_audit_basis",
        "level": "sub",
        "priority": 120,
        "parent_types": {"internal_control_opinion"},
    },
]