from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Block:
    block_id: str
    block_type: str
    tag_name: str
    section_id: str | None
    section_type: str | None
    section_title: str | None
    text: str
    html_fragment: str
    dom_path: str
    source_order: int
    prev_block_id: str | None = None
    next_block_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Cell:
    cell_id: str
    row_index: int
    col_index: int
    text: str
    rowspan: int
    colspan: int
    is_header: bool
    source_html: str
    header_path: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RowObject:
    row_id: str
    raw_label: str
    normalized_label: str
    row_depth: int
    parent_row_id: str | None
    is_section_header: bool
    source_row_index: int
    section_id: str | None


@dataclass(slots=True)
class ValueObject:
    value_id: str
    row_id: str
    column_index: int
    period: str | None
    value_raw: str
    value_numeric: float | None
    unit: str | None
    column_header_path: list[str]


@dataclass(slots=True)
class NormalizedTable:
    table_id: str
    filing_id: str
    fiscal_year: int
    block_id: str
    section_id: str | None
    section_type: str | None
    statement_type: str | None
    table_role: str
    title: str | None
    unit: str | None
    year_candidates: list[int] = field(default_factory=list)
    note_reference_candidates: list[str] = field(default_factory=list)
    context_before: str | None = None
    context_after: str | None = None
    html_fragment: str | None = None
    dom_path: str | None = None
    cells: list[Cell] = field(default_factory=list)
    grid: list[list[str]] = field(default_factory=list)
    rows: list[RowObject] = field(default_factory=list)
    values: list[ValueObject] = field(default_factory=list)


@dataclass(slots=True)
class TextChunk:
    chunk_id: str
    filing_id: str
    fiscal_year: int
    section_id: str | None
    section_type: str | None
    section_title: str | None
    auditor_name: str | None
    near_table_id: str | None
    topic_hint: str | None
    text: str


@dataclass(slots=True)
class DocumentIR:
    filing_id: str
    fiscal_year: int
    company_name: str | None
    report_type: str
    auditor_name: str | None
    auditor_report_date: str | None
    source_file: str
    extracted_at: str
    blocks: list[Block] = field(default_factory=list)
    sections: list[dict[str, Any]] = field(default_factory=list)
    tables: list[NormalizedTable] = field(default_factory=list)
    text_chunks: list[TextChunk] = field(default_factory=list)
    qa_warnings: list[dict[str, Any]] = field(default_factory=list)
