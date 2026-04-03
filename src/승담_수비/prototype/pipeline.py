from __future__ import annotations

import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from models import (
    FilingParseResult,
    NormalizedTable,
    TableRow,
    TableValue,
    TextChunk,
)
from parser import AuditReportParser, decode_html_file
from table_processor import TableProcessor


class AuditReportPipeline:
    """HTML 감사보고서 -> 구조 IR -> RDB/VDB 파생 파이프라인."""

    TEXT_BLOCK_TYPES_FOR_CHUNK = {"paragraph", "footnote", "cover", "section_heading"}

    def parse_file(
        self,
        file_path: str,
        filing_id: Optional[str] = None,
        fiscal_year: Optional[int] = None,
    ) -> FilingParseResult:
        path = Path(file_path)
        html, encoding = decode_html_file(path)
        filing_id = filing_id or self._make_filing_id(path)

        parser = AuditReportParser(html)
        blocks = parser.parse()
        sections = parser.build_sections(filing_id)
        meta = parser.extract_document_meta(
            filing_id=filing_id,
            source_file=str(path),
            source_encoding=encoding,
        )
        if fiscal_year is not None:
            meta.fiscal_year = fiscal_year
        elif meta.fiscal_year is None:
            inferred_year = self._extract_year_from_filename(path.name)
            meta.fiscal_year = inferred_year

        tables = self._build_tables(
            blocks=blocks,
            filing_id=filing_id,
            fiscal_year=meta.fiscal_year,
        )
        chunks = self._build_text_chunks(
            blocks=blocks,
            tables=tables,
            filing_id=filing_id,
            fiscal_year=meta.fiscal_year,
            auditor_name=meta.auditor_name,
        )

        return FilingParseResult(
            meta=meta,
            blocks=blocks,
            sections=sections,
            tables=tables,
            text_chunks=chunks,
        )

    @staticmethod
    def _extract_year_from_filename(filename: str) -> Optional[int]:
        match = re.search(r"(19\d{2}|20\d{2})", filename)
        return int(match.group(1)) if match else None

    def _make_filing_id(self, path: Path) -> str:
        year = self._extract_year_from_filename(path.name) or "unknown"
        stem = re.sub(r"[^0-9A-Za-z가-힣_]+", "_", path.stem)
        return f"{stem}_{year}"

    @staticmethod
    def _collect_context(blocks, table_idx: int, direction: int, window: int = 3) -> str:
        step = -1 if direction < 0 else 1
        idx = table_idx + step
        collected: List[str] = []
        while 0 <= idx < len(blocks) and len(collected) < window:
            block = blocks[idx]
            if block.block_type in {"paragraph", "section_heading", "footnote", "cover"} and block.text:
                collected.append(block.text)
            elif block.block_type == "table":
                table_text = AuditReportPipeline._extract_table_context_text(block.html_fragment)
                if table_text:
                    collected.append(table_text)
            idx += step
        if direction < 0:
            collected.reverse()
        return "\n".join(collected)

    @staticmethod
    def _extract_table_context_text(html_fragment: str, max_len: int = 280) -> str:
        soup = BeautifulSoup(html_fragment, "html.parser")
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
        if not text:
            return ""
        compact = re.sub(r"\s+", "", text)
        # 표 제목/단위/기수 정보를 우선 context로 사용
        keywords = ("재무상태표", "손익계산서", "포괄손익계산서", "자본변동표", "현금흐름표", "단위", "제 ")
        if any(k in compact for k in keywords[:-1]) or "단위" in text or "제 " in text:
            return text[:max_len]
        return ""

    def _build_tables(
        self,
        blocks,
        filing_id: str,
        fiscal_year: Optional[int],
    ) -> List[NormalizedTable]:
        tables: List[NormalizedTable] = []
        for idx, block in enumerate(blocks):
            if block.block_type != "table":
                continue

            context_before = self._collect_context(blocks, idx, direction=-1)
            context_after = self._collect_context(blocks, idx, direction=1)
            processor = TableProcessor(
                block=block,
                filing_id=filing_id,
                fiscal_year=fiscal_year,
                context_before=context_before,
                context_after=context_after,
            )
            normalized = processor.process()
            if normalized:
                tables.append(normalized)
        return tables

    @staticmethod
    def _infer_topic_hint(text: str, section_type: Optional[str]) -> str:
        if "핵심감사사항" in text:
            return "key_audit_matters"
        if "감사의견" in text:
            return "audit_opinion"
        if "내부회계관리제도" in text:
            return "internal_control"
        if "우발부채" in text or "약정사항" in text:
            return "contingent_liabilities_and_commitments"
        if "보고기간 후 사건" in text:
            return "subsequent_events"
        if "회계정책" in text:
            return "accounting_policies"
        return section_type or "general"

    @staticmethod
    def _nearest_table_id(
        table_positions: List[Tuple[int, str]],
        start_idx: int,
        end_idx: int,
        max_distance: int = 5,
    ) -> Optional[str]:
        if not table_positions:
            return None
        mid = (start_idx + end_idx) // 2
        closest_idx, closest_table_id = min(table_positions, key=lambda item: abs(item[0] - mid))
        return closest_table_id if abs(closest_idx - mid) <= max_distance else None

    def _build_text_chunks(
        self,
        blocks,
        tables: List[NormalizedTable],
        filing_id: str,
        fiscal_year: Optional[int],
        auditor_name: Optional[str],
        max_chars: int = 1200,
    ) -> List[TextChunk]:
        block_index = {block.block_id: idx for idx, block in enumerate(blocks)}
        table_positions = sorted(
            (
                block_index[table.source_block_id],
                table.table_id,
            )
            for table in tables
            if table.source_block_id in block_index
        )

        chunks: List[TextChunk] = []
        current_texts: List[str] = []
        current_start_idx: Optional[int] = None
        current_section_id: Optional[str] = None
        current_section_type: Optional[str] = None
        current_section_title: Optional[str] = None

        def flush(end_idx: int) -> None:
            nonlocal current_texts, current_start_idx, current_section_id, current_section_type, current_section_title
            if not current_texts or current_start_idx is None:
                return
            text = "\n".join(part for part in current_texts if part).strip()
            if not text:
                current_texts = []
                current_start_idx = None
                return

            start_block = blocks[current_start_idx]
            end_block = blocks[end_idx]
            topic_hint = self._infer_topic_hint(text=text, section_type=current_section_type)
            near_table_id = self._nearest_table_id(table_positions, current_start_idx, end_idx)
            chunk_id = f"{filing_id}_ch{len(chunks):04d}"
            chunks.append(
                TextChunk(
                    chunk_id=chunk_id,
                    filing_id=filing_id,
                    fiscal_year=fiscal_year,
                    section_type=current_section_type,
                    section_title=current_section_title,
                    auditor_name=auditor_name,
                    near_table_id=near_table_id,
                    topic_hint=topic_hint,
                    text=text,
                    start_block_id=start_block.block_id,
                    end_block_id=end_block.block_id,
                    metadata={"section_id": current_section_id},
                )
            )
            current_texts = []
            current_start_idx = None

        for idx, block in enumerate(blocks):
            if block.block_type not in self.TEXT_BLOCK_TYPES_FOR_CHUNK:
                continue
            if not block.text:
                continue

            section_changed = current_section_id is not None and block.section_id != current_section_id
            current_size = sum(len(part) for part in current_texts)
            oversized = current_texts and (current_size + len(block.text) > max_chars)
            if section_changed or oversized:
                flush(idx - 1)

            if current_start_idx is None:
                current_start_idx = idx
                current_section_id = block.section_id
                current_section_type = block.section_type
                current_section_title = block.section_title

            current_texts.append(block.text)

        if blocks:
            flush(len(blocks) - 1)
        return chunks

    @staticmethod
    def to_rdb_payload(result: FilingParseResult) -> Dict[str, List[Dict[str, object]]]:
        filings = [
            {
                "filing_id": result.meta.filing_id,
                "company_name": result.meta.company_name,
                "fiscal_year": result.meta.fiscal_year,
                "report_type": result.meta.report_type,
                "auditor_name": result.meta.auditor_name,
                "auditor_report_date": result.meta.auditor_report_date,
                "source_file": result.meta.source_file,
                "source_encoding": result.meta.source_encoding,
                "parser_backend": result.meta.parser_backend,
            }
        ]

        sections = [
            {
                "section_id": section.section_id,
                "filing_id": section.filing_id,
                "section_type": section.section_type,
                "section_title": section.section_title,
                "start_block_id": section.start_block_id,
                "end_block_id": section.end_block_id,
                "order_index": section.order_index,
            }
            for section in result.sections
        ]

        tables_payload = []
        table_rows_payload = []
        table_values_payload = []

        for table in result.tables:
            tables_payload.append(
                {
                    "table_id": table.table_id,
                    "filing_id": table.filing_id,
                    "section_id": table.section_id,
                    "section_type": table.section_type,
                    "statement_type": table.statement_type,
                    "table_role": table.table_role,
                    "title": table.title,
                    "unit": table.unit,
                    "year_candidates_json": json.dumps(table.year_candidates, ensure_ascii=False),
                    "source_block_id": table.source_block_id,
                    "context_before": table.context_before,
                    "context_after": table.context_after,
                }
            )

            for row in table.rows:
                table_rows_payload.append(
                    {
                        "row_id": row.row_id,
                        "table_id": row.table_id,
                        "row_index": row.row_index,
                        "raw_label": row.raw_label,
                        "normalized_label": row.normalized_label,
                        "row_depth": row.row_depth,
                        "parent_row_id": row.parent_row_id,
                        "is_section_header": row.is_section_header,
                        "note_reference_json": json.dumps(
                            row.metadata.get("note_reference_candidates", []),
                            ensure_ascii=False,
                        ),
                    }
                )

            for value in table.values:
                table_values_payload.append(
                    {
                        "value_id": value.value_id,
                        "table_id": value.table_id,
                        "row_id": value.row_id,
                        "col_index": value.col_index,
                        "period": value.period,
                        "value_raw": value.value_raw,
                        "value_numeric": value.value_numeric,
                        "unit": value.unit,
                        "column_header_path": value.column_header_path,
                        "is_primary_value": value.is_primary_value,
                        "note_reference_json": json.dumps(value.note_reference_candidates, ensure_ascii=False),
                    }
                )

        return {
            "filings": filings,
            "sections": sections,
            "tables": tables_payload,
            "table_rows": table_rows_payload,
            "table_values": table_values_payload,
            "text_chunks": [
                {
                    "chunk_id": chunk.chunk_id,
                    "filing_id": chunk.filing_id,
                    "fiscal_year": chunk.fiscal_year,
                    "section_type": chunk.section_type,
                    "section_title": chunk.section_title,
                    "auditor_name": chunk.auditor_name,
                    "near_table_id": chunk.near_table_id,
                    "topic_hint": chunk.topic_hint,
                    "text": chunk.text,
                    "start_block_id": chunk.start_block_id,
                    "end_block_id": chunk.end_block_id,
                }
                for chunk in result.text_chunks
            ],
        }

    @staticmethod
    def to_markdown_preview(result: FilingParseResult, max_tables: int = 8) -> str:
        """정규화 객체에서 파생되는 검수용 markdown."""
        lines: List[str] = []
        lines.append(f"# Filing {result.meta.filing_id}")
        lines.append("")
        lines.append(f"- 회사: {result.meta.company_name}")
        lines.append(f"- 회계연도: {result.meta.fiscal_year}")
        lines.append(f"- 감사인: {result.meta.auditor_name}")
        lines.append("")
        lines.append("## Tables (Preview)")

        selected_tables = [
            table
            for table in result.tables
            if table.table_role in {"financial_table", "internal_control_table", "unknown_table"}
        ][:max_tables]

        for table in selected_tables:
            lines.append("")
            lines.append(f"### {table.table_id} | {table.title or 'Untitled'}")
            lines.append(f"- role: {table.table_role}")
            lines.append(f"- statement_type: {table.statement_type}")
            lines.append(f"- unit: {table.unit}")
            lines.append(f"- years: {table.year_candidates}")

            major_rows = [row for row in table.rows if not row.is_section_header][:6]
            for row in major_rows:
                row_values = [value for value in table.values if value.row_id == row.row_id][:3]
                preview = ", ".join(
                    f"{value.period or value.col_index}: {value.value_raw}" for value in row_values
                )
                lines.append(f"- {row.normalized_label}: {preview}")

        lines.append("")
        lines.append("## Text Chunks (Preview)")
        for chunk in result.text_chunks[:5]:
            lines.append("")
            lines.append(f"### {chunk.chunk_id}")
            lines.append(
                f"- section: {chunk.section_type} | {chunk.section_title} | topic={chunk.topic_hint}"
            )
            lines.append(f"- near_table_id: {chunk.near_table_id}")
            lines.append(f"- text: {chunk.text[:260]}...")

        return "\n".join(lines).strip() + "\n"
