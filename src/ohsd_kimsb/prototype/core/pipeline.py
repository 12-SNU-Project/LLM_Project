from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

try:
    from .models import FilingParseResult, NormalizedTable, TextChunk
    from .html_io import decode_html_file
    from .parser import AuditReportParser
    from .table_processor import TableProcessor
except ImportError:
    from models import FilingParseResult, NormalizedTable, TextChunk
    from html_io import decode_html_file
    from parser import AuditReportParser
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
        for block in blocks:
            block.filing_id = filing_id
        sections = parser.build_sections(filing_id)
        meta = parser.extract_document_meta(
            filing_id=filing_id,
            source_file=str(path),
            source_encoding=encoding,
        )

        if fiscal_year is not None:
            meta.fiscal_year = fiscal_year
        elif meta.fiscal_year is None:
            meta.fiscal_year = self._extract_year_from_filename(path.name)

        tables = self._build_tables(
            blocks=blocks,
            filing_id=filing_id,
            fiscal_year=meta.fiscal_year,
            source_file=str(path),
        )
        text_chunks = self._build_text_chunks(
            blocks=blocks,
            tables=tables,
            filing_id=filing_id,
            fiscal_year=meta.fiscal_year,
            auditor_name=meta.auditor_name,
            source_file=str(path),
        )

        return FilingParseResult(
            meta=meta,
            blocks=blocks,
            sections=sections,
            tables=tables,
            text_chunks=text_chunks,
        )

    @staticmethod
    def _extract_year_from_filename(filename: str) -> Optional[int]:
        match = re.search(r"(19\d{2}|20\d{2})", filename)
        return int(match.group(1)) if match else None

    def _make_filing_id(self, path: Path) -> str:
        year = self._extract_year_from_filename(path.name) or "unknown"
        stem = re.sub(r"[^0-9A-Za-z가-힣]+", "_", path.stem).strip("_")
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
    def _extract_table_context_text(html_fragment: str, max_len: int = 320) -> str:
        soup = BeautifulSoup(html_fragment, "html.parser")
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
        if not text:
            return ""
        return text[:max_len]

    def _build_tables(
        self,
        blocks,
        filing_id: str,
        fiscal_year: Optional[int],
        source_file: Optional[str] = None,
    ) -> List[NormalizedTable]:
        tables: List[NormalizedTable] = []
        for idx, block in enumerate(blocks):
            if block.block_type != "table":
                continue
            processor = TableProcessor(
                block=block,
                filing_id=filing_id,
                fiscal_year=fiscal_year,
                context_before=self._collect_context(blocks, idx, direction=-1),
                context_after=self._collect_context(blocks, idx, direction=1),
            )
            table = processor.process()
            if table:
                table.page_start = block.page_index
                table.page_end = block.page_index
                table.source_file = source_file
                tables.append(table)
        return tables

    @staticmethod
    def _infer_topic_hint(section_type: Optional[str], section_title: Optional[str], text: str) -> str:
        if section_type and section_type != "cover":
            return section_type
        compact = re.sub(r"\s+", "", text)
        for keyword, topic in (
            ("핵심감사사항", "key_audit_matters"),
            ("감사의견", "audit_opinion"),
            ("내부회계관리제도", "internal_control"),
            ("우발부채", "contingent_liabilities_and_commitments"),
            ("보고기간후사건", "subsequent_events"),
            ("회계정책", "accounting_policies"),
        ):
            if keyword in compact:
                return topic
        return section_title or "general"

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

    @staticmethod
    def _split_chunk_text(text: str, max_chars: int) -> List[str]:
        text = text.strip()
        if not text:
            return []
        if len(text) <= max_chars:
            return [text]

        sentence_parts = re.split(r"(?<=[.!?。])\s+|(?<=다\.)\s+|(?<=요\.)\s+|\n+", text)
        sentence_parts = [part.strip() for part in sentence_parts if part.strip()]
        if not sentence_parts:
            sentence_parts = [text]

        chunks: List[str] = []
        current = ""
        for part in sentence_parts:
            candidate = part if not current else f"{current} {part}"
            if len(candidate) <= max_chars:
                current = candidate
                continue
            if current:
                chunks.append(current)
                current = ""
            if len(part) <= max_chars:
                current = part
                continue
            start = 0
            while start < len(part):
                chunks.append(part[start:start + max_chars].strip())
                start += max_chars
        if current:
            chunks.append(current)
        return [chunk for chunk in chunks if chunk]

    def _build_text_chunks(
        self,
        blocks,
        tables: List[NormalizedTable],
        filing_id: str,
        fiscal_year: Optional[int],
        auditor_name: Optional[str],
        source_file: Optional[str] = None,
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
            if current_start_idx is None or not current_texts:
                return
            text = "\n".join(part for part in current_texts if part).strip()
            if not text:
                current_texts = []
                current_start_idx = None
                return

            start_block = blocks[current_start_idx]
            end_block = blocks[end_idx]
            near_table_id = self._nearest_table_id(table_positions, current_start_idx, end_idx)
            topic_hint = self._infer_topic_hint(current_section_type, current_section_title, text)
            for chunk_text in self._split_chunk_text(text, max_chars=max_chars):
                chunk_id = f"{filing_id}_ch{len(chunks):04d}"
                chunks.append(
                    TextChunk(
                        chunk_id=chunk_id,
                        filing_id=filing_id,
                        fiscal_year=fiscal_year,
                        section_id=current_section_id,
                        section_type=current_section_type,
                        section_title=current_section_title,
                        auditor_name=auditor_name,
                        near_table_id=near_table_id,
                        topic_hint=topic_hint,
                        text=chunk_text,
                        start_block_id=start_block.block_id,
                        end_block_id=end_block.block_id,
                        page_start=start_block.page_index,
                        page_end=end_block.page_index,
                        source_file=source_file,
                        metadata={},
                    )
                )
            current_texts = []
            current_start_idx = None

        for idx, block in enumerate(blocks):
            if block.block_type not in self.TEXT_BLOCK_TYPES_FOR_CHUNK or not block.text:
                continue

            heading_boundary = block.block_type == "section_heading" and current_texts
            section_changed = current_section_id is not None and block.section_id != current_section_id
            oversized = current_texts and (sum(len(part) for part in current_texts) + len(block.text) > max_chars)
            if heading_boundary or section_changed or oversized:
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

        metric_facts_payload: List[Dict[str, object]] = []

        for table in result.tables:
            row_lookup = {row.row_id: row for row in table.rows}

            for value in table.values:
                row = row_lookup.get(value.row_id)
                if row is None:
                    continue
                metric_facts_payload.append(
                    {
                        "value_id": value.value_id,
                        "filing_id": result.meta.filing_id,
                        "fiscal_year": result.meta.fiscal_year,
                        "table_id": table.table_id,
                        "section_type": table.section_type,
                        "statement_type": table.statement_type,
                        "table_role": table.table_role,
                        "table_subrole": table.table_subrole,
                        "semantic_table_type": table.semantic_table_type,
                        "table_title": table.title,
                        "table_unit": table.unit,
                        "page_start": table.page_start,
                        "page_end": table.page_end,
                        "row_id": row.row_id,
                        "row_index": row.row_index,
                        "raw_label": row.raw_label,
                        "normalized_label": row.normalized_label,
                        "row_group_label": row.metadata.get("group_label"),
                        "col_index": value.col_index,
                        "column_key": value.column_key,
                        "period": value.period,
                        "value_role": value.value_role,
                        "value_raw": value.value_raw,
                        "value_numeric": value.value_numeric,
                        "unit": value.unit,
                        "column_header_path": value.column_header_path,
                        "is_primary_value": value.is_primary_value,
                    }
                )

        # Runtime SQLite is intentionally lean: table metrics go into one flat
        # fact table. We still keep text_chunks so the exact Chroma source
        # texts remain inspectable during debugging.
        return {
            "filings": filings,
            "metric_facts": metric_facts_payload,
            "text_chunks": [
                {
                    "chunk_id": chunk.chunk_id,
                    "filing_id": chunk.filing_id,
                    "fiscal_year": chunk.fiscal_year,
                    "section_type": chunk.section_type,
                    "section_title": chunk.section_title,
                    "near_table_id": chunk.near_table_id,
                    "topic_hint": chunk.topic_hint,
                    "text": chunk.text,
                    "page_start": chunk.page_start,
                    "page_end": chunk.page_end,
                    "source_file": chunk.source_file,
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
            lines.append(f"- subrole: {table.table_subrole}")
            lines.append(f"- semantic: {table.semantic_table_type}")
            lines.append(f"- statement_type: {table.statement_type}")
            lines.append(f"- unit: {table.unit}")
            lines.append(f"- years: {table.year_candidates}")

            major_rows = [row for row in table.rows if not row.is_section_header][:6]
            for row in major_rows:
                row_values = [value for value in table.values if value.row_id == row.row_id][:3]
                preview = ", ".join(
                    f"{value.column_key} ({value.period or '-'}) = {value.value_raw}"
                    for value in row_values
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
