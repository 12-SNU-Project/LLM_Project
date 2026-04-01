from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable

from bs4 import BeautifulSoup, Tag

from .structure_first_models import (
    Block,
    Cell,
    DocumentIR,
    NormalizedTable,
    RowObject,
    TextChunk,
    ValueObject,
)
from .structure_first_utils import (
    clean_header_token,
    decode_html_file,
    extract_first_date,
    extract_note_refs,
    extract_year_candidates,
    infer_fiscal_year_from_name,
    is_numeric_like,
    normalize_space,
    now_iso_utc,
    parse_numeric,
    slugify,
    strip_tags,
)


SECTION_KEYWORDS: list[tuple[str, str]] = [
    ("\ub3c5\ub9bd\ub41c \uac10\uc0ac\uc778\uc758 \uac10\uc0ac\ubcf4\uace0\uc11c", "independent_auditor_report"),
    ("\ub0b4\ubd80\ud68c\uacc4\uad00\ub9ac\uc81c\ub3c4 \uac10\uc0ac \ub610\ub294 \uac80\ud1a0\uc758\uacac", "internal_control_report"),
    ("\ub0b4\ubd80\ud68c\uacc4\uad00\ub9ac\uc81c\ub3c4 \uac80\ud1a0\uc758\uacac", "internal_control_report"),
    ("\ub0b4\ubd80\ud68c\uacc4\uad00\ub9ac\uc81c\ub3c4", "internal_control_report"),
    ("\uc678\ubd80\uac10\uc0ac \uc2e4\uc2dc\ub0b4\uc6a9", "external_audit_activity"),
    ("\uc8fc\uc11d", "notes"),
    ("\uc7ac \ubb34 \uc81c \ud45c", "financial_statements"),
    ("\uc7ac\ubb34\uc81c\ud45c", "financial_statements"),
    ("\uac10 \uc0ac \ubcf4 \uace0 \uc11c", "cover"),
]

TABLE_ROLE_KEYWORDS = {
    "cover": [
        "\uc81c ",
        "\ubd80\ud130",
        "\uae4c\uc9c0",
        "\uc8fc\uc8fc \ubc0f \uc774\uc0ac\ud68c \uadc0\uc911",
        "\ud68c\uacc4\ubc95\uc778",
        "\uac10\uc0ac\ubcf4\uace0\uc11c\uc77c",
        "\ubcf8\uc810 \uc18c\uc7ac\uc9c0",
    ],
    "internal_control": [
        "\ub0b4\ubd80\ud68c\uacc4\uad00\ub9ac\uc81c\ub3c4",
        "\uac10\uc0ac\ucc38\uc5ec\uc790",
        "\uc8fc\uc694 \uac10\uc0ac\uc2e4\uc2dc\ub0b4\uc6a9",
        "\uac10\uc0ac\ub300\uc0c1\uc5c5\ubb34",
        "\uc6b4\uc601\uc2e4\ud0dc \ubcf4\uace0",
    ],
    "financial": [
        "\uc7ac \ubb34 \uc0c1 \ud0dc \ud45c",
        "\uc7ac\ubb34\uc0c1\ud0dc\ud45c",
        "\uc190 \uc775 \uacc4 \uc0b0 \uc11c",
        "\uc190\uc775\uacc4\uc0b0\uc11c",
        "\ud3ec \uad04 \uc190 \uc775 \uacc4 \uc0b0 \uc11c",
        "\ud3ec\uad04\uc190\uc775\uacc4\uc0b0\uc11c",
        "\ud604 \uae08 \ud750 \ub984 \ud45c",
        "\ud604\uae08\ud750\ub984\ud45c",
        "\uc790 \ubcf8 \ubcc0 \ub3d9 \ud45c",
        "\uc790\ubcf8\ubcc0\ub3d9\ud45c",
        "\uc790\uc0b0",
        "\ubd80\ucc44",
        "\ub2f9\uae30\uc21c\uc774\uc775",
        "(\ub2e8\uc704",
        "\uc8fc\uc11d",
    ],
}

STATEMENT_KEYWORDS: list[tuple[str, str]] = [
    ("\uc7ac\ubb34\uc0c1\ud0dc\ud45c", "statement_of_financial_position"),
    ("\uc7ac \ubb34 \uc0c1 \ud0dc \ud45c", "statement_of_financial_position"),
    ("\uc190\uc775\uacc4\uc0b0\uc11c", "income_statement"),
    ("\uc190 \uc775 \uacc4 \uc0b0 \uc11c", "income_statement"),
    ("\ud3ec\uad04\uc190\uc775\uacc4\uc0b0\uc11c", "comprehensive_income_statement"),
    ("\ud3ec \uad04 \uc190 \uc775 \uacc4 \uc0b0 \uc11c", "comprehensive_income_statement"),
    ("\ud604\uae08\ud750\ub984\ud45c", "cash_flow_statement"),
    ("\ud604 \uae08 \ud750 \ub984 \ud45c", "cash_flow_statement"),
    ("\uc790\ubcf8\ubcc0\ub3d9\ud45c", "changes_in_equity_statement"),
    ("\uc790 \ubcf8 \ubcc0 \ub3d9 \ud45c", "changes_in_equity_statement"),
]

FOOTNOTE_PATTERNS = (
    "\ubcc4\ucca8 \uc8fc\uc11d\uc740 \ubcf8 \uc7ac\ubb34\uc81c\ud45c\uc758 \uc77c\ubd80\uc785\ub2c8\ub2e4",
    "\uc774 \uac10\uc0ac\ubcf4\uace0\uc11c\ub294 \uac10\uc0ac\ubcf4\uace0\uc11c\uc77c",
)

TOPIC_HINT_PATTERNS: list[tuple[str, str]] = [
    ("\uac10\uc0ac\uc758\uacac", "audit_opinion"),
    ("\ud575\uc2ec\uac10\uc0ac\uc0ac\ud56d", "key_audit_matter"),
    ("\ub0b4\ubd80\ud68c\uacc4\uad00\ub9ac\uc81c\ub3c4", "internal_control"),
    ("\uc6b0\ubc1c\ubd80\ucc44", "contingent_liabilities"),
    ("\uc57d\uc815\uc0ac\ud56d", "commitments"),
    ("\ubcf4\uace0\uae30\uac04 \ud6c4 \uc0ac\uac74", "subsequent_events"),
    ("\ud68c\uacc4\uc815\ucc45", "accounting_policy"),
]


class StructureFirstAuditReportParser:
    def parse_file(self, html_path: Path) -> DocumentIR:
        html_text, encoding = decode_html_file(html_path)
        soup = BeautifulSoup(html_text, "html.parser")
        body = soup.body if soup.body else soup
        filing_id, fiscal_year = self._make_filing_id(html_path)

        blocks = self._build_blocks(body, filing_id, html_path.name)
        sections = self._build_sections(blocks)
        self._attach_sections_to_blocks(blocks, sections)

        doc = DocumentIR(
            filing_id=filing_id,
            fiscal_year=fiscal_year,
            company_name=self._extract_company_name(blocks),
            report_type="annual_audit_report",
            auditor_name=self._extract_auditor_name(blocks),
            auditor_report_date=self._extract_auditor_report_date(blocks),
            source_file=str(html_path),
            extracted_at=now_iso_utc(),
            blocks=blocks,
            sections=sections,
        )
        doc.tables = self._build_tables(doc)
        doc.text_chunks = self._build_text_chunks(doc)
        doc.qa_warnings = self._run_quality_checks(doc, encoding)
        return doc

    def parse_directory(self, data_dir: Path) -> list[DocumentIR]:
        docs: list[DocumentIR] = []
        for html_path in sorted(data_dir.glob("*.htm")) + sorted(data_dir.glob("*.html")):
            docs.append(self.parse_file(html_path))
        return docs

    @staticmethod
    def table_to_dict(table: NormalizedTable) -> dict[str, Any]:
        return asdict(table)

    @staticmethod
    def document_to_dict(doc: DocumentIR) -> dict[str, Any]:
        return {
            "filing_id": doc.filing_id,
            "fiscal_year": doc.fiscal_year,
            "company_name": doc.company_name,
            "report_type": doc.report_type,
            "auditor_name": doc.auditor_name,
            "auditor_report_date": doc.auditor_report_date,
            "source_file": doc.source_file,
            "extracted_at": doc.extracted_at,
            "sections": doc.sections,
            "blocks": [asdict(block) for block in doc.blocks],
            "tables": [StructureFirstAuditReportParser.table_to_dict(t) for t in doc.tables],
            "text_chunks": [asdict(chunk) for chunk in doc.text_chunks],
            "qa_warnings": doc.qa_warnings,
        }

    def write_documents_json(self, docs: list[DocumentIR], out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps([self.document_to_dict(doc) for doc in docs], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_sqlite(self, docs: list[DocumentIR], db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            self._create_schema(conn)
            self._insert_documents(conn, docs)
            conn.commit()
        finally:
            conn.close()

    def write_markdown_review(self, docs: list[DocumentIR], out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        for doc in docs:
            path = out_dir / f"{doc.filing_id}.md"
            lines = [
                f"# {doc.filing_id}",
                "",
                f"- fiscal_year: {doc.fiscal_year}",
                f"- company_name: {doc.company_name}",
                f"- auditor_name: {doc.auditor_name}",
                "",
            ]
            for table in doc.tables:
                if table.table_role not in {"financial_table", "internal_control_table"}:
                    continue
                lines.extend(self._table_to_markdown(table))
                lines.append("")
            path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    def _make_filing_id(self, html_path: Path) -> tuple[str, int]:
        year = infer_fiscal_year_from_name(html_path.name) or 0
        filing_id = f"samsung_audit_{year}" if year else f"samsung_audit_{slugify(html_path.stem)}"
        return filing_id, year

    def _iter_target_tags(self, body: Tag) -> Iterable[Tag]:
        for node in body.descendants:
            if not isinstance(node, Tag):
                continue
            name = node.name.lower()
            if name not in {"p", "table", "h1", "h2", "h3", "h4", "h5", "h6"}:
                continue
            if name == "table" and node.find_parent("table") is not None:
                continue
            if name != "table" and node.find_parent("table") is not None:
                continue
            yield node

    def _build_blocks(self, body: Tag, filing_id: str, source_name: str) -> list[Block]:
        blocks: list[Block] = []
        for order, tag in enumerate(self._iter_target_tags(body)):
            text = normalize_space(tag.get_text(" ", strip=True))
            blocks.append(
                Block(
                    block_id=f"{filing_id}_b{order:04d}",
                    block_type=self._classify_block_type(tag, text, order),
                    tag_name=tag.name.lower(),
                    section_id=None,
                    section_type=None,
                    section_title=None,
                    text=text,
                    html_fragment=str(tag),
                    dom_path=self._dom_path(tag),
                    source_order=order,
                    meta={"source_file": source_name, "class": self._tag_class_string(tag)},
                )
            )
        for idx, block in enumerate(blocks):
            block.prev_block_id = blocks[idx - 1].block_id if idx > 0 else None
            block.next_block_id = blocks[idx + 1].block_id if idx < len(blocks) - 1 else None
        return blocks

    def _dom_path(self, tag: Tag) -> str:
        parts: list[str] = []
        current: Tag | None = tag
        while current is not None and isinstance(current, Tag):
            name = current.name.lower()
            index = 1
            sibling = current.previous_sibling
            while sibling is not None:
                if isinstance(sibling, Tag) and sibling.name.lower() == name:
                    index += 1
                sibling = sibling.previous_sibling
            parts.append(f"{name}[{index}]")
            if name == "body":
                break
            parent = current.parent
            current = parent if isinstance(parent, Tag) else None
        return "/" + "/".join(reversed(parts))

    def _tag_class_string(self, tag: Tag) -> str:
        cls = tag.get("class")
        if isinstance(cls, list):
            return " ".join(str(x) for x in cls)
        if isinstance(cls, str):
            return cls
        return ""

    def _classify_block_type(self, tag: Tag, text: str, order: int) -> str:
        name = tag.name.lower()
        if name == "table":
            return "table"
        cls = self._tag_class_string(tag).upper()
        if "PGBRK" in cls:
            return "page_break"
        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            return "section_heading"
        if "SECTION-" in cls or "PART" in cls:
            return "section_heading"
        if "COVER-TITLE" in cls:
            return "cover"
        if any(pattern in text for pattern in FOOTNOTE_PATTERNS):
            return "footnote"
        if order < 20 and "\uac10 \uc0ac \ubcf4 \uace0 \uc11c" in text:
            return "cover"
        return "paragraph"

    def _build_sections(self, blocks: list[Block]) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = [
            {
                "section_id": "sec_000",
                "section_type": "cover",
                "section_title": "cover",
                "start_block_id": blocks[0].block_id if blocks else None,
                "end_block_id": None,
            }
        ]
        current = sections[0]
        counter = 1
        for block in blocks:
            if block.block_type != "section_heading":
                continue
            current["end_block_id"] = block.prev_block_id
            current = {
                "section_id": f"sec_{counter:03d}",
                "section_type": self._classify_section_type(block.text),
                "section_title": block.text or f"section_{counter:03d}",
                "start_block_id": block.block_id,
                "end_block_id": None,
            }
            sections.append(current)
            counter += 1
        if sections and blocks:
            sections[-1]["end_block_id"] = blocks[-1].block_id
        return sections

    def _attach_sections_to_blocks(self, blocks: list[Block], sections: list[dict[str, Any]]) -> None:
        sec_idx = 0
        for block in blocks:
            while sec_idx + 1 < len(sections):
                next_start = sections[sec_idx + 1]["start_block_id"]
                if next_start and block.block_id >= next_start:
                    sec_idx += 1
                else:
                    break
            sec = sections[sec_idx]
            block.section_id = sec["section_id"]
            block.section_type = sec["section_type"]
            block.section_title = sec["section_title"]

    def _classify_section_type(self, title: str) -> str:
        text = normalize_space(title)
        for keyword, section_type in SECTION_KEYWORDS:
            if keyword in text:
                return section_type
        if "\uac10\uc0ac\uc758\uacac" in text:
            return "audit_opinion"
        if "\ud575\uc2ec\uac10\uc0ac\uc0ac\ud56d" in text:
            return "key_audit_matters"
        return "general"

    def _extract_company_name(self, blocks: list[Block]) -> str | None:
        for block in blocks[:40]:
            m = re.search(r"([가-힣A-Za-z0-9㈜\(\)\s]+주식회사)", block.text)
            if m:
                return normalize_space(m.group(1))
        return None

    def _extract_auditor_name(self, blocks: list[Block]) -> str | None:
        for block in blocks[:120]:
            if "\ud68c\uacc4\ubc95\uc778" not in block.text:
                continue
            m = re.search(r"([가-힣A-Za-z0-9\s]+회계법인)", block.text)
            if m:
                return normalize_space(m.group(1))
        return None

    def _extract_auditor_report_date(self, blocks: list[Block]) -> str | None:
        for block in blocks[:120]:
            found = extract_first_date(block.text)
            if found:
                return found
        return None

    def _build_tables(self, doc: DocumentIR) -> list[NormalizedTable]:
        tables: list[NormalizedTable] = []
        counter = 1
        for idx, block in enumerate(doc.blocks):
            if block.block_type != "table":
                continue

            table_tag = BeautifulSoup(block.html_fragment, "html.parser").find("table")
            if table_tag is None:
                continue

            context_before = self._nearest_text(doc.blocks, idx, -1)
            context_after = self._nearest_text(doc.blocks, idx, 1)
            flat_text = normalize_space(table_tag.get_text(" ", strip=True))
            parsed_cells, grid = self._parse_table_cells(table_tag, doc.filing_id, counter)
            title = self._infer_table_title(doc.blocks, idx)
            unit = self._infer_unit(title, context_before, flat_text)
            years = sorted(
                {
                    *extract_year_candidates(flat_text),
                    *extract_year_candidates(title or ""),
                    *extract_year_candidates(context_before or ""),
                }
            )
            table_role = self._classify_table_role(
                table_text=flat_text,
                table_class=self._tag_class_string(table_tag),
                section_type=block.section_type,
                section_title=block.section_title,
                context_before=context_before,
                context_after=context_after,
            )
            table = NormalizedTable(
                table_id=f"{doc.filing_id}_t{counter:04d}",
                filing_id=doc.filing_id,
                fiscal_year=doc.fiscal_year,
                block_id=block.block_id,
                section_id=block.section_id,
                section_type=block.section_type,
                statement_type=self._classify_statement_type(
                    table_text=flat_text,
                    title=title,
                    section_type=block.section_type,
                    table_role=table_role,
                ),
                table_role=table_role,
                title=title,
                unit=unit,
                year_candidates=years,
                note_reference_candidates=extract_note_refs(flat_text + " " + (context_before or "")),
                context_before=context_before,
                context_after=context_after,
                html_fragment=block.html_fragment,
                dom_path=block.dom_path,
                cells=parsed_cells,
                grid=grid,
            )
            self._post_process_table(table)
            tables.append(table)
            counter += 1
        return tables

    def _parse_table_cells(
        self,
        table_tag: Tag,
        filing_id: str,
        table_counter: int,
    ) -> tuple[list[Cell], list[list[str]]]:
        rows: list[Tag] = []
        for tr in table_tag.find_all("tr"):
            parent_table = tr.find_parent("table")
            if parent_table is table_tag:
                rows.append(tr)

        cells: list[Cell] = []
        grid_map: dict[tuple[int, int], str] = {}
        occupied: dict[tuple[int, int], str] = {}

        for row_idx, tr in enumerate(rows):
            col_idx = 0
            for cell_tag in tr.find_all(["td", "th"], recursive=False):
                while (row_idx, col_idx) in occupied:
                    col_idx += 1

                cell_text = normalize_space(cell_tag.get_text(" ", strip=True))
                rowspan = self._safe_int(cell_tag.get("rowspan"), 1)
                colspan = self._safe_int(cell_tag.get("colspan"), 1)
                class_tokens = [str(x).upper() for x in (cell_tag.get("class") or [])]
                is_header = cell_tag.name.lower() == "th" or "TH" in class_tokens

                cell_id = f"{filing_id}_t{table_counter:04d}_c{len(cells):05d}"
                cells.append(
                    Cell(
                        cell_id=cell_id,
                        row_index=row_idx,
                        col_index=col_idx,
                        text=cell_text,
                        rowspan=rowspan,
                        colspan=colspan,
                        is_header=is_header,
                        source_html=str(cell_tag),
                        header_path=[],
                    )
                )

                for rr in range(row_idx, row_idx + rowspan):
                    for cc in range(col_idx, col_idx + colspan):
                        occupied[(rr, cc)] = cell_id
                        grid_map[(rr, cc)] = cell_text
                col_idx += colspan

        if not grid_map:
            return cells, []
        max_row = max(r for r, _ in grid_map) + 1
        max_col = max(c for _, c in grid_map) + 1
        grid = [[grid_map.get((r, c), "") for c in range(max_col)] for r in range(max_row)]
        return cells, grid

    def _post_process_table(self, table: NormalizedTable) -> None:
        if not table.grid:
            return
        header_rows = self._detect_header_rows(table)
        header_by_col = self._build_column_header_paths(table.grid, header_rows)
        for cell in table.cells:
            if cell.row_index < header_rows:
                cell.is_header = True
            if cell.col_index < len(header_by_col):
                cell.header_path = header_by_col[cell.col_index]
        self._extract_rows_and_values(table, header_rows, header_by_col)

    def _detect_header_rows(self, table: NormalizedTable) -> int:
        probe_rows = min(6, len(table.grid))
        header_rows = 0
        for r in range(probe_rows):
            row = table.grid[r]
            non_empty = [x for x in row if x.strip()]
            if not non_empty:
                header_rows += 1
                continue
            explicit_header = any(c.row_index == r and c.is_header for c in table.cells)
            numeric_non_first = sum(1 for x in row[1:] if is_numeric_like(x))
            if explicit_header or numeric_non_first == 0:
                header_rows += 1
                continue
            break
        return max(1, header_rows)

    def _build_column_header_paths(self, grid: list[list[str]], header_rows: int) -> list[list[str]]:
        col_count = max((len(row) for row in grid), default=0)
        paths: list[list[str]] = []
        for c in range(col_count):
            parts: list[str] = []
            for r in range(min(header_rows, len(grid))):
                token = clean_header_token(grid[r][c])
                if token and token not in parts:
                    parts.append(token)
            paths.append(parts)
        return paths

    def _extract_rows_and_values(
        self,
        table: NormalizedTable,
        header_rows: int,
        header_by_col: list[list[str]],
    ) -> None:
        depth_stack: dict[int, str] = {}
        row_count = 0
        value_count = 0

        for r in range(header_rows, len(table.grid)):
            row = table.grid[r]
            if not any(x.strip() for x in row):
                continue
            raw_label = normalize_space(row[0]) if row else ""
            normalized_label = self._normalize_label(raw_label)
            row_depth = self._infer_row_depth(raw_label)
            parent_row_id = depth_stack.get(row_depth - 1) if row_depth > 0 else None
            numeric_count = sum(1 for token in row[1:] if parse_numeric(token) is not None)
            row_id = f"{table.table_id}_r{row_count:04d}"
            table.rows.append(
                RowObject(
                    row_id=row_id,
                    raw_label=raw_label,
                    normalized_label=normalized_label,
                    row_depth=row_depth,
                    parent_row_id=parent_row_id,
                    is_section_header=(bool(raw_label) and numeric_count == 0),
                    source_row_index=r,
                    section_id=table.section_id,
                )
            )
            depth_stack[row_depth] = row_id
            for depth in list(depth_stack.keys()):
                if depth > row_depth:
                    del depth_stack[depth]

            for c in range(1, len(row)):
                raw_value = row[c].strip()
                if not raw_value:
                    continue
                header_path = header_by_col[c] if c < len(header_by_col) else []
                table.values.append(
                    ValueObject(
                        value_id=f"{table.table_id}_v{value_count:05d}",
                        row_id=row_id,
                        column_index=c,
                        period=self._infer_period(header_path),
                        value_raw=raw_value,
                        value_numeric=parse_numeric(raw_value),
                        unit=table.unit,
                        column_header_path=header_path,
                    )
                )
                value_count += 1
            row_count += 1

    def _normalize_label(self, label: str) -> str:
        text = normalize_space(label)
        text = re.sub(r"^[0-9IVX가-힣]+\.\s*", "", text)
        text = re.sub(r"^[\-\·\•]+\s*", "", text)
        return normalize_space(text)

    def _infer_row_depth(self, label: str) -> int:
        text = label.lstrip()
        if re.match(r"^[IVX]+\.", text):
            return 0
        if re.match(r"^[0-9]+\.", text):
            return 1
        if re.match(r"^[가-힣]\.", text):
            return 2
        if re.match(r"^[\-\·\•]", text):
            return 2
        leading = len(label) - len(label.lstrip())
        if leading >= 6:
            return 3
        if leading >= 3:
            return 2
        return 1 if text else 0

    def _infer_period(self, header_path: list[str]) -> str | None:
        if not header_path:
            return None
        joined = " ".join(header_path)
        year = re.search(r"(20\d{2})", joined)
        if year:
            return year.group(1)
        term = re.search(r"\uc81c\s*([0-9]+)\s*\uae30", joined)
        if term:
            return f"term_{term.group(1)}"
        if "\ub2f9\uae30" in joined:
            return "current"
        if "\uc804\uae30" in joined:
            return "previous"
        return None

    def _nearest_text(self, blocks: list[Block], index: int, direction: int) -> str | None:
        pos = index + direction
        while 0 <= pos < len(blocks):
            candidate = blocks[pos]
            if candidate.block_type in {"paragraph", "section_heading", "cover", "footnote"} and candidate.text:
                return candidate.text
            pos += direction
        return None

    def _infer_table_title(self, blocks: list[Block], index: int) -> str | None:
        current = strip_tags(blocks[index].html_fragment)
        title_markers = (
            "\uc7ac\ubb34\uc0c1\ud0dc\ud45c",
            "\uc7ac \ubb34 \uc0c1 \ud0dc \ud45c",
            "\uc190\uc775\uacc4\uc0b0\uc11c",
            "\uc190 \uc775 \uacc4 \uc0b0 \uc11c",
            "\ud3ec\uad04\uc190\uc775\uacc4\uc0b0\uc11c",
            "\ud3ec \uad04 \uc190 \uc775 \uacc4 \uc0b0 \uc11c",
            "\ud604\uae08\ud750\ub984\ud45c",
            "\ud604 \uae08 \ud750 \ub984 \ud45c",
            "\uc790\ubcf8\ubcc0\ub3d9\ud45c",
            "\uc790 \ubcf8 \ubcc0 \ub3d9 \ud45c",
        )
        if len(current) <= 120 and any(marker in current for marker in title_markers):
            return current
        for offset in (1, 2):
            prev_idx = index - offset
            if prev_idx < 0:
                break
            prev_text = blocks[prev_idx].text
            if not prev_text:
                continue
            if any(marker in prev_text for marker in ("\ud45c", "\uc7ac\ubb34", "\uc190\uc775", "\ud604\uae08\ud750\ub984", "\uc790\ubcf8\ubcc0\ub3d9")) and len(prev_text) <= 180:
                return prev_text
        return None

    def _infer_unit(self, *texts: str | None) -> str | None:
        for text in texts:
            if not text:
                continue
            match = re.search(r"\(\ub2e8\uc704\s*[:：]?\s*([^)]+)\)", text)
            if match:
                return normalize_space(match.group(1))
            match2 = re.search(r"\ub2e8\uc704\s*[:：]\s*([가-힣A-Za-z0-9천만억\(\)\s]+)", text)
            if match2:
                return normalize_space(match2.group(1))
        return None

    def _classify_table_role(
        self,
        table_text: str,
        table_class: str,
        section_type: str | None,
        section_title: str | None,
        context_before: str | None,
        context_after: str | None,
    ) -> str:
        source = " ".join(
            token
            for token in (
                table_text,
                section_type or "",
                section_title or "",
                context_before or "",
                context_after or "",
            )
            if token
        )
        cover_score = sum(1 for keyword in TABLE_ROLE_KEYWORDS["cover"] if keyword in source)
        internal_score = sum(1 for keyword in TABLE_ROLE_KEYWORDS["internal_control"] if keyword in source)
        financial_score = sum(1 for keyword in TABLE_ROLE_KEYWORDS["financial"] if keyword in source)

        if section_type == "cover":
            cover_score += 3
        if section_type in {"internal_control_report", "external_audit_activity"}:
            internal_score += 3
        if section_type in {"financial_statements", "notes"}:
            financial_score += 2

        if "nb" in table_class and cover_score >= 2 and financial_score == 0:
            return "cover_table"
        if internal_score >= max(3, financial_score):
            return "internal_control_table"
        if financial_score >= 2:
            return "financial_table"
        return "unknown_table"

    def _classify_statement_type(
        self,
        table_text: str,
        title: str | None,
        section_type: str | None,
        table_role: str,
    ) -> str | None:
        source = " ".join(token for token in (table_text, title or "", section_type or "") if token)
        for keyword, statement_type in STATEMENT_KEYWORDS:
            if keyword in source:
                return statement_type
        if table_role == "internal_control_table":
            return "internal_control"
        if table_role == "cover_table":
            return "cover"
        if section_type == "notes":
            return "notes_table"
        if table_role == "financial_table":
            return "financial_misc"
        return None

    def _build_text_chunks(self, doc: DocumentIR) -> list[TextChunk]:
        by_section: dict[str, list[Block]] = {}
        for block in doc.blocks:
            if block.block_type not in {"paragraph", "footnote", "cover"} or not block.text:
                continue
            by_section.setdefault(block.section_id or "sec_000", []).append(block)

        chunks: list[TextChunk] = []
        chunk_idx = 0
        max_chars = 900
        min_flush = 280
        for section in doc.sections:
            section_id = section["section_id"]
            section_blocks = by_section.get(section_id, [])
            if not section_blocks:
                continue

            current_texts: list[str] = []
            current_topic: str | None = None
            start_idx = section_blocks[0].source_order

            for block in section_blocks:
                topic_hint = self._infer_topic_hint(block.text, section["section_title"])
                candidate_len = sum(len(x) for x in current_texts) + len(block.text)
                flush = False
                if current_texts and topic_hint != current_topic and candidate_len >= min_flush:
                    flush = True
                if current_texts and candidate_len > max_chars:
                    flush = True

                if flush:
                    chunks.append(
                        TextChunk(
                            chunk_id=f"{doc.filing_id}_ck{chunk_idx:05d}",
                            filing_id=doc.filing_id,
                            fiscal_year=doc.fiscal_year,
                            section_id=section_id,
                            section_type=section["section_type"],
                            section_title=section["section_title"],
                            auditor_name=doc.auditor_name,
                            near_table_id=self._find_near_table_id(doc, section_id, start_idx),
                            topic_hint=current_topic,
                            text="\n".join(current_texts),
                        )
                    )
                    chunk_idx += 1
                    current_texts = []
                    start_idx = block.source_order

                current_texts.append(block.text)
                current_topic = topic_hint if topic_hint is not None else current_topic

            if current_texts:
                chunks.append(
                    TextChunk(
                        chunk_id=f"{doc.filing_id}_ck{chunk_idx:05d}",
                        filing_id=doc.filing_id,
                        fiscal_year=doc.fiscal_year,
                        section_id=section_id,
                        section_type=section["section_type"],
                        section_title=section["section_title"],
                        auditor_name=doc.auditor_name,
                        near_table_id=self._find_near_table_id(doc, section_id, start_idx),
                        topic_hint=current_topic,
                        text="\n".join(current_texts),
                    )
                )
                chunk_idx += 1
        return chunks

    def _infer_topic_hint(self, text: str, section_title: str | None) -> str | None:
        source = f"{section_title or ''} {text}"
        for keyword, topic in TOPIC_HINT_PATTERNS:
            if keyword in source:
                return topic
        if section_title and "\uc8fc\uc11d" in section_title:
            return "notes"
        if section_title and "\ub3c5\ub9bd\ub41c \uac10\uc0ac\uc778\uc758 \uac10\uc0ac\ubcf4\uace0\uc11c" in section_title:
            return "auditor_report"
        return None

    def _find_near_table_id(self, doc: DocumentIR, section_id: str, block_index: int) -> str | None:
        candidates = [
            table
            for table in doc.tables
            if table.section_id == section_id and table.table_role in {"financial_table", "internal_control_table"}
        ]
        if not candidates:
            return None
        best = min(candidates, key=lambda t: abs(self._block_index(doc.blocks, t.block_id) - block_index))
        distance = abs(self._block_index(doc.blocks, best.block_id) - block_index)
        return best.table_id if distance <= 6 else None

    def _block_index(self, blocks: list[Block], block_id: str) -> int:
        for idx, block in enumerate(blocks):
            if block.block_id == block_id:
                return idx
        return 10**9

    def _run_quality_checks(self, doc: DocumentIR, encoding: str) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        required = {"independent_auditor_report", "notes", "internal_control_report"}
        seen = {section["section_type"] for section in doc.sections}
        missing = sorted(required - seen)
        if missing:
            warnings.append(
                {
                    "level": "warn",
                    "type": "missing_section",
                    "details": {"missing_section_types": missing},
                }
            )
        for table in doc.tables:
            if table.table_role != "financial_table":
                continue
            if not any(value.value_numeric is not None for value in table.values):
                warnings.append(
                    {
                        "level": "warn",
                        "type": "financial_table_without_numeric",
                        "details": {"table_id": table.table_id, "title": table.title},
                    }
                )
        if encoding not in {"cp949", "euc-kr"}:
            warnings.append(
                {
                    "level": "info",
                    "type": "unexpected_encoding",
                    "details": {"encoding": encoding},
                }
            )
        return warnings

    def _table_to_markdown(self, table: NormalizedTable, max_rows: int = 20) -> list[str]:
        lines: list[str] = []
        lines.append(f"## {table.title or table.statement_type or table.table_id}")
        lines.append(f"- table_id: `{table.table_id}`")
        lines.append(f"- table_role: `{table.table_role}`")
        lines.append(f"- statement_type: `{table.statement_type}`")
        lines.append(f"- unit: `{table.unit}`")
        lines.append(f"- year_candidates: `{table.year_candidates}`")
        lines.append("")
        lines.append("| row_label | period | value |")
        lines.append("|---|---:|---:|")

        row_map = {row.row_id: row for row in table.rows}
        printed = 0
        for value in table.values:
            row = row_map.get(value.row_id)
            if row is None or value.value_numeric is None:
                continue
            lines.append(f"| {row.normalized_label or row.raw_label} | {value.period or '-'} | {value.value_raw} |")
            printed += 1
            if printed >= max_rows:
                break
        if printed == 0:
            lines.append("| (no numeric rows) | - | - |")
        return lines

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS filings (
                filing_id TEXT PRIMARY KEY,
                company_name TEXT,
                fiscal_year INTEGER NOT NULL,
                report_type TEXT NOT NULL,
                auditor_name TEXT,
                auditor_report_date TEXT,
                source_file TEXT NOT NULL,
                extracted_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sections (
                section_id TEXT PRIMARY KEY,
                filing_id TEXT NOT NULL,
                section_type TEXT NOT NULL,
                section_title TEXT,
                start_block_id TEXT,
                end_block_id TEXT,
                FOREIGN KEY (filing_id) REFERENCES filings(filing_id)
            );

            CREATE TABLE IF NOT EXISTS tables (
                table_id TEXT PRIMARY KEY,
                filing_id TEXT NOT NULL,
                section_id TEXT,
                block_id TEXT NOT NULL,
                table_role TEXT NOT NULL,
                statement_type TEXT,
                title TEXT,
                unit TEXT,
                year_candidates TEXT,
                note_reference_candidates TEXT,
                context_before TEXT,
                context_after TEXT,
                FOREIGN KEY (filing_id) REFERENCES filings(filing_id),
                FOREIGN KEY (section_id) REFERENCES sections(section_id)
            );

            CREATE TABLE IF NOT EXISTS table_rows (
                row_id TEXT PRIMARY KEY,
                table_id TEXT NOT NULL,
                raw_label TEXT,
                normalized_label TEXT,
                row_depth INTEGER,
                parent_row_id TEXT,
                is_section_header INTEGER NOT NULL,
                source_row_index INTEGER,
                FOREIGN KEY (table_id) REFERENCES tables(table_id),
                FOREIGN KEY (parent_row_id) REFERENCES table_rows(row_id)
            );

            CREATE TABLE IF NOT EXISTS table_values (
                value_id TEXT PRIMARY KEY,
                row_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                column_index INTEGER NOT NULL,
                period TEXT,
                value_raw TEXT,
                value_numeric REAL,
                unit TEXT,
                column_header_path TEXT,
                FOREIGN KEY (row_id) REFERENCES table_rows(row_id),
                FOREIGN KEY (table_id) REFERENCES tables(table_id)
            );

            CREATE TABLE IF NOT EXISTS text_chunks (
                chunk_id TEXT PRIMARY KEY,
                filing_id TEXT NOT NULL,
                fiscal_year INTEGER NOT NULL,
                section_id TEXT,
                section_type TEXT,
                section_title TEXT,
                auditor_name TEXT,
                near_table_id TEXT,
                topic_hint TEXT,
                text TEXT NOT NULL,
                FOREIGN KEY (filing_id) REFERENCES filings(filing_id),
                FOREIGN KEY (section_id) REFERENCES sections(section_id),
                FOREIGN KEY (near_table_id) REFERENCES tables(table_id)
            );
            """
        )

    def _insert_documents(self, conn: sqlite3.Connection, docs: list[DocumentIR]) -> None:
        for doc in docs:
            conn.execute(
                """
                INSERT OR REPLACE INTO filings (
                    filing_id, company_name, fiscal_year, report_type, auditor_name,
                    auditor_report_date, source_file, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc.filing_id,
                    doc.company_name,
                    doc.fiscal_year,
                    doc.report_type,
                    doc.auditor_name,
                    doc.auditor_report_date,
                    doc.source_file,
                    doc.extracted_at,
                ),
            )
            for section in doc.sections:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sections (
                        section_id, filing_id, section_type, section_title, start_block_id, end_block_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        section["section_id"],
                        doc.filing_id,
                        section["section_type"],
                        section["section_title"],
                        section["start_block_id"],
                        section["end_block_id"],
                    ),
                )
            for table in doc.tables:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO tables (
                        table_id, filing_id, section_id, block_id, table_role, statement_type,
                        title, unit, year_candidates, note_reference_candidates, context_before, context_after
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        table.table_id,
                        table.filing_id,
                        table.section_id,
                        table.block_id,
                        table.table_role,
                        table.statement_type,
                        table.title,
                        table.unit,
                        json.dumps(table.year_candidates, ensure_ascii=False),
                        json.dumps(table.note_reference_candidates, ensure_ascii=False),
                        table.context_before,
                        table.context_after,
                    ),
                )
                for row in table.rows:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO table_rows (
                            row_id, table_id, raw_label, normalized_label, row_depth,
                            parent_row_id, is_section_header, source_row_index
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row.row_id,
                            table.table_id,
                            row.raw_label,
                            row.normalized_label,
                            row.row_depth,
                            row.parent_row_id,
                            1 if row.is_section_header else 0,
                            row.source_row_index,
                        ),
                    )
                for value in table.values:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO table_values (
                            value_id, row_id, table_id, column_index, period,
                            value_raw, value_numeric, unit, column_header_path
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            value.value_id,
                            value.row_id,
                            table.table_id,
                            value.column_index,
                            value.period,
                            value.value_raw,
                            value.value_numeric,
                            value.unit,
                            json.dumps(value.column_header_path, ensure_ascii=False),
                        ),
                    )
            for chunk in doc.text_chunks:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO text_chunks (
                        chunk_id, filing_id, fiscal_year, section_id, section_type, section_title,
                        auditor_name, near_table_id, topic_hint, text
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk.chunk_id,
                        chunk.filing_id,
                        chunk.fiscal_year,
                        chunk.section_id,
                        chunk.section_type,
                        chunk.section_title,
                        chunk.auditor_name,
                        chunk.near_table_id,
                        chunk.topic_hint,
                        chunk.text,
                    ),
                )

    def _safe_int(self, raw: Any, default: int = 1) -> int:
        if raw is None:
            return default
        try:
            return max(1, int(raw))
        except (TypeError, ValueError):
            return default
