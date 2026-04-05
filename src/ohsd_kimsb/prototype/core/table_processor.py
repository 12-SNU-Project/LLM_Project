from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup, FeatureNotFound, Tag

try:
    from .models import Block, NormalizedTable, TableCell, TableRow, TableValue
except ImportError:
    from models import Block, NormalizedTable, TableCell, TableRow, TableValue


class TableProcessor:
    FINANCIAL_KEYWORDS = (
        "재무상태표",
        "손익계산서",
        "포괄손익계산서",
        "자본변동표",
        "현금흐름표",
        "과목",
        "주석",
        "당기",
        "전기",
        "단위",
    )
    INTERNAL_CONTROL_KEYWORDS = (
        "내부회계관리제도",
        "감사의견",
        "검토의견",
    )
    HEADER_KEYWORDS = (
        "과목",
        "구분",
        "기업명",
        "내역",
        "항목",
        "주석",
        "당기",
        "전기",
        "기말",
        "기초",
        "단위",
    )
    STATEMENT_TYPE_MAP = { # db에 저장할 키워드
        "포괄손익계산서": "statement_of_comprehensive_income",
        "재무상태표": "statement_of_financial_position",
        "손익계산서": "income_statement",
        "자본변동표": "statement_of_changes_in_equity",
        "현금흐름표": "cash_flow_statement",
    }
    NOTE_COLUMN_KEYWORDS = ("주석",)
    LABEL_COLUMN_KEYWORDS = ("과목", "구분", "기업명", "내역", "항목", "종류")
    PERIOD_KEYWORDS = ("당기", "전기", "당기말", "전기말", "기말", "기초", "반기", "분기")

    def __init__(
        self,
        block: Block,
        filing_id: str,
        fiscal_year: Optional[int] = None,
        context_before: Optional[str] = None,
        context_after: Optional[str] = None,
    ) -> None:
        self.block = block
        self.filing_id = filing_id
        self.fiscal_year = fiscal_year
        self.context_before = context_before or ""
        self.context_after = context_after or ""
        self.soup = self._make_soup(block.html_fragment)
        self.max_cols = 0
        self.label_col_idx = 0
        self.table_id = f"{self.filing_id}_{self.block.block_id}"

    @staticmethod
    def _make_soup(fragment: str) -> BeautifulSoup:
        for backend in ("lxml", "html5lib", "html.parser"):
            try:
                return BeautifulSoup(fragment, backend)
            except FeatureNotFound:
                continue
        return BeautifulSoup(fragment, "html.parser")

    @staticmethod
    def _safe_int(value: Optional[str], default: int = 1) -> int:
        try:
            return max(1, int(value)) if value is not None else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()

    @classmethod
    def _compact_text(cls, text: str) -> str:
        return re.sub(r"\s+", "", cls._normalize_text(text))

    @staticmethod
    def _leading_indent_score(raw_text: str) -> int:
        """원본 텍스트 선행 공백/nbsp를 계층 신호로 환산."""
        prefix = re.match(r"^[\s\xa0]*", raw_text or "")
        if not prefix:
            return 0
        score = 0
        for ch in prefix.group(0):
            score += 2 if ch == "\xa0" else 1
        return score

    @staticmethod
    def _is_numeric_text(text: str) -> bool:
        stripped = text.strip()
        if stripped in {"", "-", "N/A"}:
            return False
        candidate = stripped.strip("()")
        candidate = candidate.replace(" ", "")
        return bool(
            re.fullmatch(r"-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?", candidate)
        )

    @staticmethod
    def _parse_numeric(text: str) -> Optional[float]:
        stripped = text.strip()
        if stripped in {"", "-", "N/A"}:
            return None
        negative = stripped.startswith("(") and stripped.endswith(")")
        candidate = stripped.strip("()").replace(" ", "")
        if not re.fullmatch(r"-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?", candidate):
            return None
        candidate = candidate.replace(",", "")
        value = float(candidate)
        return -value if negative else value

    @staticmethod
    def _extract_note_references(text: str) -> List[str]:
        refs = re.findall(r"\d+(?:\.\d+)?", text)
        return refs if len(refs) <= 10 else []

    @staticmethod
    def _unique_cells_in_row(row: List[Optional[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        unique: List[Dict[str, Any]] = []
        seen: Set[int] = set()
        for cell in row:
            if not cell:
                continue
            key = id(cell)
            if key in seen:
                continue
            seen.add(key)
            unique.append(cell)
        return unique

    def _row_numeric_density(self, row: List[Optional[Dict[str, Any]]]) -> float:
        unique_cells = self._unique_cells_in_row(row)
        meaningful = [cell for cell in unique_cells if self._normalize_text(cell.get("text", ""))]
        if not meaningful:
            return 0.0
        numeric = sum(1 for cell in meaningful if self._is_numeric_text(self._normalize_text(cell.get("text", ""))))
        return numeric / len(meaningful)

    def _unroll_grid(self) -> List[List[Optional[Dict[str, Any]]]]:
        """rowspan/colspan을 논리 grid로 확장."""
        table = self.soup.find("table")
        if not table:
            return []

        rows: List[Tag] = []
        sections = table.find_all(["thead", "tbody", "tfoot"], recursive=False)
        if sections:
            for section in sections:
                rows.extend(section.find_all("tr", recursive=False))
        else:
            rows = table.find_all("tr", recursive=False)
        if not rows:
            return []

        grid: List[List[Optional[Dict[str, Any]]]] = []
        carry: Dict[Tuple[int, int], Dict[str, Any]] = {}

        for row_idx, tr in enumerate(rows):
            row: List[Optional[Dict[str, Any]]] = []
            col_idx = 0

            while (row_idx, col_idx) in carry:
                row.append(carry[(row_idx, col_idx)])
                col_idx += 1

            for td_idx, cell_tag in enumerate(tr.find_all(["th", "td"], recursive=False)):
                while (row_idx, col_idx) in carry:
                    row.append(carry[(row_idx, col_idx)])
                    col_idx += 1

                rowspan = self._safe_int(cell_tag.get("rowspan"), 1)
                colspan = self._safe_int(cell_tag.get("colspan"), 1)
                raw_text = cell_tag.get_text("", strip=False)
                text = self._normalize_text(raw_text)
                class_names = cell_tag.get("class", [])
                if isinstance(class_names, str):
                    class_names = [class_names]
                cell_obj = {
                    "origin_row": row_idx,
                    "origin_col": col_idx,
                    "td_index": td_idx,
                    "rowspan": rowspan,
                    "colspan": colspan,
                    "raw_text": raw_text,
                    "text": text,
                    "is_header": (
                        cell_tag.name == "th"
                        or cell_tag.find_parent("thead") is not None
                        or any(cls.lower() == "th" for cls in class_names)
                    ),
                    "source_html": str(cell_tag),
                }

                for col_offset in range(colspan):
                    row.append(cell_obj)
                    if rowspan > 1:
                        for row_offset in range(1, rowspan):
                            carry[(row_idx + row_offset, col_idx + col_offset)] = cell_obj
                col_idx += colspan

            while (row_idx, col_idx) in carry:
                row.append(carry[(row_idx, col_idx)])
                col_idx += 1

            grid.append(row)

        self.max_cols = max((len(row) for row in grid), default=0)
        for row in grid:
            if len(row) < self.max_cols:
                row.extend([None] * (self.max_cols - len(row)))
        return grid

    def _find_header_rows(self, grid: List[List[Optional[Dict[str, Any]]]]) -> List[int]:
        explicit_rows = []
        for row_idx, row in enumerate(grid):
            unique_cells = self._unique_cells_in_row(row)
            if unique_cells and all(cell.get("is_header") for cell in unique_cells):
                explicit_rows.append(row_idx)
        if explicit_rows:
            return list(range(min(explicit_rows), max(explicit_rows) + 1))

        header_rows: List[int] = []
        for row_idx, row in enumerate(grid[:4]):
            joined = " ".join(
                self._normalize_text(cell.get("text", ""))
                for cell in self._unique_cells_in_row(row)
            )
            density = self._row_numeric_density(row)
            has_keyword = any(keyword in joined for keyword in self.HEADER_KEYWORDS)
            if row_idx == 0 and (has_keyword or density < 0.15):
                header_rows.append(row_idx)
            elif has_keyword and density < 0.45:
                header_rows.append(row_idx)
            else:
                break
        return header_rows

    def _make_column_key(self, header_path: List[str], col_idx: int) -> str:
        if not header_path:
            return f"col_{col_idx}"
        joined = "__".join(self._normalize_text(part) for part in header_path if part)
        joined = re.sub(r"[^\w가-힣]+", "_", joined, flags=re.UNICODE).strip("_").lower()
        return joined or f"col_{col_idx}"

    def _build_column_headers(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        header_rows: List[int],
    ) -> Dict[int, Dict[str, Any]]:
        col_info: Dict[int, Dict[str, Any]] = {}
        for col_idx in range(self.max_cols):
            path: List[str] = []
            for row_idx in header_rows:
                if row_idx >= len(grid):
                    continue
                cell = grid[row_idx][col_idx]
                if not cell:
                    continue
                text = self._normalize_text(cell.get("text", ""))
                if text and text not in path:
                    path.append(text)
            header_joined = " ".join(path)
            col_info[col_idx] = {
                "header_path": path,
                "column_key": self._make_column_key(path, col_idx),
                "is_note_column": any(keyword in header_joined for keyword in self.NOTE_COLUMN_KEYWORDS),
                "is_label_column": any(keyword in header_joined for keyword in self.LABEL_COLUMN_KEYWORDS),
            }
        return col_info

    def _detect_label_column_index(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        col_headers: Dict[int, Dict[str, Any]],
        header_rows: List[int],
    ) -> int:
        for col_idx in range(self.max_cols):
            if col_headers.get(col_idx, {}).get("is_label_column"):
                return col_idx

        header_row_set = set(header_rows)
        best_col = 0
        best_score = float("-inf")
        for col_idx in range(min(self.max_cols, 4)):
            non_empty = 0
            text_like = 0
            numeric_like = 0
            for row_idx, row in enumerate(grid):
                if row_idx in header_row_set:
                    continue
                cell = row[col_idx] if col_idx < len(row) else None
                if not cell or (cell["origin_row"], cell["origin_col"]) != (row_idx, col_idx):
                    continue
                text = self._normalize_text(cell.get("text", ""))
                if not text:
                    continue
                non_empty += 1
                if self._is_numeric_text(text):
                    numeric_like += 1
                else:
                    text_like += 1
            if non_empty == 0:
                continue
            score = (text_like / non_empty) * 2.5 - (numeric_like / non_empty) * 2.0 - (col_idx * 0.15)
            if score > best_score:
                best_score = score
                best_col = col_idx
        return best_col

    def _is_row_empty(self, row: List[Optional[Dict[str, Any]]]) -> bool:
        return not any(self._normalize_text(cell.get("text", "")) for cell in self._unique_cells_in_row(row))

    def _is_section_header(self, row: List[Optional[Dict[str, Any]]], label_text: str) -> bool:
        stripped = self._normalize_text(label_text)
        if not stripped:
            return False
        unique_cells = self._unique_cells_in_row(row)
        non_empty = [cell for cell in unique_cells if self._normalize_text(cell.get("text", ""))]
        if unique_cells and unique_cells[0].get("colspan", 1) >= max(1, self.max_cols - 1):
            return True
        if len(non_empty) == 1 and self._row_numeric_density(row) == 0:
            return True
        if stripped.endswith(":"):
            return True
        return False

    def _calculate_row_depth(self, raw_text: str) -> int:
        stripped = self._normalize_text(raw_text)
        depth = max(0, self._leading_indent_score(raw_text) // 4)
        if re.match(r"^[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+\.", stripped):
            return 0
        if re.match(r"^\d+\.", stripped):
            return max(1, depth or 1)
        if re.match(r"^[가-힣A-Za-z]\.", stripped):
            return max(1, depth or 1)
        if re.match(r"^\(\d+\)", stripped):
            return max(2, depth + 1)
        if stripped.startswith("-"):
            return max(2, depth + 1)
        return depth

    @staticmethod
    def _stabilize_row_depth(raw_depth: int, parent_stack: Dict[int, str]) -> int:
        if raw_depth <= 0 or not parent_stack:
            return 0 if raw_depth <= 0 else min(raw_depth, 1)
        max_existing = max(parent_stack.keys())
        stabilized = min(raw_depth, max_existing + 1)
        while stabilized > 0 and (stabilized - 1) not in parent_stack:
            stabilized -= 1
        return max(0, stabilized)

    @staticmethod
    def _normalize_label(raw_text: str) -> str:
        text = raw_text.replace("\xa0", " ").strip()
        text = re.sub(r"^[\-\u2022]\s*", "", text)
        text = re.sub(r"^[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+\.\s*", "", text)
        text = re.sub(r"^\d+\.\s*", "", text)
        text = re.sub(r"^[가-힣A-Za-z]\.\s*", "", text)
        return text.strip()

    def _build_row_hierarchy(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        header_rows: List[int],
    ) -> Tuple[Dict[int, TableRow], List[TableRow]]:
        header_row_set = set(header_rows)
        row_map: Dict[int, TableRow] = {}
        rows: List[TableRow] = []
        parent_stack: Dict[int, str] = {}

        for row_idx, row in enumerate(grid):
            if row_idx in header_row_set or self._is_row_empty(row):
                continue

            label_cell = row[self.label_col_idx] if self.label_col_idx < len(row) else None
            if label_cell and (label_cell["origin_row"], label_cell["origin_col"]) != (row_idx, self.label_col_idx):
                label_cell = None
            if not label_cell or not self._normalize_text(label_cell.get("text", "")):
                label_cell = next(
                    (cell for cell in self._unique_cells_in_row(row) if self._normalize_text(cell.get("text", ""))),
                    None,
                )
            if not label_cell:
                continue

            raw_label = label_cell.get("text", "")
            raw_label_with_indent = label_cell.get("raw_text", raw_label)
            is_section_header = self._is_section_header(row, raw_label)
            if is_section_header:
                row_depth = 0
            else:
                raw_depth = self._calculate_row_depth(raw_label_with_indent)
                row_depth = self._stabilize_row_depth(raw_depth, parent_stack)
            parent_row_id = parent_stack.get(row_depth - 1) if row_depth > 0 else None
            row_id = f"{self.table_id}_r{row_idx:04d}"

            row_obj = TableRow(
                row_id=row_id,
                table_id=self.table_id,
                row_index=row_idx,
                raw_label=raw_label,
                normalized_label=self._normalize_label(raw_label),
                row_depth=row_depth,
                parent_row_id=parent_row_id,
                is_section_header=is_section_header,
            )
            row_map[row_idx] = row_obj
            rows.append(row_obj)

            parent_stack[row_depth] = row_id
            for depth in list(parent_stack):
                if depth > row_depth:
                    del parent_stack[depth]

        return row_map, rows

    def _extract_table_title_unit_years(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        col_headers: Dict[int, Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[str], List[int]]:
        context_lines = [
            self._normalize_text(line)
            for line in (self.context_before or "").splitlines()
            if self._normalize_text(line)
        ]
        top_rows = []
        for row in grid[:4]:
            row_text = " ".join(
                self._normalize_text(cell.get("text", ""))
                for cell in self._unique_cells_in_row(row)
                if self._normalize_text(cell.get("text", ""))
            )
            if row_text:
                top_rows.append(row_text)
        candidate_text = "\n".join(context_lines[-6:] + top_rows)
        compact = self._compact_text(candidate_text)

        title = None
        for keyword in self.STATEMENT_TYPE_MAP:
            if keyword in compact:
                title = keyword
                break

        if title is None:
            for line in reversed(context_lines[-4:]):
                line_compact = self._compact_text(line)
                if "단위" in line_compact:
                    continue
                if len(line) <= 80:
                    title = line
                    break

        unit_match = re.search(r"\(\s*단위\s*[:：]?\s*([^)]+)\)", candidate_text)
        unit = self._normalize_text(unit_match.group(1)) if unit_match else None

        year_tokens = re.findall(r"(19\d{2}|20\d{2})", candidate_text)
        for col_info in col_headers.values():
            joined = " ".join(col_info.get("header_path", []))
            year_tokens.extend(re.findall(r"(19\d{2}|20\d{2})", joined))
        years = sorted({int(token) for token in year_tokens})
        return title, unit, years

    def _has_numeric_body(self, grid: List[List[Optional[Dict[str, Any]]]], header_rows: List[int]) -> bool:
        header_set = set(header_rows)
        numeric_cells = 0
        text_cells = 0
        for row_idx, row in enumerate(grid):
            if row_idx in header_set:
                continue
            for cell in self._unique_cells_in_row(row):
                text = self._normalize_text(cell.get("text", ""))
                if not text:
                    continue
                text_cells += 1
                if self._is_numeric_text(text):
                    numeric_cells += 1
        return text_cells > 0 and (numeric_cells / text_cells) >= 0.25

    def _is_single_cell_narrative(self, grid: List[List[Optional[Dict[str, Any]]]]) -> bool:
        if len(grid) != 1:
            return False
        unique = self._unique_cells_in_row(grid[0])
        return len(unique) == 1 and len(self._normalize_text(unique[0].get("text", ""))) >= 60

    def _looks_like_cover_period_table(self, grid: List[List[Optional[Dict[str, Any]]]]) -> bool:
        text = " ".join(
            self._normalize_text(cell.get("text", ""))
            for row in grid[:4]
            for cell in self._unique_cells_in_row(row)
        )
        compact = self._compact_text(text)
        return bool(re.search(r"제\d+기", compact) and re.search(r"(19|20)\d{2}년", text))

    def _classify_table(self, grid: List[List[Optional[Dict[str, Any]]]], title: Optional[str]) -> Tuple[str, Optional[str]]:
        table = self.soup.find("table")
        class_names: List[str] = []
        if table:
            classes = table.get("class", [])
            if isinstance(classes, str):
                classes = [classes]
            class_names = [cls.lower() for cls in classes]

        context_text = " ".join(part for part in (self.context_before, self.block.section_title or "", title or "") if part)
        signal_text = self._compact_text(context_text)
        numeric_body = self._has_numeric_body(grid, self._find_header_rows(grid))
        single_cell_narrative = self._is_single_cell_narrative(grid)

        if self.block.section_type and self.block.section_type.startswith("internal_control"):
            if numeric_body:
                subrole = "internal_control_schedule"
            elif single_cell_narrative:
                subrole = "internal_control_notice"
            else:
                subrole = "internal_control_cover"
            return "internal_control_table", subrole

        if self.block.section_type and self.block.section_type.startswith("external_audit"):
            return "unknown_table", "external_audit_table"

        if title and title in self.STATEMENT_TYPE_MAP:
            return "financial_table", "primary_statement"

        if any(keyword in signal_text for keyword in map(self._compact_text, self.FINANCIAL_KEYWORDS)) and numeric_body:
            if self.block.section_type == "attached_financial_statements":
                return "financial_table", "primary_statement"

        if self.block.section_type in {"notes", "note_section", "contingent_liabilities_and_commitments", "subsequent_events"}:
            if single_cell_narrative:
                return "unknown_table", "narrative_notice"
            if numeric_body:
                return "unknown_table", "note_quant_table"
            return "unknown_table", "note_layout_table"

        if self.block.section_type == "cover" or ("nb" in class_names and not numeric_body):
            if self._looks_like_cover_period_table(grid):
                return "cover_table", "cover_period"
            if single_cell_narrative:
                return "cover_table", "cover_notice"
            return "cover_table", "cover_layout"

        if single_cell_narrative:
            return "unknown_table", "narrative_notice"

        if numeric_body:
            return "unknown_table", "quant_table"
        return "unknown_table", "layout_table"

    def _infer_statement_type(self, title: Optional[str]) -> Optional[str]:
        if not title:
            return None
        for keyword, statement_type in self.STATEMENT_TYPE_MAP.items():
            if keyword in title:
                return statement_type
        return None

    def _infer_period(self, header_path: List[str]) -> Optional[str]:
        joined = " ".join(header_path)
        compact = self._compact_text(joined)
        year_match = re.search(r"(19\d{2}|20\d{2})", joined)
        if year_match:
            return year_match.group(1)
        if re.search(r"당.?기말", compact):
            return "당기말"
        if re.search(r"전.?기말", compact):
            return "전기말"
        if re.search(r"당.?기초", compact):
            return "당기초"
        if re.search(r"전.?기초", compact):
            return "전기초"
        if re.search(r"당.?기", compact):
            return "당기"
        if re.search(r"전.?기", compact):
            return "전기"
        for token in self.PERIOD_KEYWORDS:
            if token in joined or self._compact_text(token) in compact:
                return token
        return None

    def _infer_value_role(self, header_path: List[str], value_raw: str) -> Optional[str]:
        header_joined = " ".join(header_path)
        if "%" in header_joined or "%" in value_raw:
            return "ratio"
        if "주식수" in header_joined or header_joined.endswith("(주)") or "주" == header_joined.strip():
            return "share_count"
        if "시간" in header_joined:
            return "hours"
        if "명" in header_joined:
            return "headcount"
        return "amount"

    def _build_cells_and_values(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        col_headers: Dict[int, Dict[str, Any]],
        row_map: Dict[int, TableRow],
        unit: Optional[str],
    ) -> Tuple[List[TableCell], List[TableValue]]:
        cells: List[TableCell] = []
        values: List[TableValue] = []
        seen_cells: Set[Tuple[int, int]] = set()

        for row_idx, row in enumerate(grid):
            row_obj = row_map.get(row_idx)
            row_note_refs: List[str] = []

            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                origin = (cell["origin_row"], cell["origin_col"])
                if origin != (row_idx, col_idx):
                    continue
                header_info = col_headers.get(col_idx, {})
                if header_info.get("is_note_column"):
                    row_note_refs.extend(self._extract_note_references(cell.get("text", "")))

            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                origin = (cell["origin_row"], cell["origin_col"])
                if origin in seen_cells:
                    continue
                seen_cells.add(origin)

                header_info = col_headers.get(col_idx, {})
                header_path = header_info.get("header_path", [])
                cell_id = f"{self.table_id}_c{origin[0]:04d}_{origin[1]:03d}"
                cell_obj = TableCell(
                    cell_id=cell_id,
                    row_index=origin[0],
                    col_index=origin[1],
                    text=cell.get("text", ""),
                    rowspan=cell.get("rowspan", 1),
                    colspan=cell.get("colspan", 1),
                    is_header=bool(cell.get("is_header", False)),
                    source_html=cell.get("source_html", ""),
                    header_path=header_path,
                    row_id=row_obj.row_id if row_obj else None,
                    row_depth=row_obj.row_depth if row_obj else 0,
                    parent_row_id=row_obj.parent_row_id if row_obj else None,
                    is_section_header=row_obj.is_section_header if row_obj else False,
                )
                cells.append(cell_obj)

                if not row_obj or cell_obj.is_header or col_idx == self.label_col_idx:
                    continue
                if header_info.get("is_note_column"):
                    continue

                value_raw = self._normalize_text(cell_obj.text)
                value_numeric = self._parse_numeric(value_raw)
                if value_numeric is None:
                    continue

                period = self._infer_period(header_path)
                column_key = str(header_info.get("column_key", f"col_{col_idx}"))
                values.append(
                    TableValue(
                        value_id=f"{self.table_id}_v{row_idx:04d}_{col_idx:03d}",
                        table_id=self.table_id,
                        row_id=row_obj.row_id,
                        col_index=col_idx,
                        column_key=column_key,
                        period=period,
                        value_role=self._infer_value_role(header_path, value_raw),
                        value_raw=value_raw,
                        value_numeric=value_numeric,
                        unit=unit,
                        column_header_path=" > ".join(header_path),
                        is_primary_value=False,
                        note_reference_candidates=sorted(set(row_note_refs)),
                        metadata={
                            "period_group_key": period or column_key,
                        },
                    )
                )

            if row_obj and row_note_refs:
                row_obj.metadata["note_reference_candidates"] = sorted(set(row_note_refs))

        grouped: Dict[Tuple[str, str], List[TableValue]] = {}
        for value in values:
            grouped.setdefault((value.row_id, str(value.metadata.get("period_group_key"))), []).append(value)
        for group_values in grouped.values():
            if len(group_values) == 1:
                group_values[0].is_primary_value = True
                continue
            primary = max(group_values, key=lambda item: (item.col_index, item.value_role == "amount"))
            primary.is_primary_value = True

        return cells, values

    def process(self) -> Optional[NormalizedTable]:
        raw_grid = self._unroll_grid()
        if not raw_grid:
            return None

        header_rows = self._find_header_rows(raw_grid)
        col_headers = self._build_column_headers(raw_grid, header_rows)
        self.label_col_idx = self._detect_label_column_index(raw_grid, col_headers, header_rows)
        row_map, rows = self._build_row_hierarchy(raw_grid, header_rows)
        title, unit, year_candidates = self._extract_table_title_unit_years(raw_grid, col_headers)
        table_role, table_subrole = self._classify_table(raw_grid, title)
        statement_type = self._infer_statement_type(title)
        cells, values = self._build_cells_and_values(raw_grid, col_headers, row_map, unit)

        return NormalizedTable(
            table_id=self.table_id,
            filing_id=self.filing_id,
            source_block_id=self.block.block_id,
            statement_type=statement_type,
            table_role=table_role,
            table_subrole=table_subrole,
            title=title,
            unit=unit,
            year_candidates=year_candidates,
            context_before=self.context_before or None,
            context_after=self.context_after or None,
            section_id=self.block.section_id,
            section_type=self.block.section_type,
            section_title=self.block.section_title,
            html_fragment=self.block.html_fragment,
            cells=cells,
            rows=rows,
            values=values,
            metadata={
                "max_cols": self.max_cols,
                "label_col_index": self.label_col_idx,
                "row_count": len(raw_grid),
                "header_row_indexes": header_rows,
            },
        )
