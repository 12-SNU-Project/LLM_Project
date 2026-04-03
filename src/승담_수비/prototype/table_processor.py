from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup, FeatureNotFound, Tag

from models import Block, NormalizedTable, TableCell, TableRow, TableValue


class TableProcessor:
    """구조 보존형 테이블 정규화기."""

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
        "운영실태",
        "감사계획",
        "커뮤니케이션",
    )
    HEADER_KEYWORDS = ("과목", "구분", "주석", "당기", "전기", "금액", "단위", "기초", "기말")
    STATEMENT_TYPE_MAP = {
        "재무상태표": "statement_of_financial_position",
        "손익계산서": "income_statement",
        "포괄손익계산서": "statement_of_comprehensive_income",
        "자본변동표": "statement_of_changes_in_equity",
        "현금흐름표": "cash_flow_statement",
    }

    def __init__(
        self,
        block: Block,
        filing_id: str,
        fiscal_year: Optional[int] = None,
        context_before: Optional[str] = None,
        context_after: Optional[str] = None,
    ):
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
        compact = text.replace("\xa0", " ")
        compact = re.sub(r"\s+", " ", compact)
        return compact.strip()

    @staticmethod
    def _leading_indent_score(raw_text: str) -> int:
        """원본 텍스트 선행 공백/nbsp를 계층 신호로 환산."""
        if not raw_text:
            return 0
        prefix_match = re.match(r"^[\s\xa0]*", raw_text)
        prefix = prefix_match.group(0) if prefix_match else ""
        score = 0
        for ch in prefix:
            if ch == "\xa0":
                score += 2
            elif ch == " ":
                score += 1
            elif ch in {"\t", "\n", "\r"}:
                score += 1
        return score

    @staticmethod
    def _is_numeric_text(text: str) -> bool:
        stripped = text.strip()
        if stripped in {"", "-", "—", "N/A"}:
            return False
        candidate = stripped.replace(",", "").replace(" ", "")
        candidate = candidate.strip("()")
        return bool(re.fullmatch(r"-?\d+(\.\d+)?", candidate))

    @staticmethod
    def _parse_numeric(text: str) -> Optional[float]:
        stripped = text.strip()
        if stripped in {"", "-", "—", "N/A"}:
            return None

        negative = stripped.startswith("(") and stripped.endswith(")")
        candidate = stripped.replace(",", "").replace(" ", "").strip("()")
        if not re.fullmatch(r"-?\d+(\.\d+)?", candidate):
            return None
        value = float(candidate)
        return -value if negative else value

    @staticmethod
    def _extract_note_references(text: str) -> List[str]:
        refs = re.findall(r"\d+", text)
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
        numeric = sum(1 for cell in meaningful if self._is_numeric_text(self._normalize_text(cell["text"])))
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
                is_header = (
                    cell_tag.name == "th"
                    or cell_tag.find_parent("thead") is not None
                    or "th" in {cls.lower() for cls in cell_tag.get("class", [])}
                )
                cell_obj = {
                    "origin_row": row_idx,
                    "origin_col": col_idx,
                    "td_index": td_idx,
                    "start_col": col_idx,
                    "rowspan": rowspan,
                    "colspan": colspan,
                    "raw_text": raw_text,
                    "text": text,
                    "is_header": is_header,
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
        explicit_header_rows = []
        for row_idx, row in enumerate(grid):
            unique_cells = self._unique_cells_in_row(row)
            if unique_cells and all(cell.get("is_header") for cell in unique_cells):
                explicit_header_rows.append(row_idx)

        if explicit_header_rows:
            start = min(explicit_header_rows)
            end = max(explicit_header_rows)
            return list(range(start, end + 1))

        # th가 없는 경우: 상위 행에서 헤더 키워드 + 낮은 숫자밀도를 함께 충족할 때만 헤더로 간주
        header_rows: List[int] = []
        for row_idx, row in enumerate(grid[:4]):
            unique_cells = self._unique_cells_in_row(row)
            density = self._row_numeric_density(row)
            joined = " ".join(self._normalize_text(cell.get("text", "")) for cell in unique_cells)
            has_header_keyword = any(token in joined for token in self.HEADER_KEYWORDS)

            if row_idx == 0 and (has_header_keyword or density < 0.15):
                header_rows.append(row_idx)
            elif has_header_keyword and density < 0.45:
                header_rows.append(row_idx)
            else:
                break
        return header_rows

    def _build_column_headers(
        self, grid: List[List[Optional[Dict[str, Any]]]], header_rows: List[int]
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

            joined = " ".join(path)
            col_info[col_idx] = {
                "header_path": path,
                "is_primary": col_idx == (self.max_cols - 1)
                or any(token in joined for token in ("당기", "기말", "총계", "합계")),
            }
        return col_info

    def _detect_label_column_index(
        self, grid: List[List[Optional[Dict[str, Any]]]], col_headers: Dict[int, Dict[str, Any]], header_rows: List[int]
    ) -> int:
        # 1) 헤더 경로에 과목/구분이 명시되면 최우선 사용
        for col_idx in range(self.max_cols):
            header_joined = " ".join(col_headers.get(col_idx, {}).get("header_path", []))
            if any(token in header_joined for token in ("과목", "구분", "항목")):
                return col_idx

        # 2) 본문에서 텍스트 비중이 높은 열을 선택 (초반 열 우선)
        header_set = set(header_rows)
        best_col = 0
        best_score = float("-inf")
        scan_cols = min(self.max_cols, 4)
        for col_idx in range(scan_cols):
            non_empty = 0
            text_like = 0
            numeric_like = 0
            for row_idx, row in enumerate(grid):
                if row_idx in header_set:
                    continue
                cell = row[col_idx] if col_idx < len(row) else None
                if not cell:
                    continue
                if (cell["origin_row"], cell["origin_col"]) != (row_idx, col_idx):
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
            ratio_text = text_like / non_empty
            ratio_numeric = numeric_like / non_empty
            # 텍스트 비율이 높고, 컬럼 인덱스가 앞쪽일수록 점수 가산
            score = (ratio_text * 2.5) - (ratio_numeric * 2.0) - (col_idx * 0.15)
            if score > best_score:
                best_score = score
                best_col = col_idx
        return best_col

    def _is_row_empty(self, row: List[Optional[Dict[str, Any]]]) -> bool:
        for cell in self._unique_cells_in_row(row):
            if self._normalize_text(cell.get("text", "")):
                return False
        return True

    def _is_section_header(self, row: List[Optional[Dict[str, Any]]], label_text: str) -> bool:
        if not label_text:
            return False
        stripped = label_text.strip()
        unique_cells = self._unique_cells_in_row(row)
        non_empty_cells = [c for c in unique_cells if self._normalize_text(c.get("text", ""))]

        if unique_cells and unique_cells[0].get("colspan", 1) >= max(1, self.max_cols - 1):
            return True

        if len(non_empty_cells) == 1 and self._row_numeric_density(row) == 0:
            return True

        if re.match(r"^\[.*\]$|.*:$", stripped):
            return True

        return any(keyword in stripped for keyword in ("재무상태표", "손익계산서", "자본변동표"))

    def _calculate_row_depth(self, raw_text: str) -> int:
        stripped = self._normalize_text(raw_text)
        indent_score = self._leading_indent_score(raw_text)
        depth = max(0, indent_score // 4)

        if re.match(r"^[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+\.", stripped):
            return 0
        if re.match(r"^\d+\.", stripped):
            return max(1, depth)
        if re.match(r"^[가-힣A-Za-z]\.", stripped):
            return max(2, depth + 1)
        if re.match(r"^\([0-9]+\)", stripped):
            return max(2, depth + 1)
        if stripped.startswith("-"):
            return max(1, depth + 1)
        return depth

    @staticmethod
    def _stabilize_row_depth(raw_depth: int, parent_stack: Dict[int, str]) -> int:
        if raw_depth <= 0:
            return 0
        if not parent_stack:
            return 0
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
        self, grid: List[List[Optional[Dict[str, Any]]]], header_rows: List[int]
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
                for cell in self._unique_cells_in_row(row):
                    if self._normalize_text(cell.get("text", "")):
                        label_cell = cell
                        break

            if not label_cell:
                continue

            raw_label_text = label_cell.get("text", "")
            raw_label_with_indent = label_cell.get("raw_text", raw_label_text)
            is_section_header = self._is_section_header(row, raw_label_text)
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
                raw_label=raw_label_text,
                normalized_label=self._normalize_label(raw_label_text),
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

    def _collect_table_context_text(self) -> str:
        return " ".join(chunk for chunk in (self.context_before, self.block.section_title or "") if chunk)

    def _extract_table_title_unit_years(
        self, grid: List[List[Optional[Dict[str, Any]]]], col_headers: Dict[int, Dict[str, Any]]
    ) -> Tuple[Optional[str], Optional[str], List[int]]:
        # 제목 추론은 표 상단 + 직전 문맥 위주로 제한하여 인접 섹션 오염 방지
        context_text = " ".join(part for part in (self.context_before, self.block.section_title or "") if part)
        candidate_text = context_text
        top_rows_texts: List[str] = []
        for row in grid[:4]:
            row_text = " ".join(
                self._normalize_text(cell.get("text", ""))
                for cell in self._unique_cells_in_row(row)
                if self._normalize_text(cell.get("text", ""))
            )
            if row_text:
                top_rows_texts.append(row_text)
                candidate_text += f"\n{row_text}"

        title = None
        title_source = "\n".join(top_rows_texts) + "\n" + context_text
        title_source_compact = re.sub(r"\s+", "", title_source)
        for keyword in self.STATEMENT_TYPE_MAP:
            if keyword in title_source_compact:
                title = keyword
                break

        unit_match = re.search(r"\(단위\s*[:：]\s*([^)]+)\)", candidate_text)
        unit = unit_match.group(1).strip() if unit_match else None

        year_tokens = re.findall(r"(19\d{2}|20\d{2})년?", candidate_text)
        for col_info in col_headers.values():
            joined = " ".join(col_info.get("header_path", []))
            year_tokens.extend(re.findall(r"(19\d{2}|20\d{2})년?", joined))
        year_candidates = sorted({int(token) for token in year_tokens})
        return title, unit, year_candidates

    def _has_numeric_body(
        self, grid: List[List[Optional[Dict[str, Any]]]], header_rows: List[int]
    ) -> bool:
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
        if text_cells == 0:
            return False
        return (numeric_cells / text_cells) >= 0.25

    def _classify_table_role(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        header_rows: List[int],
        title: Optional[str],
    ) -> str:
        table = self.soup.find("table")
        class_names = []
        if table:
            classes = table.get("class", [])
            if isinstance(classes, str):
                classes = [classes]
            class_names = [cls.lower() for cls in classes]

        context_text = self._collect_table_context_text()
        header_text = " ".join(
            " ".join(self._normalize_text(cell.get("text", "")) for cell in self._unique_cells_in_row(grid[row_idx]))
            for row_idx in header_rows
            if row_idx < len(grid)
        )
        signal_text = " ".join(chunk for chunk in (context_text, header_text, title or "") if chunk)
        in_internal_section = self.block.section_type == "internal_control_opinion"
        has_internal_keyword = any(keyword in signal_text for keyword in self.INTERNAL_CONTROL_KEYWORDS)
        has_financial_keyword = any(keyword in signal_text for keyword in self.FINANCIAL_KEYWORDS)

        if in_internal_section:
            return "internal_control_table"
        if has_internal_keyword and not has_financial_keyword:
            return "internal_control_table"

        if has_financial_keyword and self._has_numeric_body(grid, header_rows):
            return "financial_table"

        if "nb" in class_names and not self._has_numeric_body(grid, header_rows):
            return "cover_table"

        if not header_rows and not self._has_numeric_body(grid, header_rows):
            return "cover_table"

        return "unknown_table"

    def _infer_statement_type(self, title: Optional[str]) -> Optional[str]:
        if not title:
            return None
        for keyword, statement_type in self.STATEMENT_TYPE_MAP.items():
            if keyword in title:
                return statement_type
        return None

    @staticmethod
    def _infer_period(header_path: List[str]) -> Optional[str]:
        joined = " ".join(header_path)
        compact = re.sub(r"\s+", "", joined)
        year_match = re.search(r"(19\d{2}|20\d{2})년?", joined)
        if year_match:
            return year_match.group(1)
        for token in ("당기", "전기", "기말", "기초", "누적"):
            if token in joined:
                return token
            if token in compact:
                return token
        if re.search(r"제\d+.*당.*기", compact):
            return "당기"
        if re.search(r"제\d+.*전.*기", compact):
            return "전기"
        return None

    @staticmethod
    def _period_group_key(header_path: List[str], period: Optional[str], col_index: int) -> str:
        if period:
            return period
        joined = " ".join(header_path)
        key_match = re.search(r"(제\s*\d+\s*기|당기|전기|기말|기초)", joined)
        if key_match:
            return key_match.group(1).replace(" ", "")
        return f"col_{col_index}"

    def _build_cells_and_values(
        self,
        grid: List[List[Optional[Dict[str, Any]]]],
        col_headers: Dict[int, Dict[str, Any]],
        row_map: Dict[int, TableRow],
    ) -> Tuple[List[TableCell], List[TableValue]]:
        cells: List[TableCell] = []
        values: List[TableValue] = []
        seen_cells: Set[Tuple[int, int]] = set()

        for row_idx, row in enumerate(grid):
            row_obj = row_map.get(row_idx)
            row_note_refs: List[str] = []

            # 1) 노트 참조 컬럼 선집계
            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                origin = (cell["origin_row"], cell["origin_col"])
                if origin != (row_idx, col_idx):
                    continue
                header_path = col_headers.get(col_idx, {}).get("header_path", [])
                if "주석" in " ".join(header_path):
                    row_note_refs.extend(self._extract_note_references(cell.get("text", "")))

            # 2) cell/value 생성
            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                origin = (cell["origin_row"], cell["origin_col"])
                if origin in seen_cells:
                    continue
                seen_cells.add(origin)

                header_path = col_headers.get(col_idx, {}).get("header_path", [])
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

                value_raw = self._normalize_text(cell_obj.text)
                header_joined = " ".join(header_path)
                is_note_col = "주석" in header_joined
                numeric_value = self._parse_numeric(value_raw)

                if is_note_col:
                    if row_note_refs:
                        row_obj.metadata["note_reference_candidates"] = sorted(set(row_note_refs))
                    continue

                if numeric_value is None:
                    continue

                value_id = f"{self.table_id}_v{row_idx:04d}_{col_idx:03d}"
                period = self._infer_period(header_path)
                values.append(
                    TableValue(
                        value_id=value_id,
                        table_id=self.table_id,
                        row_id=row_obj.row_id,
                        col_index=col_idx,
                        period=period,
                        value_raw=value_raw,
                        value_numeric=numeric_value,
                        unit=None,  # 테이블 단위는 process()에서 상위에서 일괄 주입
                        column_header_path=" > ".join(header_path),
                        is_primary_value=False,
                        note_reference_candidates=sorted(set(row_note_refs)),
                        metadata={
                            "period_group_key": self._period_group_key(
                                header_path=header_path,
                                period=period,
                                col_index=col_idx,
                            )
                        },
                    )
                )

        # 같은 행-기간 그룹 내 대표값(primary) 선정
        grouped: Dict[Tuple[str, str], List[TableValue]] = {}
        for value in values:
            group_key = value.metadata.get("period_group_key", f"col_{value.col_index}")
            grouped.setdefault((value.row_id, group_key), []).append(value)

        for _, group_values in grouped.items():
            if len(group_values) == 1:
                group_values[0].is_primary_value = True
                continue
            # 국내 재무제표에서 동일 기간 내 복수열이면 우측 열을 대표로 간주
            primary = max(group_values, key=lambda item: item.col_index)
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
        table_role = self._classify_table_role(raw_grid, header_rows, title)
        statement_type = self._infer_statement_type(title)
        cells, values = self._build_cells_and_values(raw_grid, col_headers, row_map)

        for value in values:
            value.unit = unit

        return NormalizedTable(
            table_id=self.table_id,
            filing_id=self.filing_id,
            source_block_id=self.block.block_id,
            statement_type=statement_type,
            table_role=table_role,
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
