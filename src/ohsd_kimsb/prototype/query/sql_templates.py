from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional

from .catalog import METRIC_DEFINITIONS
from .schema import QueryIntent, QueryInterpretation, SQLQueryPlan

TABLE_CONTEXT_SELECT = """
SELECT
    t.table_id,
    t.filing_id,
    t.table_title,
    t.semantic_table_type,
    t.table_unit,
    t.table_markdown,
    t.footnotes
FROM tables_registry t
WHERE t.filing_id = ? AND t.semantic_table_type = ?
"""

TABLE_CONTEXT_BY_TABLE_IDS_SELECT = """
SELECT
    t.table_id,
    t.filing_id,
    t.table_title,
    t.semantic_table_type,
    t.table_unit,
    t.table_markdown,
    t.footnotes
FROM tables_registry t
WHERE t.table_id IN ({placeholders})
"""

TABLE_ROWS_BY_TABLE_IDS_SELECT = """
SELECT
    r.row_id,
    r.table_id,
    r.filing_id,
    r.row_index,
    r.raw_label,
    r.normalized_label,
    r.row_depth,
    r.parent_row_id,
    r.is_section_header,
    r.row_group_label,
    r.company_kind
FROM table_rows r
WHERE r.table_id IN ({placeholders})
ORDER BY r.table_id, r.row_index
"""


class SQLTemplateEngine:
    # Runtime DB keeps one flat fact table, so metric lookups only need a few
    # normalized expressions instead of multi-table joins.
    LABEL_EXPR = (
        "REPLACE(REPLACE(REPLACE(COALESCE(m.normalized_label, m.raw_label), ' ', ''), char(10), ''), char(13), '')"
    )
    ROW_LABEL_EXPR = (
        "REPLACE(REPLACE(REPLACE(COALESCE(m.raw_label, m.normalized_label), ' ', ''), char(10), ''), char(13), '')"
    )
    COLUMN_EXPR = (
        "REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(m.column_key, m.column_header_path), '_', ''), ' ', ''), char(10), ''), char(13), '')"
    )
    TITLE_EXPR = (
        "REPLACE(REPLACE(REPLACE(COALESCE(m.table_title, ''), ' ', ''), char(10), ''), char(13), '')"
    )

    BASE_SELECT = """
SELECT
    f.filing_id,
    f.company_name,
    f.fiscal_year,
    m.table_id,
    m.section_type,
    m.statement_type,
    m.table_role,
    m.table_subrole,
    m.semantic_table_type,
    m.table_title,
    m.table_unit,
    m.page_start,
    m.page_end,
    m.row_id,
    m.row_index,
    m.raw_label,
    m.normalized_label,
    m.row_group_label,
    m.company_kind,
    m.value_id,
    m.col_index,
    m.column_key,
    m.period,
    m.value_role,
    m.value_numeric,
    m.value_raw,
    m.unit,
    m.column_header_path,
    m.is_primary_value
FROM metric_facts m
JOIN filings f ON m.filing_id = f.filing_id
"""

    def build(self, interpretation: QueryInterpretation) -> Optional[SQLQueryPlan]:
        if not interpretation.need_sql:
            return None

        if interpretation.intent == QueryIntent.COMPARISON_LIST_LOOKUP:
            return self._build_comparison_list_lookup(interpretation)
        if interpretation.intent == QueryIntent.TABLE_CELL_LOOKUP:
            return self._build_table_cell_lookup(interpretation)
        if (
            interpretation.intent == QueryIntent.TEXT_EXPLANATION
            and interpretation.table_title_terms
            and not interpretation.metric_candidates
        ):
            return self._build_table_structure_lookup(interpretation)
        if interpretation.intent in {QueryIntent.METRIC_LOOKUP, QueryIntent.METRIC_WITH_EXPLANATION}:
            if not interpretation.metric_candidates:
                return None
            return self._build_metric_lookup(interpretation)
        if interpretation.intent == QueryIntent.TREND_COMPARE:
            if not interpretation.metric_candidates:
                return None
            return self._build_trend_compare(interpretation)
        return None

    def execute(self, conn: sqlite3.Connection, plan: Optional[SQLQueryPlan]) -> List[Dict[str, Any]]:
        if plan is None:
            return []
        conn.row_factory = sqlite3.Row
        rows = conn.execute(plan.sql, plan.params).fetchall()
        return [dict(row) for row in rows]

    def fetch_table_contexts(
        self,
        conn: sqlite3.Connection,
        table_ids: Optional[List[str]] = None,
        filing_id: Optional[str] = None,
        semantic_types: Optional[List[str]] = None,
        table_title_terms: Optional[List[str]] = None,
        sql_rows: Optional[List[Dict[str, Any]]] = None,
        interpretation: Optional[QueryInterpretation] = None,
        required_dimensions: Optional[List[str]] = None,
        limit: int = 3,
    ) -> List[Dict[str, Any]]:
        conn.row_factory = sqlite3.Row
        table_ids = self._dedupe_strs(table_ids or [])
        semantic_types = self._dedupe_strs(semantic_types or [])
        table_title_terms = self._dedupe_strs(table_title_terms or [])
        required_dimensions = self._dedupe_strs(required_dimensions or [])
        sql_rows = list(sql_rows or [])

        contexts: List[Dict[str, Any]] = []
        if table_ids:
            placeholders = self._placeholders(table_ids)
            sql = TABLE_CONTEXT_BY_TABLE_IDS_SELECT.format(placeholders=placeholders)
            rows = conn.execute(sql, table_ids).fetchall()
            row_map = {str(row["table_id"]): dict(row) for row in rows}
            contexts.extend(row_map[table_id] for table_id in table_ids if table_id in row_map)

        if not contexts and filing_id and semantic_types:
            for semantic_type in semantic_types:
                rows = conn.execute(TABLE_CONTEXT_SELECT, [filing_id, semantic_type]).fetchall()
                for row in rows:
                    context = dict(row)
                    contexts.append(context)

        filtered = [
            context
            for context in contexts
            if str(context.get("table_markdown") or "").strip() or str(context.get("footnotes") or "").strip()
        ]
        if table_title_terms:
            title_filtered = [
                context
                for context in filtered
                if any(term in str(context.get("table_title") or "") for term in table_title_terms)
            ]
            if title_filtered:
                filtered = title_filtered

        deduped: List[Dict[str, Any]] = []
        seen = set()
        row_context_map = self._fetch_table_row_contexts(
            conn=conn,
            table_ids=[str(context.get("table_id") or "") for context in filtered],
            sql_rows=sql_rows,
            interpretation=interpretation,
            required_dimensions=required_dimensions,
        )
        for context in filtered:
            key = (
                context.get("table_id"),
                context.get("table_title"),
                context.get("semantic_table_type"),
            )
            if key in seen:
                continue
            seen.add(key)
            matched_rows = self._match_context_sql_rows(context, sql_rows)
            deduped.append(
                self._focus_table_context(
                    context=context,
                    sql_rows=matched_rows,
                    interpretation=interpretation,
                    required_dimensions=required_dimensions,
                    focused_rows=row_context_map.get(str(context.get("table_id") or ""), []),
                )
            )
        deduped.sort(
            key=lambda context: self._table_context_rank(
                context=context,
                prioritized_table_ids=table_ids,
                table_title_terms=table_title_terms,
                required_dimensions=required_dimensions,
            ),
            reverse=True,
        )
        return deduped[: max(1, limit)]

    def _focus_table_context(
        self,
        context: Dict[str, Any],
        sql_rows: List[Dict[str, Any]],
        interpretation: Optional[QueryInterpretation],
        required_dimensions: List[str],
        focused_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        focused = dict(context)
        focus_row_labels = self._dedupe_strs(
            [
                *(row.get("raw_label") or row.get("normalized_label") or "" for row in sql_rows),
                *((interpretation.row_label_terms + interpretation.row_label_filters) if interpretation else []),
            ]
        )
        focus_column_keys = self._dedupe_strs(
            [
                *(row.get("column_key") or "" for row in sql_rows),
                *(row.get("period") or "" for row in sql_rows),
                *(interpretation.column_terms if interpretation else []),
            ]
        )
        focused["focus_row_labels"] = focus_row_labels
        focused["focus_column_keys"] = focus_column_keys
        focused["focused_rows"] = list(focused_rows or [])
        focused["focus_row_ids"] = [
            str(row.get("row_id") or "")
            for row in focused["focused_rows"]
            if str(row.get("row_id") or "")
        ]
        focused["focused_table_markdown"] = self._build_focused_table_markdown(
            table_markdown=str(context.get("table_markdown") or ""),
            focus_row_labels=focus_row_labels,
            focus_column_keys=focus_column_keys,
            required_dimensions=required_dimensions,
        )
        focused["focused_footnotes"] = self._build_focused_footnotes(
            footnotes=str(context.get("footnotes") or ""),
            interpretation=interpretation,
            focus_row_labels=focus_row_labels,
            focus_column_keys=focus_column_keys,
            required_dimensions=required_dimensions,
        )
        return focused

    def _fetch_table_row_contexts(
        self,
        conn: sqlite3.Connection,
        table_ids: List[str],
        sql_rows: List[Dict[str, Any]],
        interpretation: Optional[QueryInterpretation],
        required_dimensions: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        table_ids = [table_id for table_id in self._dedupe_strs(table_ids) if table_id]
        if not table_ids:
            return {}

        placeholders = self._placeholders(table_ids)
        sql = TABLE_ROWS_BY_TABLE_IDS_SELECT.format(placeholders=placeholders)
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(sql, table_ids).fetchall()]
        rows_by_table: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            rows_by_table.setdefault(str(row.get("table_id") or ""), []).append(row)

        focus_row_labels = self._dedupe_strs(
            [
                *(row.get("raw_label") or row.get("normalized_label") or "" for row in sql_rows),
                *((interpretation.row_label_terms + interpretation.row_label_filters) if interpretation else []),
            ]
        )
        include_structure = "structure" in required_dimensions
        return {
            table_id: self._select_relevant_table_rows(
                table_rows=table_rows,
                focus_row_labels=focus_row_labels,
                include_structure=include_structure,
            )
            for table_id, table_rows in rows_by_table.items()
        }

    @classmethod
    def _select_relevant_table_rows(
        cls,
        table_rows: List[Dict[str, Any]],
        focus_row_labels: List[str],
        include_structure: bool,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        if not table_rows:
            return []

        by_row_id = {
            str(row.get("row_id") or ""): row
            for row in table_rows
            if str(row.get("row_id") or "")
        }
        normalized_focus = [cls._normalize_lookup_text(label) for label in focus_row_labels if label]

        selected_ids: List[str] = []
        seen_ids = set()
        for row in table_rows:
            row_id = str(row.get("row_id") or "")
            if not row_id:
                continue
            row_text = cls._normalize_lookup_text(
                " ".join(
                    part
                    for part in (
                        row.get("raw_label"),
                        row.get("normalized_label"),
                        row.get("row_group_label"),
                    )
                    if part
                )
            )
            if any(label and label in row_text for label in normalized_focus):
                current_id = row_id
                while current_id:
                    if current_id not in seen_ids and current_id in by_row_id:
                        seen_ids.add(current_id)
                        selected_ids.append(current_id)
                    parent_id = str(by_row_id.get(current_id, {}).get("parent_row_id") or "")
                    current_id = parent_id if parent_id and parent_id in by_row_id else ""

        if include_structure or not selected_ids:
            for row in table_rows:
                row_id = str(row.get("row_id") or "")
                if (
                    row_id
                    and row_id not in seen_ids
                    and (bool(row.get("is_section_header")) or int(row.get("row_depth") or 0) <= 1)
                ):
                    seen_ids.add(row_id)
                    selected_ids.append(row_id)

        selected_rows = [by_row_id[row_id] for row_id in selected_ids if row_id in by_row_id]
        selected_rows.sort(key=lambda row: int(row.get("row_index") or 0))
        return selected_rows[: max(1, limit)]

    @staticmethod
    def _match_context_sql_rows(
        context: Dict[str, Any],
        sql_rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        table_id = str(context.get("table_id") or "").strip()
        if table_id:
            exact = [row for row in sql_rows if str(row.get("table_id") or "").strip() == table_id]
            if exact:
                return exact

        semantic_type = str(context.get("semantic_table_type") or "").strip()
        if semantic_type:
            semantic_rows = [
                row
                for row in sql_rows
                if str(row.get("semantic_table_type") or "").strip() == semantic_type
            ]
            if semantic_rows:
                return semantic_rows
        return sql_rows

    def _build_focused_table_markdown(
        self,
        table_markdown: str,
        focus_row_labels: List[str],
        focus_column_keys: List[str],
        required_dimensions: List[str],
    ) -> str:
        if not table_markdown.strip():
            return ""
        structure_requested = "structure" in required_dimensions
        if not structure_requested and not focus_row_labels and not focus_column_keys:
            return ""

        lines = [line.rstrip() for line in table_markdown.splitlines() if line.strip()]
        prefix_lines = [line for line in lines if not line.lstrip().startswith("|")]
        table_lines = [line for line in lines if line.lstrip().startswith("|")]
        if len(table_lines) < 2:
            return "\n".join(prefix_lines[:2])

        header_cells = self._parse_markdown_row(table_lines[0])
        if not header_cells:
            return "\n".join(prefix_lines[:2])
        has_align_row = len(table_lines) > 1 and self._is_alignment_row(table_lines[1])
        body_lines = table_lines[2:] if has_align_row else table_lines[1:]
        semantic_header_cells = header_cells
        if self._is_placeholder_header(header_cells) and body_lines:
            semantic_header_cells = self._parse_markdown_row(body_lines[0]) or header_cells
            body_lines = body_lines[1:]

        selected_column_indices = self._select_column_indices(semantic_header_cells, focus_column_keys)
        if len(selected_column_indices) == 1 and len(semantic_header_cells) > 1:
            selected_column_indices = self._fallback_column_indices(semantic_header_cells)

        selected_rows: List[List[str]] = []
        row_terms = [self._compact_text(term) for term in focus_row_labels if str(term).strip()]
        for line in body_lines:
            cells = self._parse_markdown_row(line)
            if not cells:
                continue
            label = self._compact_text(cells[0])
            if row_terms and any(term and (term in label or label in term) for term in row_terms):
                selected_rows.append(cells)

        if not selected_rows:
            selected_rows = [
                self._parse_markdown_row(line)
                for line in body_lines[: min(3, len(body_lines))]
                if self._parse_markdown_row(line)
            ]

        selected_indices = sorted(selected_column_indices)
        focused_lines = list(prefix_lines[:2])
        focused_lines.append(self._compose_markdown_row([semantic_header_cells[idx] for idx in selected_indices]))
        focused_lines.append(self._compose_markdown_row(["---"] * len(selected_indices)))
        for cells in selected_rows[:4]:
            padded = list(cells) + [""] * max(0, len(semantic_header_cells) - len(cells))
            focused_lines.append(self._compose_markdown_row([padded[idx] for idx in selected_indices]))
        return "\n".join(line for line in focused_lines if line)

    def _build_focused_footnotes(
        self,
        footnotes: str,
        interpretation: Optional[QueryInterpretation],
        focus_row_labels: List[str],
        focus_column_keys: List[str],
        required_dimensions: List[str],
    ) -> str:
        if not footnotes.strip() or "footnote" not in required_dimensions:
            return ""

        query_markers = self._extract_footnote_markers(interpretation.raw_question if interpretation else "")
        markers = query_markers or self._extract_footnote_markers(focus_row_labels, focus_column_keys)
        lines = [line.strip() for line in footnotes.splitlines() if line.strip()]
        if not lines:
            return ""
        if markers:
            segments = self._split_footnote_segments("\n".join(lines))
            selected = [segment for segment in segments if any(marker in segment for marker in markers)]
            if selected:
                return "\n".join(selected)

        focus_terms = [self._compact_text(term) for term in [*focus_row_labels, *focus_column_keys] if str(term).strip()]
        if focus_terms:
            selected = [
                line
                for line in lines
                if any(term in self._compact_text(line) for term in focus_terms)
            ]
            if selected:
                return "\n".join(selected[:2])
        return "\n".join(lines[:1])

    def _select_column_indices(self, header_cells: List[str], focus_column_keys: List[str]) -> set[int]:
        selected = {0}
        column_terms = [self._compact_text(term) for term in focus_column_keys if str(term).strip()]
        for idx, cell in enumerate(header_cells[1:], start=1):
            compact_cell = self._compact_text(cell)
            if any(term and (term in compact_cell or compact_cell in term) for term in column_terms):
                selected.add(idx)
        return selected

    def _fallback_column_indices(self, header_cells: List[str], limit: int = 5) -> set[int]:
        selected = {0}
        for idx, cell in enumerate(header_cells[1:], start=1):
            compact_cell = self._compact_text(cell)
            if compact_cell in {"합계", "계", "총계", "주석"}:
                continue
            selected.add(idx)
            if len(selected) >= limit:
                break
        return selected

    @staticmethod
    def _parse_markdown_row(line: str) -> List[str]:
        stripped = line.strip()
        if not stripped.startswith("|"):
            return []
        stripped = stripped.strip("|")
        return [cell.strip() for cell in stripped.split("|")]

    @staticmethod
    def _compose_markdown_row(cells: List[str]) -> str:
        return "| " + " | ".join(cells) + " |"

    @staticmethod
    def _is_alignment_row(line: str) -> bool:
        compact = line.replace("|", "").replace(":", "").replace("-", "").replace(" ", "")
        return compact == ""

    @staticmethod
    def _is_placeholder_header(cells: List[str]) -> bool:
        if not cells:
            return False
        return all(re.fullmatch(r"열\d+", cell or "") for cell in cells)

    @staticmethod
    def _extract_footnote_markers(*values: Any) -> List[str]:
        markers: List[str] = []
        for value in values:
            text = str(value or "")
            for marker in re.findall(r"\(\*\d+\)|\[\*\d+\]", text):
                if marker not in markers:
                    markers.append(marker)
        return markers

    @staticmethod
    def _split_footnote_segments(text: str) -> List[str]:
        if not text.strip():
            return []
        matches = re.finditer(r"(\(\*\d+\)|\[\*\d+\])", text)
        spans = [(match.start(), match.group(1)) for match in matches]
        if not spans:
            return [text.strip()]
        segments: List[str] = []
        for index, (start, _) in enumerate(spans):
            end = spans[index + 1][0] if index + 1 < len(spans) else len(text)
            segment = text[start:end].strip()
            if segment:
                segments.append(segment)
        return segments

    @staticmethod
    def _compact_text(value: Any) -> str:
        return re.sub(r"[^0-9A-Za-z가-힣]+", "", str(value or "")).lower()

    @staticmethod
    def _normalize_lookup_text(value: Any) -> str:
        return re.sub(r"[\s_]+", "", str(value or "")).lower()

    def _build_metric_lookup(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        label_candidates = self._expand_label_candidates(interpretation.metric_candidates)
        column_candidates = self._expand_column_candidates(interpretation.metric_candidates)
        statement_types = self._expand_statement_types(interpretation.metric_candidates)

        where_clauses, params = self._build_metric_predicates(
            interpretation=interpretation,
            label_candidates=label_candidates,
            column_candidates=column_candidates,
            statement_types=statement_types,
        )

        if interpretation.year is not None:
            where_clauses.append("f.fiscal_year = ?")
            params.append(interpretation.year)
        elif interpretation.year_range is not None:
            where_clauses.append("f.fiscal_year BETWEEN ? AND ?")
            params.extend(list(interpretation.year_range))

        table_title_filter_terms = list(interpretation.table_title_terms)
        if interpretation.row_label_filters and len(table_title_filter_terms) <= 1:
            table_title_filter_terms = []
        if table_title_filter_terms:
            where_clauses.append(self._build_like_clause(self.TITLE_EXPR, table_title_filter_terms))
            params.extend([self._like_param(value) for value in table_title_filter_terms])

        if interpretation.row_label_filters and interpretation.column_terms:
            where_clauses.append(self._build_like_clause(self.COLUMN_EXPR, interpretation.column_terms))
            params.extend([self._like_param(value) for value in interpretation.column_terms])

        sql = (
            self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY "
            + self._semantic_order_expr(interpretation)
            + ", "
            + "f.fiscal_year DESC, m.table_id, m.row_index, m.col_index\nLIMIT ?"
        )
        params.append(interpretation.limit)
        return SQLQueryPlan(
            template_name="metric_lookup",
            sql=sql.strip(),
            params=params,
            metadata={
                "metric_candidates": interpretation.metric_candidates,
                "row_label_filters": interpretation.row_label_filters,
                "label_candidates": label_candidates,
                "column_candidates": column_candidates,
                "statement_types": statement_types,
            },
        )

    def _build_trend_compare(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        label_candidates = self._expand_label_candidates(interpretation.metric_candidates)
        column_candidates = self._expand_column_candidates(interpretation.metric_candidates)
        statement_types = self._expand_statement_types(interpretation.metric_candidates)
        year_params: List[Any] = []
        cte = ""

        if interpretation.year_range is not None:
            year_filter_sql = "f.fiscal_year BETWEEN ? AND ?"
            year_params.extend(list(interpretation.year_range))
        elif interpretation.year_window:
            cte = """
WITH target_years AS (
    SELECT DISTINCT fiscal_year
    FROM filings
    WHERE fiscal_year IS NOT NULL
"""
            if interpretation.year is not None:
                cte += "      AND fiscal_year <= ?\n"
                year_params.append(interpretation.year)
            cte += "    ORDER BY fiscal_year DESC\n    LIMIT ?\n)\n"
            year_params.append(interpretation.year_window)
            year_filter_sql = "f.fiscal_year IN (SELECT fiscal_year FROM target_years)"
        elif interpretation.year is not None:
            year_filter_sql = "f.fiscal_year <= ?"
            year_params.append(interpretation.year)
        else:
            year_filter_sql = "f.fiscal_year IS NOT NULL"

        where_clauses, metric_params = self._build_metric_predicates(
            interpretation=interpretation,
            label_candidates=label_candidates,
            column_candidates=column_candidates,
            statement_types=statement_types,
        )
        where_clauses.append(year_filter_sql)

        table_title_filter_terms = list(interpretation.table_title_terms)
        if interpretation.row_label_filters and len(table_title_filter_terms) <= 1:
            table_title_filter_terms = []
        if table_title_filter_terms:
            where_clauses.append(self._build_like_clause(self.TITLE_EXPR, table_title_filter_terms))
            metric_params.extend([self._like_param(value) for value in table_title_filter_terms])

        if cte:
            params = list(year_params) + metric_params
        else:
            params = metric_params + list(year_params)

        sql = (
            cte
            + self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY "
            + self._semantic_order_expr(interpretation)
            + ", "
            + "f.fiscal_year ASC, m.table_id, m.row_index, m.col_index"
        )
        return SQLQueryPlan(
            template_name="trend_compare",
            sql=sql.strip(),
            params=params,
            metadata={
                "metric_candidates": interpretation.metric_candidates,
                "row_label_filters": interpretation.row_label_filters,
                "label_candidates": label_candidates,
                "column_candidates": column_candidates,
                "statement_types": statement_types,
                "year_window": interpretation.year_window,
            },
        )

    def _build_table_cell_lookup(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        where_clauses = ["m.is_primary_value = 1"]
        params: List[Any] = []

        if interpretation.year is not None:
            where_clauses.append("f.fiscal_year = ?")
            params.append(interpretation.year)
        elif interpretation.year_range is not None:
            where_clauses.append("f.fiscal_year BETWEEN ? AND ?")
            params.extend(list(interpretation.year_range))

        if interpretation.row_label_filters:
            where_clauses.append(self._build_like_clause(self.ROW_LABEL_EXPR, interpretation.row_label_filters))
            params.extend([self._like_param(value) for value in interpretation.row_label_filters])

        if interpretation.row_label_terms:
            where_clauses.append(self._build_like_clause(self.ROW_LABEL_EXPR, interpretation.row_label_terms))
            params.extend([self._like_param(value) for value in interpretation.row_label_terms])

        if interpretation.column_terms:
            where_clauses.append(self._build_like_clause(self.COLUMN_EXPR, interpretation.column_terms))
            params.extend([self._like_param(value) for value in interpretation.column_terms])

        if interpretation.table_title_terms:
            where_clauses.append(self._build_like_clause(self.TITLE_EXPR, interpretation.table_title_terms))
            params.extend([self._like_param(value) for value in interpretation.table_title_terms])

        entity_scope_terms = {
            term for term in interpretation.table_title_terms
            if term in {"종속기업", "관계기업", "공동기업"}
        }
        if interpretation.row_label_filters and entity_scope_terms and not interpretation.column_terms:
            where_clauses.append("COALESCE(m.semantic_table_type, '') = 'subsidiary_status_table'")

        if interpretation.period:
            # period는 전기/당기말처럼 열 헤더에도 있고, `(2) 전기` 같은 표 제목에도 숨어 있을 수 있다.
            where_clauses.append(
                "("
                "COALESCE(m.period, '') = ? "
                "OR COALESCE(m.column_key, '') LIKE ? "
                "OR COALESCE(m.column_header_path, '') LIKE ? "
                "OR COALESCE(m.table_title, '') LIKE ?"
                ")"
            )
            params.extend(
                [
                    interpretation.period,
                    f"%{interpretation.period}%",
                    f"%{interpretation.period}%",
                    f"%{interpretation.period}%",
                ]
            )

        sql = (
            self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY "
            + self._table_cell_order_expr(interpretation)
            + ", "
            + self._table_cell_period_order_expr(interpretation)
            + ", f.fiscal_year DESC, m.page_start ASC, m.row_index ASC, m.col_index ASC\nLIMIT ?"
        )
        params.append(interpretation.limit)
        return SQLQueryPlan(
            template_name="table_cell_lookup",
            sql=sql.strip(),
            params=params,
            metadata={
                "row_label_filters": interpretation.row_label_filters,
                "row_label_terms": interpretation.row_label_terms,
                "column_terms": interpretation.column_terms,
                "table_title_terms": interpretation.table_title_terms,
                "period": interpretation.period,
            },
        )

    def _build_table_structure_lookup(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        where_clauses = ["m.is_primary_value = 1"]
        params: List[Any] = []

        if interpretation.year is not None:
            where_clauses.append("f.fiscal_year = ?")
            params.append(interpretation.year)
        elif interpretation.year_range is not None:
            where_clauses.append("f.fiscal_year BETWEEN ? AND ?")
            params.extend(list(interpretation.year_range))

        if interpretation.table_title_terms:
            where_clauses.append(self._build_like_clause(self.TITLE_EXPR, interpretation.table_title_terms))
            params.extend([self._like_param(value) for value in interpretation.table_title_terms])

        sql = (
            self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY "
            + self._table_structure_order_expr()
            + ", f.fiscal_year DESC, m.page_start ASC, m.row_index ASC, m.col_index ASC\nLIMIT ?"
        )
        params.append(max(interpretation.limit, 24))
        return SQLQueryPlan(
            template_name="table_structure_lookup",
            sql=sql.strip(),
            params=params,
            metadata={
                "table_title_terms": interpretation.table_title_terms,
                "mode": "structure_lookup",
            },
        )

    def _build_comparison_list_lookup(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        where_clauses = [
            "m.is_primary_value = 1",
            "m.value_numeric IS NOT NULL",
        ]
        params: List[Any] = []

        if interpretation.year is not None:
            where_clauses.append("f.fiscal_year = ?")
            params.append(interpretation.year)
        elif interpretation.year_range is not None:
            where_clauses.append("f.fiscal_year BETWEEN ? AND ?")
            params.extend(list(interpretation.year_range))

        if interpretation.table_title_terms:
            where_clauses.append(self._build_like_clause(self.TITLE_EXPR, interpretation.table_title_terms))
            params.extend([self._like_param(value) for value in interpretation.table_title_terms])

        if interpretation.column_terms:
            where_clauses.append(self._build_like_clause(self.COLUMN_EXPR, interpretation.column_terms))
            params.extend([self._like_param(value) for value in interpretation.column_terms])

        if interpretation.entity_scope in {"subsidiary", "associate", "joint_venture"}:
            # 현황표 계열 비교는 파싱 단계에서 추론한 company_kind를 우선 신뢰한다.
            where_clauses.append("COALESCE(m.semantic_table_type, '') = 'subsidiary_status_table'")
            if interpretation.entity_scope == "subsidiary":
                where_clauses.append("COALESCE(m.company_kind, '') = 'subsidiary'")
            elif interpretation.entity_scope == "associate":
                where_clauses.append("COALESCE(m.company_kind, '') = 'associate'")
            elif interpretation.entity_scope == "joint_venture":
                where_clauses.append("COALESCE(m.company_kind, '') = 'joint_venture'")

        if interpretation.column_terms and any("지분율" in term for term in interpretation.column_terms):
            where_clauses.append("COALESCE(m.unit, '') IN ('percent', '%')")

        if interpretation.comparison_operator and interpretation.threshold_value is not None:
            operator = self._comparison_sql_operator(interpretation.comparison_operator)
            where_clauses.append(f"m.value_numeric {operator} ?")
            params.append(interpretation.threshold_value)

        order_direction = "ASC"
        if interpretation.comparison_operator in {"gt", "gte"}:
            order_direction = "DESC"

        sql = (
            self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY "
            + f"f.fiscal_year DESC, m.value_numeric {order_direction}, m.page_start ASC, m.row_index ASC, m.col_index ASC\nLIMIT ?"
        )
        params.append(max(interpretation.limit, 25))
        return SQLQueryPlan(
            template_name="comparison_list_lookup",
            sql=sql.strip(),
            params=params,
            metadata={
                "comparison_operator": interpretation.comparison_operator,
                "threshold_value": interpretation.threshold_value,
                "entity_scope": interpretation.entity_scope,
                "column_terms": interpretation.column_terms,
                "table_title_terms": interpretation.table_title_terms,
            },
        )

    def _build_metric_predicates(
        self,
        interpretation: QueryInterpretation,
        label_candidates: List[str],
        column_candidates: List[str],
        statement_types: List[str],
    ) -> tuple[List[str], List[Any]]:
        where_clauses = [
            "m.is_primary_value = 1",
            # 주석 참조 열은 숫자 fact가 아니라 note 번호라서 metric 조회에서 제외한다.
            f"{self.COLUMN_EXPR} NOT IN ('주석', '주')",
        ]
        params: List[Any] = []

        if interpretation.row_label_filters:
            metric_predicates = []
            if label_candidates:
                metric_predicates.append(f"{self.LABEL_EXPR} IN ({self._placeholders(label_candidates)})")
                params.extend(label_candidates)
            if column_candidates:
                metric_predicates.append(self._build_like_clause(self.COLUMN_EXPR, column_candidates))
                params.extend([self._like_param(value) for value in column_candidates])
            if metric_predicates:
                where_clauses.append("(" + " OR ".join(metric_predicates) + ")")

            # Entity-specific lookups target note/related-party style tables as
            # well, so match the row label instead of forcing primary statements.
            where_clauses.append(self._build_like_clause(self.ROW_LABEL_EXPR, interpretation.row_label_filters))
            params.extend([self._like_param(value) for value in interpretation.row_label_filters])
        else:
            where_clauses.append(f"{self.LABEL_EXPR} IN ({self._placeholders(label_candidates)})")
            params.extend(label_candidates)
            where_clauses.append("m.section_type = 'attached_financial_statements'")
            if statement_types:
                where_clauses.append(f"m.statement_type IN ({self._placeholders(statement_types)})")
                params.extend(statement_types)

        return where_clauses, params

    @staticmethod
    def _semantic_order_expr(interpretation: QueryInterpretation) -> str:
        if interpretation.row_label_filters:
            # 특정 회사/거래 상대방이 들어오면 특수관계자 거래표를 먼저 본다.
            return (
                "CASE COALESCE(m.semantic_table_type, '') "
                "WHEN 'related_party_transaction_table' THEN 0 "
                "WHEN 'related_party_balance_table' THEN 1 "
                "WHEN 'subsidiary_summary_financial_table' THEN 2 "
                "WHEN 'subsidiary_status_table' THEN 3 "
                "WHEN 'primary_financial_statement' THEN 4 "
                "ELSE 9 END"
            )

        # 일반 metric 질의는 본표를 우선하고, 그 다음 보조 설명표를 본다.
        return (
            "CASE COALESCE(m.semantic_table_type, '') "
            "WHEN 'primary_financial_statement' THEN 0 "
            "WHEN 'subsidiary_summary_financial_table' THEN 1 "
            "WHEN 'related_party_transaction_table' THEN 2 "
            "WHEN 'related_party_balance_table' THEN 3 "
            "ELSE 9 END"
        )

    @staticmethod
    def _table_cell_order_expr(interpretation: QueryInterpretation) -> str:
        # Entity-specific note lookups should prefer the semantically closest
        # note table before falling back to generic primary statements.
        column_terms = [str(term or "") for term in interpretation.column_terms]
        if any(("채권" in term) or ("채무" in term) for term in column_terms):
            return (
                "CASE COALESCE(m.semantic_table_type, '') "
                "WHEN 'related_party_balance_table' THEN 0 "
                "WHEN 'subsidiary_summary_financial_table' THEN 1 "
                "WHEN 'subsidiary_status_table' THEN 2 "
                "WHEN 'primary_financial_statement' THEN 3 "
                "ELSE 9 END"
            )
        if any(("매출" in term) or ("매입" in term) for term in column_terms):
            return (
                "CASE COALESCE(m.semantic_table_type, '') "
                "WHEN 'related_party_transaction_table' THEN 0 "
                "WHEN 'subsidiary_summary_financial_table' THEN 1 "
                "WHEN 'primary_financial_statement' THEN 2 "
                "ELSE 9 END"
            )
        if any("지분" in term for term in column_terms) or interpretation.entity_scope:
            return (
                "CASE COALESCE(m.semantic_table_type, '') "
                "WHEN 'subsidiary_status_table' THEN 0 "
                "WHEN 'subsidiary_summary_financial_table' THEN 1 "
                "WHEN 'primary_financial_statement' THEN 2 "
                "ELSE 9 END"
            )
        # 셀 단위 조회는 note table까지 포함되므로 의미 라벨과 period 일치도를 우선한다.
        return (
            "CASE COALESCE(m.semantic_table_type, '') "
            "WHEN 'primary_financial_statement' THEN 0 "
            "WHEN 'property_plant_equipment_rollforward_table' THEN 1 "
            "WHEN 'intangible_asset_rollforward_table' THEN 1 "
            "WHEN 'actuarial_assumption_table' THEN 2 "
            "WHEN 'subsidiary_summary_financial_table' THEN 3 "
            "WHEN 'related_party_transaction_table' THEN 4 "
            "WHEN 'related_party_balance_table' THEN 5 "
            "WHEN 'subsidiary_status_table' THEN 6 "
            "ELSE 9 END"
        )

    @staticmethod
    def _table_structure_order_expr() -> str:
        # 표 구성 질문은 대표 행(기초/기말 장부가액)을 먼저 보여 주면 열 목록을 설명하기 쉽다.
        return (
            "CASE COALESCE(m.semantic_table_type, '') "
            "WHEN 'property_plant_equipment_rollforward_table' THEN 0 "
            "WHEN 'intangible_asset_rollforward_table' THEN 1 "
            "WHEN 'subsidiary_summary_financial_table' THEN 2 "
            "ELSE 9 END, "
            "CASE COALESCE(m.raw_label, '') "
            "WHEN '기초장부가액' THEN 0 "
            "WHEN '기말장부가액' THEN 1 "
            "WHEN '취득원가' THEN 2 "
            "ELSE 9 END, "
            "CASE WHEN COALESCE(m.column_key, '') IN ('합계', '합_계', '계') THEN 9 ELSE 0 END"
        )

    @staticmethod
    def _table_context_rank(
        context: Dict[str, Any],
        prioritized_table_ids: List[str],
        table_title_terms: List[str],
        required_dimensions: List[str],
    ) -> tuple[int, int, int, int, int]:
        table_id = str(context.get("table_id") or "")
        title = str(context.get("table_title") or "")
        markdown = str(context.get("table_markdown") or "").strip()
        footnotes = str(context.get("footnotes") or "").strip()
        unit = str(context.get("table_unit") or "").strip()
        exact_table_rank = 0
        if table_id in prioritized_table_ids:
            exact_table_rank = len(prioritized_table_ids) - prioritized_table_ids.index(table_id)
        title_match_count = sum(term in title for term in table_title_terms)
        dimension_score = 0
        if "structure" in required_dimensions and markdown:
            dimension_score += 1
        if "unit" in required_dimensions and unit:
            dimension_score += 1
        if "footnote" in required_dimensions and footnotes:
            dimension_score += 1
        return (
            exact_table_rank,
            title_match_count,
            dimension_score,
            SQLTemplateEngine._table_context_content_score(markdown),
            int(bool(markdown)),
            int(bool(footnotes)),
        )

    @staticmethod
    def _table_cell_period_order_expr(interpretation: QueryInterpretation) -> str:
        if interpretation.period:
            return "CASE WHEN 1=1 THEN 0 END"
        return (
            "CASE "
            "WHEN COALESCE(m.period, '') IN ('당기', '당기말', '당기초') THEN 0 "
            "WHEN COALESCE(m.table_title, '') LIKE '%당기%' THEN 0 "
            "WHEN COALESCE(m.period, '') IN ('전기', '전기말', '전기초') THEN 1 "
            "WHEN COALESCE(m.table_title, '') LIKE '%전기%' THEN 1 "
            "ELSE 0 END"
        )

    @staticmethod
    def _table_context_content_score(markdown: str) -> int:
        table_lines = [line.strip() for line in markdown.splitlines() if line.strip().startswith("|")]
        if len(table_lines) <= 2:
            return 0
        body_lines = table_lines[2:]
        numeric_lines = sum(1 for line in body_lines if re.search(r"\d", line))
        header_hint = int(any(token in markdown for token in ("| 과 목 |", "| 구 분 |", "| 회사명 |")))
        return numeric_lines + header_hint

    @staticmethod
    def _dedupe_strs(values: List[Any]) -> List[str]:
        deduped: List[str] = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in deduped:
                deduped.append(text)
        return deduped

    @staticmethod
    def _placeholders(values: List[Any]) -> str:
        return ", ".join("?" for _ in values)

    @staticmethod
    def _build_like_clause(expr: str, values: List[str]) -> str:
        return "(" + " OR ".join(f"{expr} LIKE ?" for _ in values) + ")"

    @staticmethod
    def _like_param(value: str) -> str:
        compact = re.sub(r"\s+", "", value or "")
        compact = compact.replace("_", "")
        return f"%{compact}%"

    @staticmethod
    def _comparison_sql_operator(operator: str) -> str:
        mapping = {
            "lt": "<",
            "lte": "<=",
            "gt": ">",
            "gte": ">=",
        }
        return mapping.get(operator, "<")

    @staticmethod
    def _expand_label_candidates(metric_ids: List[str]) -> List[str]:
        labels: List[str] = []
        for metric_id in metric_ids:
            definition = METRIC_DEFINITIONS.get(metric_id)
            if not definition:
                continue
            for alias in definition.row_label_aliases:
                compact = alias.replace(" ", "")
                if compact not in labels:
                    labels.append(compact)
        return labels

    @staticmethod
    def _expand_column_candidates(metric_ids: List[str]) -> List[str]:
        labels: List[str] = []
        for metric_id in metric_ids:
            definition = METRIC_DEFINITIONS.get(metric_id)
            if not definition:
                continue
            for alias in (*definition.aliases, *definition.row_label_aliases):
                compact = alias.replace(" ", "")
                if compact not in labels:
                    labels.append(compact)
        return labels

    @staticmethod
    def _expand_statement_types(metric_ids: List[str]) -> List[str]:
        statement_types: List[str] = []
        for metric_id in metric_ids:
            definition = METRIC_DEFINITIONS.get(metric_id)
            if not definition:
                continue
            for statement_type in definition.statement_types:
                if statement_type not in statement_types:
                    statement_types.append(statement_type)
        return statement_types
