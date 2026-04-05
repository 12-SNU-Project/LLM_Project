from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional

from .catalog import METRIC_DEFINITIONS
from .schema import QueryIntent, QueryInterpretation, SQLQueryPlan


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
    m.table_title,
    m.table_unit,
    m.page_start,
    m.page_end,
    m.row_id,
    m.row_index,
    m.raw_label,
    m.normalized_label,
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
        if not interpretation.need_sql or not interpretation.metric_candidates:
            return None

        if interpretation.intent in {QueryIntent.METRIC_LOOKUP, QueryIntent.METRIC_WITH_EXPLANATION}:
            return self._build_metric_lookup(interpretation)
        if interpretation.intent == QueryIntent.TREND_COMPARE:
            return self._build_trend_compare(interpretation)
        return None

    def execute(self, conn: sqlite3.Connection, plan: Optional[SQLQueryPlan]) -> List[Dict[str, Any]]:
        if plan is None:
            return []
        conn.row_factory = sqlite3.Row
        rows = conn.execute(plan.sql, plan.params).fetchall()
        return [dict(row) for row in rows]

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

        sql = (
            self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY CASE WHEN m.table_role = 'financial_table' THEN 0 ELSE 1 END, "
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

        if cte:
            params = list(year_params) + metric_params
        else:
            params = metric_params + list(year_params)

        sql = (
            cte
            + self.BASE_SELECT
            + "\nWHERE "
            + "\n  AND ".join(where_clauses)
            + "\nORDER BY CASE WHEN m.table_role = 'financial_table' THEN 0 ELSE 1 END, "
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

    def _build_metric_predicates(
        self,
        interpretation: QueryInterpretation,
        label_candidates: List[str],
        column_candidates: List[str],
        statement_types: List[str],
    ) -> tuple[List[str], List[Any]]:
        where_clauses = [
            "m.is_primary_value = 1",
            "COALESCE(m.column_key, '') NOT LIKE '二쇱꽍%'",
            "COALESCE(m.column_key, '') NOT LIKE '二?%'",
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
