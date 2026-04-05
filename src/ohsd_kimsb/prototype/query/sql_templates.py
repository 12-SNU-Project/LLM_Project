from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .catalog import METRIC_DEFINITIONS
from .schema import QueryIntent, QueryInterpretation, SQLQueryPlan


class SQLTemplateEngine:
    # Runtime DB keeps one flat fact table, so label matching only touches
    # columns needed for metric lookup.
    LABEL_EXPR = (
        "REPLACE(REPLACE(REPLACE(COALESCE(m.normalized_label, m.raw_label), ' ', ''), char(10), ''), char(13), '')"
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
        statement_types = self._expand_statement_types(interpretation.metric_candidates)

        where_clauses = [
            f"{self.LABEL_EXPR} IN ({self._placeholders(label_candidates)})",
            "m.is_primary_value = 1",
            "m.section_type = 'attached_financial_statements'",
            "COALESCE(m.column_key, '') NOT LIKE '주석%'",
            "COALESCE(m.column_key, '') NOT LIKE '주 %'",
        ]
        params: List[Any] = list(label_candidates)

        if interpretation.year is not None:
            where_clauses.append("f.fiscal_year = ?")
            params.append(interpretation.year)
        elif interpretation.year_range is not None:
            where_clauses.append("f.fiscal_year BETWEEN ? AND ?")
            params.extend(list(interpretation.year_range))

        if statement_types:
            where_clauses.append(f"m.statement_type IN ({self._placeholders(statement_types)})")
            params.extend(statement_types)

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
                "label_candidates": label_candidates,
                "statement_types": statement_types,
            },
        )

    def _build_trend_compare(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        label_candidates = self._expand_label_candidates(interpretation.metric_candidates)
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

        where_clauses = [
            f"{self.LABEL_EXPR} IN ({self._placeholders(label_candidates)})",
            "m.is_primary_value = 1",
            "m.section_type = 'attached_financial_statements'",
            "COALESCE(m.column_key, '') NOT LIKE '주석%'",
            "COALESCE(m.column_key, '') NOT LIKE '주 %'",
            year_filter_sql,
        ]
        if cte:
            params = list(year_params) + list(label_candidates)
        else:
            params = list(label_candidates) + list(year_params)

        if statement_types:
            where_clauses.append(f"m.statement_type IN ({self._placeholders(statement_types)})")
            params.extend(statement_types)

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
                "label_candidates": label_candidates,
                "statement_types": statement_types,
                "year_window": interpretation.year_window,
            },
        )

    @staticmethod
    def _placeholders(values: List[Any]) -> str:
        return ", ".join("?" for _ in values)

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
