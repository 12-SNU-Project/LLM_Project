from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

try:
    from ..query.catalog import DERIVED_METRIC_IDS
    from ..query.schema import QueryInterpretation, SQLQueryPlan
except ImportError:
    from query.catalog import DERIVED_METRIC_IDS
    from query.schema import QueryInterpretation, SQLQueryPlan


class RatioAnalysisTool:
    """재무상태표의 총계 값을 이용해 파생 재무비율을 계산한다."""

    LABEL_EXPR = (
        "REPLACE(REPLACE(REPLACE(COALESCE(m.normalized_label, m.raw_label), ' ', ''), char(10), ''), char(13), '')"
    )
    METRIC_LABELS = {
        "equity_ratio": "자기자본비율",
        "debt_ratio": "부채비율",
    }

    def supports(self, interpretation: QueryInterpretation) -> bool:
        return bool(interpretation.metric_candidates) and set(interpretation.metric_candidates).issubset(DERIVED_METRIC_IDS)

    def build_plan(self, interpretation: QueryInterpretation) -> SQLQueryPlan:
        sql, params = self._build_sql(interpretation)
        return SQLQueryPlan(
            template_name="derived_ratio_analysis",
            sql=sql,
            params=params,
            metadata={
                "metric_candidates": interpretation.metric_candidates,
                "year": interpretation.year,
                "year_range": interpretation.year_range,
                "year_window": interpretation.year_window,
            },
        )

    def execute(self, conn: sqlite3.Connection, interpretation: QueryInterpretation) -> List[Dict[str, Any]]:
        plan = self.build_plan(interpretation)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(plan.sql, plan.params).fetchall()
        return [dict(row) for row in rows]

    def _build_sql(self, interpretation: QueryInterpretation) -> tuple[str, List[Any]]:
        params: List[Any] = []
        cte = ""
        year_filter_sql = "f.fiscal_year IS NOT NULL"

        if interpretation.year_range is not None:
            year_filter_sql = "f.fiscal_year BETWEEN ? AND ?"
            params.extend(list(interpretation.year_range))
        elif interpretation.year_window:
            cte = """
WITH target_years AS (
    SELECT DISTINCT fiscal_year
    FROM filings
    WHERE fiscal_year IS NOT NULL
"""
            if interpretation.year is not None:
                cte += "      AND fiscal_year <= ?\n"
                params.append(interpretation.year)
            cte += "    ORDER BY fiscal_year DESC\n    LIMIT ?\n),\n"
            params.append(interpretation.year_window)
            year_filter_sql = "f.fiscal_year IN (SELECT fiscal_year FROM target_years)"
        elif interpretation.year is not None:
            year_filter_sql = "f.fiscal_year = ?"
            params.append(interpretation.year)

        metric_filters = list(interpretation.metric_candidates)
        params.extend(metric_filters)
        with_prefix = cte or "WITH\n"  # CTE가 없는 경우에도 파생 비율 계산용 WITH 절이 필요하다.

        sql = (
            with_prefix
            + """
base AS (
    SELECT
        f.filing_id,
        f.company_name,
        f.fiscal_year,
        """
            + self.LABEL_EXPR
            + """ AS label_key,
        m.value_numeric,
        m.page_start,
        m.page_end
    FROM metric_facts m
    JOIN filings f ON m.filing_id = f.filing_id
    WHERE """
            + year_filter_sql
            + """
      AND COALESCE(m.statement_type, '') = 'statement_of_financial_position'
      AND COALESCE(m.semantic_table_type, '') = 'primary_financial_statement'
      AND COALESCE(m.period, '') = '당기'
      AND """
            + self.LABEL_EXPR
            + """ IN ('자산총계', '부채총계', '자본총계')
),
pivoted AS (
    SELECT
        filing_id,
        company_name,
        fiscal_year,
        MAX(CASE WHEN label_key = '자산총계' THEN value_numeric END) AS total_assets,
        MAX(CASE WHEN label_key = '부채총계' THEN value_numeric END) AS total_liabilities,
        MAX(CASE WHEN label_key = '자본총계' THEN value_numeric END) AS total_equity,
        MIN(page_start) AS page_start,
        MAX(page_end) AS page_end
    FROM base
    GROUP BY filing_id, company_name, fiscal_year
),
derived AS (
    SELECT
        filing_id,
        company_name,
        fiscal_year,
        'equity_ratio' AS metric_id,
        '자기자본비율' AS raw_label,
        ROUND(CASE WHEN total_assets IS NOT NULL AND total_assets != 0 THEN total_equity * 100.0 / total_assets END, 2) AS value_numeric,
        total_assets,
        total_liabilities,
        total_equity,
        page_start,
        page_end
    FROM pivoted
    UNION ALL
    SELECT
        filing_id,
        company_name,
        fiscal_year,
        'debt_ratio' AS metric_id,
        '부채비율' AS raw_label,
        ROUND(CASE WHEN total_equity IS NOT NULL AND total_equity != 0 THEN total_liabilities * 100.0 / total_equity END, 2) AS value_numeric,
        total_assets,
        total_liabilities,
        total_equity,
        page_start,
        page_end
    FROM pivoted
)
SELECT
    filing_id,
    company_name,
    fiscal_year,
    'derived_ratio_' || fiscal_year || '_' || metric_id AS table_id,
    'attached_financial_statements' AS section_type,
    'statement_of_financial_position' AS statement_type,
    'derived_metric' AS table_role,
    'ratio_analysis' AS table_subrole,
    'derived_ratio_metric' AS semantic_table_type,
    '재무비율 계산' AS table_title,
    'percent' AS table_unit,
    page_start,
    page_end,
    metric_id || '_' || fiscal_year AS row_id,
    0 AS row_index,
    raw_label,
    raw_label AS normalized_label,
    NULL AS row_group_label,
    metric_id || '_' || fiscal_year || '_value' AS value_id,
    1 AS col_index,
    '당기' AS column_key,
    '당기' AS period,
    'ratio' AS value_role,
    value_numeric,
    CASE WHEN value_numeric IS NOT NULL THEN printf('%.2f%%', value_numeric) END AS value_raw,
    'percent' AS unit,
    '당기' AS column_header_path,
    1 AS is_primary_value,
    total_assets,
    total_liabilities,
    total_equity
FROM derived
WHERE metric_id IN ("""
            + ", ".join("?" for _ in metric_filters)
            + """)
  AND value_numeric IS NOT NULL
ORDER BY fiscal_year ASC,
         CASE metric_id WHEN 'equity_ratio' THEN 0 WHEN 'debt_ratio' THEN 1 ELSE 9 END
"""
        )
        return sql.strip(), params
