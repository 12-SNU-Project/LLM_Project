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
            + self._table_cell_order_expr()
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
    def _table_cell_order_expr() -> str:
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
