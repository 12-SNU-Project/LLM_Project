from __future__ import annotations

import importlib
import importlib.util
import json
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


def _bootstrap_paths() -> tuple[Path, Path]:
    prototype_dir = Path(__file__).resolve().parents[2]
    repo_root = prototype_dir.parents[2]
    sys.path.insert(0, str(repo_root / ".vendor"))
    sys.path.insert(0, str(repo_root / "src"))
    return repo_root, prototype_dir


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)


def run() -> Dict[str, Any]:
    repo_root, prototype_dir = _bootstrap_paths()
    workspace_pkg = prototype_dir.parent.name
    base_pkg = f"{workspace_pkg}.prototype"

    AuditReportPipeline = importlib.import_module(f"{base_pkg}.core.pipeline").AuditReportPipeline
    SQLiteLoader = importlib.import_module(f"{base_pkg}.core.sqlite_loader").SQLiteLoader
    SQLTemplateEngine = importlib.import_module(f"{base_pkg}.query.sql_templates").SQLTemplateEngine
    QueryInterpreter = importlib.import_module(f"{base_pkg}.query.interpreter").QueryInterpreter
    RetrievalFusionEngine = importlib.import_module(f"{base_pkg}.retrieval.fusion").RetrievalFusionEngine
    AuditQAService = importlib.import_module(f"{base_pkg}.service.hybrid_qa").AuditQAService
    runtime_module = importlib.import_module(f"{base_pkg}.service.runtime_factory")
    PrototypeRuntimeConfig = runtime_module.PrototypeRuntimeConfig
    PrototypeRuntimeFactory = runtime_module.PrototypeRuntimeFactory

    data_files = sorted((repo_root / "data").glob("*.htm"))
    pipeline = AuditReportPipeline()
    parse_results = []
    parse_summary: List[Dict[str, Any]] = []
    failures: List[str] = []

    for path in data_files:
        result = pipeline.parse_file(str(path))
        parse_results.append(result)
        year = result.meta.fiscal_year
        section_counts = Counter(section.section_type for section in result.sections)
        max_chunk = max((len(chunk.text) for chunk in result.text_chunks), default=0)

        _check(
            bool(result.meta.company_name) and "삼성전자" in result.meta.company_name,
            f"{path.name}: company_name mismatch",
            failures,
        )
        _check(year is not None and str(year) in path.name, f"{path.name}: fiscal_year extraction mismatch", failures)
        _check(result.meta.auditor_name is not None, f"{path.name}: auditor_name missing", failures)
        _check(result.meta.auditor_report_date is not None, f"{path.name}: auditor_report_date missing", failures)
        _check(all(block.page_index is not None for block in result.blocks), f"{path.name}: block page_index missing", failures)
        _check(all(table.section_id for table in result.tables), f"{path.name}: table section linkage missing", failures)
        _check(all(table.page_start is not None for table in result.tables), f"{path.name}: table page_start missing", failures)
        _check(all(chunk.page_start is not None for chunk in result.text_chunks), f"{path.name}: chunk page_start missing", failures)
        _check(max_chunk <= 1200, f"{path.name}: chunk length exceeded max_chars ({max_chunk})", failures)
        _check(section_counts.get("audit_opinion", 0) == 1, f"{path.name}: audit_opinion count != 1", failures)
        _check(
            section_counts.get("management_and_governance_responsibility", 0) == 1,
            f"{path.name}: management responsibility count != 1",
            failures,
        )
        _check(
            section_counts.get("auditor_responsibility", 0) == 1,
            f"{path.name}: auditor responsibility count != 1",
            failures,
        )
        if year is not None and year >= 2018:
            _check(section_counts.get("key_audit_matters", 0) == 1, f"{path.name}: key_audit_matters count != 1", failures)
        if year is not None and year >= 2022:
            _check(
                section_counts.get("internal_control_audit_report", 0) == 1,
                f"{path.name}: internal_control_audit_report count != 1",
                failures,
            )
        if year == 2019:
            has_summary_continuation = any(
                table.semantic_table_type == "subsidiary_summary_financial_table"
                and table.title == "2) 전기"
                and any("Samsung Semiconductor, Inc." in (row.raw_label or "") for row in table.rows)
                for table in result.tables
            )
            _check(has_summary_continuation, f"{path.name}: summary continuation table semantic mismatch", failures)

        parse_summary.append(
            {
                "file": path.name,
                "fiscal_year": year,
                "blocks": len(result.blocks),
                "sections": len(result.sections),
                "tables": len(result.tables),
                "text_chunks": len(result.text_chunks),
                "max_chunk": max_chunk,
                "financial_tables": sum(1 for table in result.tables if table.table_role == "financial_table"),
                "internal_control_tables": sum(1 for table in result.tables if table.table_role == "internal_control_table"),
            }
        )

    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as temp_db:
        db_path = Path(temp_db.name)

    loader = SQLiteLoader(
        db_path=str(db_path),
        schema_path=str(prototype_dir / "core" / "rdb_schema_draft.sql"),
    )
    loader.init_schema()
    for result in parse_results:
        loader.load_payload(pipeline.to_rdb_payload(result))

    runtime = PrototypeRuntimeFactory(
        PrototypeRuntimeConfig(
            intent_model="qwen3:8b",
            answer_model="qwen3:8b",
            embedding_model="qwen3-embedding:8b",
            prefer_langchain=True,
            prefer_chroma=True,
            allow_fallback=True,
        )
    ).build(parse_results, repo_root=repo_root)

    _check(runtime.runtime_report.get("preferred_intent_model") == "qwen3:8b", "runtime: intent model default mismatch", failures)
    _check(runtime.runtime_report.get("preferred_answer_model") == "qwen3:8b", "runtime: answer model default mismatch", failures)
    _check(runtime.runtime_report.get("preferred_embedding_model") == "qwen3-embedding:8b", "runtime: embedding model default mismatch", failures)
    _check(runtime.runtime_report.get("vector_backend") in {"chroma", "in_memory"}, "runtime: vector backend missing", failures)

    drift_question = "2023년도 매출액과 2022년도 매출액을 비교해줘."
    drift_interpretation = QueryInterpreter().interpret(
        drift_question,
        llm_output={
            "intent": "trend_compare",
            "metric_candidates": ["revenue"],
            "row_label_filters": ["2023", "2022"],
            "year_range": [2022, 2023],
            "need_sql": True,
            "need_vdb": False,
        },
    )
    _check(
        not drift_interpretation.row_label_filters,
        "runtime: year-like row_label_filters should be sanitized",
        failures,
    )
    _check(
        drift_interpretation.year_range == (2022, 2023),
        "runtime: year_range should remain after year anchor sanitization",
        failures,
    )

    sql_engine = SQLTemplateEngine()
    retrieval_engine = RetrievalFusionEngine(metadata_builder=runtime.metadata_builder)
    service = AuditQAService(
        sql_engine=sql_engine,
        retrieval_engine=retrieval_engine,
        answer_composer=runtime.answer_composer,
        vector_store=runtime.vector_store,
        query_interpreter=runtime.query_interpreter,
    )

    query_cases = [
        {
            "question": "2024년 매출액이 얼마야?",
            "expected_intent": "metric_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_value_raw": "209,052,241",
            "expect_period": "당기",
        },
        {
            "question": "2023년 매출액이 얼마야?",
            "expected_intent": "metric_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_value_raw": "170,374,090",
            "expect_period": "당기",
        },
        {
            "question": "2024년 감사의견이 뭐야?",
            "expected_intent": "text_explanation",
            "expect_sql": False,
            "expect_vdb": True,
            "clarification_needed": False,
        },
        {
            "question": "2024년 매출액과 관련 설명을 알려줘",
            "expected_intent": "metric_with_explanation",
            "expect_sql": True,
            "expect_vdb": True,
            "clarification_needed": False,
        },
        {
            "question": "최근 3년 매출 추이를 보여줘",
            "expected_intent": "trend_compare",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
        },
        {
            "question": "2023년도와 2022년도 매출액을 비교해줘.",
            "expected_intent": "trend_compare",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_label_fragment": "매 출 액",
            "expect_values_raw": ["170,374,090", "211,867,483"],
            "expect_min_sql_rows": 2,
            "expect_min_fiscal_years": 2,
        },
        {
            "question": "지난 10년간 재무구조의 건전성 추이를 분석하기 위해, 자기자본비율 및 부채비율의 추이를 알려줘",
            "expected_intent": "trend_compare",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_labels": ["자기자본비율", "부채비율"],
            "expect_semantic_table_type": "derived_ratio_metric",
            "expect_min_sql_rows": 10,
        },
        {
            "question": "2024년 자기자본비율이 얼마야?",
            "expected_intent": "metric_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_label_fragment": "자기자본비율",
            "expect_semantic_table_type": "derived_ratio_metric",
            "expect_value_raw": "72.75%",
        },
        {
            "question": "재무적으로 위험한가?",
            "expected_intent": "metric_lookup",
            "expect_sql": False,
            "expect_vdb": False,
            "clarification_needed": True,
        },
        {
            "question": "이 회사는 잘될 회사야? 아니면 망할 회사야?",
            "expected_intent": "metric_lookup",
            "expect_sql": False,
            "expect_vdb": False,
            "clarification_needed": True,
        },
        {
            "question": "2024년 Samsung Semiconductor, Inc. (SSI) 종속기업과의 revenue는 얼마야?",
            "expected_intent": "metric_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_label_fragment": "Samsung Semiconductor, Inc. (SSI)",
            "expect_column_key_fragment": "매출",
            "expect_semantic_table_type": "related_party_transaction_table",
        },
        {
            "question": "2014년 전기의 개발비 기초장부가액이 얼마야?",
            "expected_intent": "table_cell_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_label_fragment": "기초장부가액",
            "expect_column_key_fragment": "개발비",
            "expect_semantic_table_type": "intangible_asset_rollforward_table",
            "expect_value_raw": "602,274",
            "expect_table_title_fragment": "전기",
        },
        {
            "question": "2014년 보험수리적 가정에 대해 알려줘(당기말의 할인율) 표를 참고해서 정확한 수치를 알려줘.",
            "expected_intent": "table_cell_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_label_fragment": "할인율",
            "expect_semantic_table_type": "actuarial_assumption_table",
            "expect_value_raw": "4.4%",
            "expect_period": "당기말",
        },
        {
            "question": "2014년 당기말에는 할인율과 임금상승률을 몇으로 가정하고 작성된거야?",
            "expected_intent": "table_cell_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_row_labels": ["할인율", "미래임금상승률"],
            "expect_semantic_table_type": "actuarial_assumption_table",
            "expect_values_raw": ["4.4%", "6.6%"],
            "expect_period": "당기말",
        },
        {
            "question": "2014년 유형자산에 어떤 종류의 자산들이 포함되어 있어?",
            "expected_intent": "text_explanation",
            "expect_sql": True,
            "expect_vdb": True,
            "clarification_needed": False,
            "expect_column_key_fragment": "토지",
            "expect_semantic_table_type": "property_plant_equipment_rollforward_table",
            "expect_table_title_fragment": "유형자산",
        },
        {
            "question": "2024년 지분율이 70% 미만인 종속기업들을 리스트업해",
            "expected_intent": "comparison_list_lookup",
            "expect_sql": True,
            "expect_vdb": False,
            "clarification_needed": False,
            "expect_column_key_fragment": "지분율",
            "expect_semantic_table_type": "subsidiary_status_table",
            "expect_row_label_fragment": "Samsung Lennox HVAC North America, LLC",
        },
    ]

    query_summary: List[Dict[str, Any]] = []
    with sqlite3.connect(db_path) as conn:
        for case in query_cases:
            response = service.answer(case["question"], conn)
            interpretation = response["interpretation"]
            bundle = response["bundle"]
            answer = response["answer"]

            _check(
                interpretation["intent"] == case["expected_intent"],
                f"query `{case['question']}`: intent mismatch -> {interpretation['intent']}",
                failures,
            )
            _check(
                interpretation["clarification_needed"] == case["clarification_needed"],
                f"query `{case['question']}`: clarification flag mismatch",
                failures,
            )

            if case["expect_sql"]:
                _check(bool(bundle["sql_results"]), f"query `{case['question']}`: sql_results empty", failures)
            else:
                _check(not bundle["sql_results"], f"query `{case['question']}`: sql_results should be empty", failures)

            expected_min_sql_rows = case.get("expect_min_sql_rows")
            if expected_min_sql_rows is not None:
                _check(
                    len(bundle["sql_results"]) >= expected_min_sql_rows,
                    f"query `{case['question']}`: sql_results fewer than expected minimum",
                    failures,
                )

            if case["expect_vdb"]:
                _check(bool(bundle["vector_hits"]), f"query `{case['question']}`: vector_hits empty", failures)
                _check(
                    all(hit["metadata"].get("page_start") is not None for hit in bundle["vector_hits"]),
                    f"query `{case['question']}`: vector hit page metadata missing",
                    failures,
                )
            else:
                _check(not bundle["vector_hits"], f"query `{case['question']}`: vector_hits should be empty", failures)

            if interpretation["intent"] == "trend_compare" and not interpretation["clarification_needed"]:
                years = {row.get("fiscal_year") for row in bundle["sql_results"] if row.get("fiscal_year") is not None}
                _check(
                    len(years) >= int(case.get("expect_min_fiscal_years", 3)),
                    "trend_compare query: fewer than expected fiscal years returned",
                    failures,
                )

            if interpretation["intent"] == "comparison_list_lookup" and not interpretation["clarification_needed"]:
                _check(
                    all(
                        row.get("value_numeric") is not None and float(row["value_numeric"]) < 70
                        for row in bundle["sql_results"]
                    ),
                    f"query `{case['question']}`: comparison threshold mismatch",
                    failures,
                )
                if interpretation.get("entity_scope") == "subsidiary":
                    _check(
                        all((row.get("company_kind") or "") == "subsidiary" for row in bundle["sql_results"]),
                        f"query `{case['question']}`: non-subsidiary rows leaked into subsidiary list",
                        failures,
                    )

            expected_row_label_fragment = case.get("expect_row_label_fragment")
            if expected_row_label_fragment:
                _check(
                    any(expected_row_label_fragment in (row.get("raw_label") or "") for row in bundle["sql_results"]),
                    f"query `{case['question']}`: expected row label fragment missing",
                    failures,
                )

            expected_row_labels = case.get("expect_row_labels")
            if expected_row_labels:
                for expected_row_label in expected_row_labels:
                    _check(
                        any(expected_row_label in (row.get("raw_label") or "") for row in bundle["sql_results"]),
                        f"query `{case['question']}`: expected row label `{expected_row_label}` missing",
                        failures,
                    )

            expected_column_key_fragment = case.get("expect_column_key_fragment")
            if expected_column_key_fragment:
                _check(
                    any(expected_column_key_fragment in (row.get("column_key") or "") for row in bundle["sql_results"]),
                    f"query `{case['question']}`: expected column key fragment missing",
                    failures,
                )

            expected_semantic_table_type = case.get("expect_semantic_table_type")
            if expected_semantic_table_type:
                _check(
                    any(row.get("semantic_table_type") == expected_semantic_table_type for row in bundle["sql_results"]),
                    f"query `{case['question']}`: expected semantic table type missing",
                    failures,
                )

            expected_value_raw = case.get("expect_value_raw")
            if expected_value_raw:
                _check(
                    any((row.get("value_raw") or "") == expected_value_raw for row in bundle["sql_results"]),
                    f"query `{case['question']}`: expected value_raw missing",
                    failures,
                )

            expected_values_raw = case.get("expect_values_raw")
            if expected_values_raw:
                for expected_value in expected_values_raw:
                    _check(
                        any((row.get("value_raw") or "") == expected_value for row in bundle["sql_results"]),
                        f"query `{case['question']}`: expected value_raw `{expected_value}` missing",
                        failures,
                    )

            expected_period = case.get("expect_period")
            if expected_period:
                _check(
                    any((row.get("period") or "") == expected_period for row in bundle["sql_results"]),
                    f"query `{case['question']}`: expected period missing",
                    failures,
                )

            expected_table_title_fragment = case.get("expect_table_title_fragment")
            if expected_table_title_fragment:
                _check(
                    any(expected_table_title_fragment in (row.get("table_title") or "") for row in bundle["sql_results"]),
                    f"query `{case['question']}`: expected table title fragment missing",
                    failures,
                )

            if interpretation["clarification_needed"]:
                _check(
                    answer["metadata"].get("clarification_needed") is True,
                    f"query `{case['question']}`: clarification answer metadata missing",
                    failures,
                )
            else:
                _check(
                    len(answer["citations"]) == len(bundle["citations"]),
                    f"query `{case['question']}`: answer citations mismatch",
                    failures,
                )

            query_summary.append(
                {
                    "question": case["question"],
                    "intent": interpretation["intent"],
                    "clarification_needed": interpretation["clarification_needed"],
                    "sql_rows": len(bundle["sql_results"]),
                    "vector_hits": len(bundle["vector_hits"]),
                    "citations": len(bundle["citations"]),
                    "retrieval_summary": bundle.get("retrieval_summary", {}),
                }
            )

    subsequent_event_chunks = [
        chunk
        for result in parse_results
        if result.meta.fiscal_year == 2024
        for chunk in result.text_chunks
        if chunk.section_type == "subsequent_events"
    ]
    _check(bool(subsequent_event_chunks), "2024 subsequent_events chunk missing", failures)
    _check(
        any(len((chunk.text or "").strip()) > len("31. 보고기간 후 사건") for chunk in subsequent_event_chunks),
        "2024 subsequent_events chunk lost body text",
        failures,
    )

    langchain_available = {
        module: bool(importlib.util.find_spec(module))
        for module in ("langchain_core", "langchain_ollama", "chromadb")
    }

    return {
        "ok": not failures,
        "failures": failures,
        "files_checked": len(data_files),
        "parse_summary": parse_summary,
        "query_summary": query_summary,
        "runtime_report": runtime.runtime_report,
        "langchain_available": langchain_available,
    }


if __name__ == "__main__":
    summary = run()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary["ok"]:
        raise SystemExit(1)
