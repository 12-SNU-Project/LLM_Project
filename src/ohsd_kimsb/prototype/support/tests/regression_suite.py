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

        _check(result.meta.company_name == "삼성전자주식회사", f"{path.name}: company_name mismatch", failures)
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
    _check(
        runtime.runtime_report.get("vector_backend") in {"chroma", "in_memory"},
        "runtime: vector backend missing",
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
            "question": "재무적으로 위험한가?",
            "expected_intent": "metric_lookup",
            "expect_sql": False,
            "expect_vdb": False,
            "clarification_needed": True,
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
                _check(len(years) >= 3, "trend_compare query: fewer than 3 fiscal years returned", failures)

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
