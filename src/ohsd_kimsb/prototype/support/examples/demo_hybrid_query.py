from __future__ import annotations

import argparse
import importlib
import json
import sqlite3
import sys
import tempfile
from pathlib import Path


def _bootstrap_paths() -> tuple[Path, Path]:
    prototype_dir = Path(__file__).resolve().parents[2]
    repo_root = prototype_dir.parents[2]
    sys.path.insert(0, str(repo_root / ".vendor"))
    sys.path.insert(0, str(repo_root / "src"))
    return repo_root, prototype_dir


def main() -> None:
    repo_root, prototype_dir = _bootstrap_paths()
    workspace_pkg = prototype_dir.parent.name
    base_pkg = f"{workspace_pkg}.prototype"

    AuditReportPipeline = importlib.import_module(f"{base_pkg}.core.pipeline").AuditReportPipeline
    SQLiteLoader = importlib.import_module(f"{base_pkg}.core.sqlite_loader").SQLiteLoader
    SQLTemplateEngine = importlib.import_module(f"{base_pkg}.query.sql_templates").SQLTemplateEngine
    RetrievalFusionEngine = importlib.import_module(f"{base_pkg}.retrieval.fusion").RetrievalFusionEngine
    runtime_module = importlib.import_module(f"{base_pkg}.service.runtime_factory")
    PrototypeRuntimeConfig = runtime_module.PrototypeRuntimeConfig
    PrototypeRuntimeFactory = runtime_module.PrototypeRuntimeFactory

    parser = argparse.ArgumentParser(description="Prototype hybrid SQL + VDB retrieval demo")
    parser.add_argument("question", help="사용자 질문")
    parser.add_argument("--year", type=int, default=None, help="특정 연도 파일만 사용")
    parser.add_argument("--top-k", type=int, default=6, help="vector retrieval top-k")
    parser.add_argument("--intent-model", default="qwen3-8b", help="질의 해석용 로컬 LLM")
    parser.add_argument("--embedding-model", default="qwen3-embedding:8b", help="Chroma 임베딩 모델")
    parser.add_argument("--ollama-base-url", default="http://localhost:11434", help="Ollama base URL")
    parser.add_argument("--chroma-dir", default=None, help="Chroma persist directory")
    parser.add_argument("--force-inmemory", action="store_true", help="Chroma 대신 in-memory vector store 사용")
    parser.add_argument("--reset-chroma", action="store_true", help="Chroma collection 재생성")
    args = parser.parse_args()

    html_files = sorted((repo_root / "data").glob("*.htm"))
    if args.year is not None:
        html_files = [path for path in html_files if str(args.year) in path.name]
    if not html_files:
        raise SystemExit("대상 HTML 파일을 찾지 못했습니다.")

    pipeline = AuditReportPipeline()
    parse_results = []

    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as temp_db:
        db_path = Path(temp_db.name)

    loader = SQLiteLoader(
        db_path=str(db_path),
        schema_path=str(prototype_dir / "core" / "rdb_schema_draft.sql"),
    )
    loader.init_schema()

    for html_file in html_files:
        result = pipeline.parse_file(str(html_file))
        parse_results.append(result)
        loader.load_payload(pipeline.to_rdb_payload(result))

    runtime = PrototypeRuntimeFactory(
        PrototypeRuntimeConfig(
            intent_model=args.intent_model,
            answer_model="qwen3-12b",
            embedding_model=args.embedding_model,
            ollama_base_url=args.ollama_base_url,
            chroma_persist_directory=args.chroma_dir,
            prefer_langchain=True,
            prefer_chroma=not args.force_inmemory,
            allow_fallback=True,
            reset_chroma_collection=args.reset_chroma,
        )
    ).build(parse_results, repo_root=repo_root)

    interpretation = runtime.query_interpreter.interpret(args.question)
    sql_engine = SQLTemplateEngine()
    sql_plan = sql_engine.build(interpretation)
    with sqlite3.connect(db_path) as conn:
        sql_rows = sql_engine.execute(conn, sql_plan)

    fusion_engine = RetrievalFusionEngine(metadata_builder=runtime.metadata_builder)
    bundle = fusion_engine.retrieve(
        question=args.question,
        interpretation=interpretation,
        vector_store=runtime.vector_store,
        sql_rows=sql_rows,
        top_k=args.top_k,
    )

    print(
        json.dumps(
            {
                "runtime_report": runtime.runtime_report,
                "interpretation": interpretation.to_dict(),
                "sql_plan": sql_plan.to_dict() if sql_plan else None,
                "context": fusion_engine.build_generation_context(bundle),
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
