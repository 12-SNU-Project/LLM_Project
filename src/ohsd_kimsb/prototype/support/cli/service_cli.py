from __future__ import annotations

import argparse
import importlib
import json
import sqlite3
import sys
from pathlib import Path


def _bootstrap_paths() -> tuple[Path, Path]:
    prototype_dir = Path(__file__).resolve().parents[2]
    repo_root = prototype_dir.parents[2]
    sys.path.insert(0, str(repo_root / ".vendor"))
    sys.path.insert(0, str(repo_root / "src"))
    return repo_root, prototype_dir


def _load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def main() -> None:
    repo_root, prototype_dir = _bootstrap_paths()
    workspace_pkg = prototype_dir.parent.name
    base_pkg = f"{workspace_pkg}.prototype"

    SQLTemplateEngine = importlib.import_module(f"{base_pkg}.query.sql_templates").SQLTemplateEngine
    RetrievalFusionEngine = importlib.import_module(f"{base_pkg}.retrieval.fusion").RetrievalFusionEngine
    AuditQAService = importlib.import_module(f"{base_pkg}.service.hybrid_qa").AuditQAService
    PrototypeRuntimeConfig = importlib.import_module(f"{base_pkg}.service.runtime_factory").PrototypeRuntimeConfig
    PrototypeRuntimeFactory = importlib.import_module(f"{base_pkg}.service.runtime_factory").PrototypeRuntimeFactory
    artifact_module = importlib.import_module(f"{base_pkg}.service.artifact_paths")
    default_artifact_paths = artifact_module.default_artifact_paths

    defaults = default_artifact_paths(repo_root)

    parser = argparse.ArgumentParser(description="QA service CLI using prebuilt SQLite + Chroma artifacts")
    parser.add_argument("question", help="사용자 질문")
    parser.add_argument("--manifest-path", default=str(defaults.manifest_path), help="offline ingest manifest 경로")
    parser.add_argument("--db-path", default=None, help="SQLite DB 경로 override")
    parser.add_argument("--chroma-dir", default=None, help="Chroma persist directory override")
    parser.add_argument("--collection-name", default=None, help="Chroma collection 이름 override")
    parser.add_argument("--intent-model", default="qwen3:8b", help="질의 해석용 로컬 LLM")
    parser.add_argument("--answer-model", default="qwen3:8b", help="최종 답변용 로컬 LLM")
    parser.add_argument("--embedding-model", default="qwen3-embedding:8b", help="Chroma 임베딩 모델")
    parser.add_argument("--ollama-base-url", default="http://localhost:11434", help="Ollama base URL")
    parser.add_argument("--force-inmemory", action="store_true", help="Chroma 대신 in-memory vector store 사용")
    parser.add_argument("--strict-runtime", action="store_true", help="LLM/Chroma 실패 시 fallback 없이 종료")
    args = parser.parse_args()

    manifest_path = Path(args.manifest_path)
    if not manifest_path.is_absolute():
        manifest_path = (repo_root / manifest_path).resolve()
    manifest = _load_manifest(manifest_path)
    db_path = Path(args.db_path or manifest.get("db_path") or defaults.sqlite_db_path)
    chroma_dir = Path(args.chroma_dir or manifest.get("chroma_dir") or defaults.chroma_directory)
    if not db_path.is_absolute():
        db_path = (repo_root / db_path).resolve()
    if not chroma_dir.is_absolute():
        chroma_dir = (repo_root / chroma_dir).resolve()
    collection_name = args.collection_name or manifest.get("collection_name") or "audit_chunks"
    embedding_model = args.embedding_model or manifest.get("embedding_model") or "qwen3-embedding:8b"
    ollama_base_url = args.ollama_base_url or manifest.get("ollama_base_url") or "http://localhost:11434"

    if not db_path.exists():
        raise SystemExit(f"SQLite DB를 찾지 못했습니다: {db_path}")

    runtime = PrototypeRuntimeFactory(
        PrototypeRuntimeConfig(
            intent_model=args.intent_model,
            answer_model=args.answer_model,
            embedding_model=embedding_model,
            ollama_base_url=ollama_base_url,
            chroma_collection_name=collection_name,
            chroma_persist_directory=str(chroma_dir),
            prefer_langchain=True,
            prefer_chroma=not args.force_inmemory,
            allow_fallback=not args.strict_runtime,
            reset_chroma_collection=False,
        )
    ).build(parse_results=None, repo_root=repo_root)

    service = AuditQAService(
        sql_engine=SQLTemplateEngine(),
        retrieval_engine=RetrievalFusionEngine(metadata_builder=runtime.metadata_builder),
        answer_composer=runtime.answer_composer,
        vector_store=runtime.vector_store,
        query_interpreter=runtime.query_interpreter,
    )

    with sqlite3.connect(db_path) as conn:
        response = service.answer(args.question, conn)

    print(
        json.dumps(
            {
                "artifact_paths": {
                    "db_path": str(db_path),
                    "chroma_dir": str(chroma_dir),
                    "collection_name": collection_name,
                },
                "runtime_report": runtime.runtime_report,
                "response": response,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
