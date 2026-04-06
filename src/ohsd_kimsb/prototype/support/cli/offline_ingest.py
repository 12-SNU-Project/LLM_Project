from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List


def _bootstrap_paths() -> tuple[Path, Path]:
    prototype_dir = Path(__file__).resolve().parents[2]
    repo_root = prototype_dir.parents[2]
    sys.path.insert(0, str(repo_root / ".vendor"))
    sys.path.insert(0, str(repo_root / "src"))
    return repo_root, prototype_dir


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _configure_runtime_noise() -> None:
    # Apply noise suppression before importing Chroma/Ollama-related modules.
    os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
    os.environ.setdefault("GLOG_minloglevel", "2")
    os.environ.setdefault("ABSL_MIN_LOG_LEVEL", "2")
    os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
    warnings.filterwarnings(
        "ignore",
        message=r"Unable to find acceptable character detection dependency.*",
        category=Warning,
        module=r"requests(\..*)?$",
    )


def main() -> None:
    _configure_runtime_noise()
    repo_root, prototype_dir = _bootstrap_paths()
    workspace_pkg = prototype_dir.parent.name
    base_pkg = f"{workspace_pkg}.prototype"

    AuditReportPipeline = importlib.import_module(f"{base_pkg}.core.pipeline").AuditReportPipeline
    SQLiteLoader = importlib.import_module(f"{base_pkg}.core.sqlite_loader").SQLiteLoader
    PrototypeRuntimeConfig = importlib.import_module(f"{base_pkg}.service.runtime_factory").PrototypeRuntimeConfig
    PrototypeRuntimeFactory = importlib.import_module(f"{base_pkg}.service.runtime_factory").PrototypeRuntimeFactory
    artifact_module = importlib.import_module(f"{base_pkg}.service.artifact_paths")
    default_artifact_paths = artifact_module.default_artifact_paths

    defaults = default_artifact_paths(repo_root)

    parser = argparse.ArgumentParser(description="Offline ingest for audit HTML -> SQLite + Chroma")
    parser.add_argument("--input-dir", default=str(repo_root / "data"), help="감사보고서 HTML 폴더")
    parser.add_argument("--glob", default="*.htm", help="입력 파일 glob")
    parser.add_argument("--db-path", default=str(defaults.sqlite_db_path), help="SQLite DB 출력 경로")
    parser.add_argument("--chroma-dir", default=str(defaults.chroma_directory), help="Chroma persist directory")
    parser.add_argument("--manifest-path", default=str(defaults.manifest_path), help="ingest manifest JSON 경로")
    parser.add_argument("--collection-name", default="audit_chunks", help="Chroma collection 이름")
    parser.add_argument("--embedding-model", default="qwen3-embedding:8b", help="Chroma 임베딩 모델")
    parser.add_argument("--ollama-base-url", default="http://127.0.0.1:11434", help="Ollama base URL")
    parser.add_argument(
        "--embedding-timeout",
        type=int,
        default=120,
        help="개별 Ollama 임베딩 요청 timeout(초)",
    )
    parser.add_argument(
        "--embedding-keep-alive",
        type=int,
        default=300,
        help="Ollama 임베딩 모델 keep-alive(초), 0 이하면 비활성화",
    )
    parser.add_argument(
        "--ollama-num-gpu",
        type=int,
        default=1,
        help="Ollama GPU 사용 수. macOS에서는 1이 Metal GPU 사용, 0이 CPU 강제",
    )
    parser.add_argument(
        "--ollama-num-thread",
        type=int,
        default=0,
        help="Ollama CPU thread 수. 0이면 Ollama 자동 결정",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=32,
        help="Chroma upsert 시 Ollama 임베딩 요청 배치 크기",
    )
    parser.add_argument(
        "--quiet-ingest-progress",
        action="store_true",
        help="Chroma 배치 적재 진행 로그를 출력하지 않음",
    )
    parser.add_argument("--reset-db", action="store_true", help="기존 SQLite DB를 삭제 후 재생성")
    parser.add_argument("--reset-chroma", action="store_true", help="기존 Chroma collection을 삭제 후 재생성")
    parser.add_argument("--strict-runtime", action="store_true", help="Chroma/임베딩 실패 시 fallback 없이 종료")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    html_files = sorted(input_dir.glob(args.glob))
    if not html_files:
        raise SystemExit("대상 HTML 파일을 찾지 못했습니다.")

    db_path = Path(args.db_path)
    chroma_dir = Path(args.chroma_dir)
    manifest_path = Path(args.manifest_path)
    if not db_path.is_absolute():
        db_path = (repo_root / db_path).resolve()
    if not chroma_dir.is_absolute():
        chroma_dir = (repo_root / chroma_dir).resolve()
    if not manifest_path.is_absolute():
        manifest_path = (repo_root / manifest_path).resolve()

    if args.reset_db and db_path.exists():
        db_path.unlink()
    if args.reset_chroma and chroma_dir.exists():
        shutil.rmtree(chroma_dir)

    _ensure_parent(db_path)
    _ensure_parent(manifest_path)
    chroma_dir.mkdir(parents=True, exist_ok=True)

    pipeline = AuditReportPipeline()
    loader = SQLiteLoader(
        db_path=str(db_path),
        schema_path=str(prototype_dir / "core" / "rdb_schema_draft.sql"),
    )
    loader.init_schema()

    parse_results = []
    parse_summary: List[Dict[str, Any]] = []
    for html_file in html_files:
        result = pipeline.parse_file(str(html_file))
        parse_results.append(result)
        load_counts = loader.load_payload(pipeline.to_rdb_payload(result))
        parse_summary.append(
            {
                "file": html_file.name,
                "filing_id": result.meta.filing_id,
                "fiscal_year": result.meta.fiscal_year,
                "blocks": len(result.blocks),
                "sections": len(result.sections),
                "tables": len(result.tables),
                "text_chunks": len(result.text_chunks),
                "loaded_rows": load_counts,
            }
        )

    # This runtime build only persists vector artifacts; it does not run QA.
    runtime = PrototypeRuntimeFactory(
        PrototypeRuntimeConfig(
            intent_model="qwen3:8b",
            answer_model="qwen3:8b",
            embedding_model=args.embedding_model,
            ollama_base_url=args.ollama_base_url,
            chroma_collection_name=args.collection_name,
            chroma_persist_directory=str(chroma_dir),
            prefer_langchain=True,
            prefer_chroma=True,
            allow_fallback=not args.strict_runtime,
            reset_chroma_collection=args.reset_chroma,
            embedding_upsert_batch_size=max(1, args.embedding_batch_size),
            log_vector_ingest_progress=not args.quiet_ingest_progress,
            embedding_timeout=max(1, args.embedding_timeout),
            embedding_keep_alive=(args.embedding_keep_alive if args.embedding_keep_alive > 0 else None),
            ollama_num_gpu=(args.ollama_num_gpu if args.ollama_num_gpu >= 0 else None),
            ollama_num_thread=(args.ollama_num_thread if args.ollama_num_thread > 0 else None),
        )
    ).build(parse_results=parse_results, repo_root=repo_root)

    manifest = {
        "workspace_package": workspace_pkg,
        "input_dir": str(input_dir),
        "glob": args.glob,
        "files_checked": len(html_files),
        "db_path": str(db_path),
        "chroma_dir": str(chroma_dir),
        "collection_name": args.collection_name,
        "embedding_model": args.embedding_model,
        "embedding_batch_size": max(1, args.embedding_batch_size),
        "embedding_timeout": max(1, args.embedding_timeout),
        "embedding_keep_alive": (args.embedding_keep_alive if args.embedding_keep_alive > 0 else None),
        "ollama_num_gpu": (args.ollama_num_gpu if args.ollama_num_gpu >= 0 else None),
        "ollama_num_thread": (args.ollama_num_thread if args.ollama_num_thread > 0 else None),
        "ollama_base_url": args.ollama_base_url,
        "runtime_report": runtime.runtime_report,
        "parse_summary": parse_summary,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
