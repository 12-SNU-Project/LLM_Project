from __future__ import annotations

import argparse
import importlib
import json
import sys
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
    runtime_module = importlib.import_module(f"{base_pkg}.service.runtime_factory")
    PrototypeRuntimeConfig = runtime_module.PrototypeRuntimeConfig
    PrototypeRuntimeFactory = runtime_module.PrototypeRuntimeFactory

    parser = argparse.ArgumentParser(description="Prototype embedding/chroma runtime probe")
    parser.add_argument("--year", type=int, default=2024, help="적재할 특정 연도")
    parser.add_argument("--embedding-model", default="qwen3-embedding:8b", help="Chroma 임베딩 모델")
    parser.add_argument("--ollama-base-url", default="http://localhost:11434", help="Ollama base URL")
    parser.add_argument("--chroma-dir", default=None, help="Chroma persist directory")
    parser.add_argument("--query", default="2024년 매출액과 관련 설명", help="샘플 검색 질의")
    args = parser.parse_args()

    html_files = [
        path
        for path in sorted((repo_root / "data").glob("*.htm"))
        if str(args.year) in path.name
    ]
    if not html_files:
        raise SystemExit("대상 HTML 파일을 찾지 못했습니다.")

    pipeline = AuditReportPipeline()
    parse_results = [pipeline.parse_file(str(path)) for path in html_files]

    runtime = PrototypeRuntimeFactory(
        PrototypeRuntimeConfig(
            intent_model="qwen3:8b",
            answer_model="qwen3:8b",
            embedding_model=args.embedding_model,
            ollama_base_url=args.ollama_base_url,
            chroma_persist_directory=args.chroma_dir,
            prefer_langchain=True,
            prefer_chroma=True,
            allow_fallback=True,
        )
    ).build(parse_results, repo_root=repo_root)

    hits = runtime.vector_store.query(args.query, top_k=3, where={}) if hasattr(runtime.vector_store, "query") else []
    print(
        json.dumps(
            {
                "runtime_report": runtime.runtime_report,
                "sample_hits": [
                    {
                        "document_id": hit.document_id,
                        "score": hit.similarity_score,
                        "section_type": hit.metadata.get("section_type"),
                        "page_start": hit.metadata.get("page_start"),
                    }
                    for hit in hits
                ],
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
