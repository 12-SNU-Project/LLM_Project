from __future__ import annotations

import argparse
import importlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List


def _bootstrap_paths() -> tuple[Path, Path]:
    prototype_dir = Path(__file__).resolve().parents[2]
    repo_root = prototype_dir.parents[2]
    sys.path.insert(0, str(repo_root / ".vendor"))
    sys.path.insert(0, str(repo_root / "src"))
    return repo_root, prototype_dir


def _configure_stdio() -> None:
    # Keep Korean prompt/input stable in Windows consoles when the user runs
    # the interactive CLI directly.
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8")


def _load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _runtime_label(runtime_report: Dict[str, Any], key: str, fallback: str = "-") -> str:
    return str(runtime_report.get(key) or fallback)


def _print_session_banner(runtime_report: dict, db_path: Path, chroma_dir: Path, collection_name: str) -> None:
    print("Audit QA interactive session")
    print(f"- db: {db_path}")
    print(f"- chroma: {chroma_dir}")
    print(f"- collection: {collection_name}")
    print(
        f"- backends: intent={_runtime_label(runtime_report, 'query_interpreter_backend')}, "
        f"answer={_runtime_label(runtime_report, 'answer_backend')}, "
        f"vector={_runtime_label(runtime_report, 'vector_backend')}"
    )
    print(
        f"- models: intent={_runtime_label(runtime_report, 'preferred_intent_model')}, "
        f"answer={_runtime_label(runtime_report, 'preferred_answer_model')}, "
        f"embedding={_runtime_label(runtime_report, 'preferred_embedding_model')}"
    )
    fallbacks = runtime_report.get("fallbacks") or []
    if fallbacks:
        print(f"- fallbacks: {', '.join(map(str, fallbacks))}")
    print("- exit: exit | quit | :q")


def _preview_text(text: str, limit: int = 140) -> str:
    compact = " ".join((text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _print_sql_rows(rows: List[Dict[str, Any]], limit: int = 5) -> None:
    if not rows:
        return
    print("\nSQL evidence")
    for row in rows[:limit]:
        label = row.get("normalized_label") or row.get("raw_label") or "-"
        year = row.get("fiscal_year") or "-"
        value = row.get("value_raw") or row.get("value_numeric") or "-"
        unit = row.get("unit") or ""
        table_title = row.get("table_title") or "-"
        column_key = row.get("column_key") or "-"
        print(f"- {year} | {label} | {value} {unit} | {table_title} | {column_key}")


def _print_vector_hits(hits: List[Dict[str, Any]], limit: int = 3) -> None:
    if not hits:
        return
    print("\nText evidence")
    for hit in hits[:limit]:
        metadata = hit.get("metadata") or {}
        section = metadata.get("section_title") or metadata.get("section_type") or "-"
        page_start = metadata.get("page_start")
        page_end = metadata.get("page_end")
        page_text = f"p.{page_start}" if page_start == page_end or page_end is None else f"p.{page_start}-{page_end}"
        print(f"- {section} | {page_text} | {_preview_text(hit.get('text', ''))}")


def _print_citations(citations: List[Dict[str, Any]], limit: int = 6) -> None:
    if not citations:
        return
    print("\nCitations")
    for citation in citations[:limit]:
        if citation.get("kind") == "text_chunk":
            page_start = citation.get("page_start")
            page_end = citation.get("page_end")
            page_text = f"p.{page_start}" if page_start == page_end or page_end is None else f"p.{page_start}-{page_end}"
            print(f"- TEXT | {citation.get('chunk_id')} | {page_text}")
        else:
            print(
                f"- SQL | {citation.get('table_id')} | "
                f"{citation.get('column_key')} | fy={citation.get('fiscal_year')}"
            )


def _print_pretty_response(question: str, response: Dict[str, Any]) -> None:
    interpretation = response.get("interpretation") or {}
    bundle = response.get("bundle") or {}
    answer = response.get("answer") or {}
    summary = bundle.get("retrieval_summary") or {}

    print("\n" + "=" * 72)
    print(f"Question: {question}")
    print(
        f"Intent: {interpretation.get('intent')} | "
        f"clarification={interpretation.get('clarification_needed')} | "
        f"sql_rows={len(bundle.get('sql_results') or [])} | "
        f"vector_hits={len(bundle.get('vector_hits') or [])}"
    )
    if summary:
        print(f"Retrieval summary: {json.dumps(summary, ensure_ascii=False)}")

    print("\nAnswer")
    print(answer.get("answer_text") or "(empty)")

    _print_sql_rows(bundle.get("sql_results") or [])
    _print_vector_hits(bundle.get("vector_hits") or [])
    _print_citations(answer.get("citations") or bundle.get("citations") or [])
    print("=" * 72)


def main() -> None:
    _configure_stdio()
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

    parser = argparse.ArgumentParser(description="Interactive QA CLI using prebuilt SQLite + Chroma artifacts")
    parser.add_argument("--manifest-path", default=str(defaults.manifest_path), help="offline ingest manifest path")
    parser.add_argument("--db-path", default=None, help="SQLite DB path override")
    parser.add_argument("--chroma-dir", default=None, help="Chroma persist directory override")
    parser.add_argument("--collection-name", default=None, help="Chroma collection name override")
    parser.add_argument("--intent-model", default="qwen3:8b", help="local LLM for query interpretation")
    parser.add_argument("--answer-model", default="qwen3:8b", help="local LLM for final answer generation")
    parser.add_argument("--embedding-model", default="qwen3-embedding:8b", help="embedding model for Chroma")
    parser.add_argument("--ollama-base-url", default="http://localhost:11434", help="Ollama base URL")
    parser.add_argument("--force-inmemory", action="store_true", help="use in-memory vector store instead of Chroma")
    parser.add_argument("--strict-runtime", action="store_true", help="abort instead of falling back on runtime errors")
    parser.add_argument("--json", action="store_true", help="print raw JSON response instead of pretty text")
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
        raise SystemExit(f"SQLite DB not found: {db_path}")

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

    _print_session_banner(runtime.runtime_report, db_path, chroma_dir, collection_name)

    with sqlite3.connect(db_path) as conn:
        while True:
            try:
                question = input("\nquestion> ").strip()
            except EOFError:
                break
            except KeyboardInterrupt:
                print("\nExiting interactive QA session.")
                break

            if not question:
                continue
            if question.lower() in {"exit", "quit", ":q"}:
                break

            response = service.answer(question, conn)
            if args.json:
                print(
                    json.dumps(
                        {
                            "question": question,
                            "response": response,
                        },
                        ensure_ascii=False,
                        indent=2,
                        default=str,
                    )
                )
            else:
                _print_pretty_response(question, response)


if __name__ == "__main__":
    main()
