from __future__ import annotations

import argparse
from pathlib import Path

from .structure_first_parser import StructureFirstAuditReportParser


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Structure-first parser for Samsung audit-report HTML files (2014-2024)."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("src/data"),
        help="Directory containing .htm/.html files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("src/extend_code/output"),
        help="Directory to write JSON/SQLite/Markdown outputs.",
    )
    parser.add_argument(
        "--json-name",
        type=str,
        default="documents_ir.json",
        help="Output JSON filename.",
    )
    parser.add_argument(
        "--sqlite-name",
        type=str,
        default="financial_rag.db",
        help="Output SQLite filename.",
    )
    parser.add_argument(
        "--skip-sqlite",
        action="store_true",
        help="Skip writing SQLite DB.",
    )
    parser.add_argument(
        "--skip-markdown",
        action="store_true",
        help="Skip writing markdown review files.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = StructureFirstAuditReportParser()
    documents = engine.parse_directory(input_dir)
    if not documents:
        raise SystemExit(f"No html files found in: {input_dir}")

    json_path = output_dir / args.json_name
    engine.write_documents_json(documents, json_path)

    if not args.skip_sqlite:
        sqlite_path = output_dir / args.sqlite_name
        engine.write_sqlite(documents, sqlite_path)

    if not args.skip_markdown:
        markdown_dir = output_dir / "markdown_review"
        engine.write_markdown_review(documents, markdown_dir)

    print(f"Parsed documents: {len(documents)}")
    print(f"JSON: {json_path}")
    if not args.skip_sqlite:
        print(f"SQLite: {output_dir / args.sqlite_name}")
    if not args.skip_markdown:
        print(f"Markdown dir: {output_dir / 'markdown_review'}")


if __name__ == "__main__":
    main()
