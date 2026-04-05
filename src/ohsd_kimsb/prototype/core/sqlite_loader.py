from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List

try:
    from .pipeline import AuditReportPipeline
except ImportError:
    from pipeline import AuditReportPipeline


class SQLiteLoader:
    """Load the lean runtime payload into SQLite."""

    TABLE_ORDER = [
        "filings",
        "metric_facts",
        "text_chunks",
    ]

    def __init__(self, db_path: str, schema_path: str = "rdb_schema_draft.sql"):
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)

    def init_schema(self) -> None:
        schema_sql = self.schema_path.read_text(encoding="utf-8")
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(schema_sql)

    @staticmethod
    def _insert_rows(conn: sqlite3.Connection, table_name: str, rows: Iterable[Dict[str, object]]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        columns = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_sql = ", ".join(columns)
        sql = f"INSERT OR REPLACE INTO {table_name} ({col_sql}) VALUES ({placeholders})"
        values = [tuple(row.get(col) for col in columns) for row in rows]
        conn.executemany(sql, values)
        return len(rows)

    def load_payload(self, payload: Dict[str, List[Dict[str, object]]]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        with sqlite3.connect(self.db_path) as conn:
            for table_name in self.TABLE_ORDER:
                rows = payload.get(table_name, [])
                counts[table_name] = self._insert_rows(conn, table_name, rows)
            conn.commit()
        return counts

    def load_file(self, file_path: str, filing_id: str | None = None, fiscal_year: int | None = None) -> Dict[str, int]:
        pipeline = AuditReportPipeline()
        result = pipeline.parse_file(file_path=file_path, filing_id=filing_id, fiscal_year=fiscal_year)
        payload = pipeline.to_rdb_payload(result)
        return self.load_payload(payload)


if __name__ == "__main__":
    # Example:
    # python sqlite_loader.py /tmp/audit.db /path/to/감사보고서_2024.htm
    import argparse
    import re

    parser = argparse.ArgumentParser(description="Load parsed audit report data into SQLite.")
    parser.add_argument("db_path", help="Target SQLite DB path")
    parser.add_argument("file_path", help="Source audit report HTML path")
    parser.add_argument("--schema", default="rdb_schema_draft.sql", help="Schema SQL path")
    parser.add_argument("--filing-id", default=None, help="Optional filing_id override")
    parser.add_argument("--fiscal-year", type=int, default=None, help="Optional fiscal year override")
    args = parser.parse_args()

    inferred_year = args.fiscal_year
    if inferred_year is None:
        m = re.search(r"(19\d{2}|20\d{2})", Path(args.file_path).name)
        inferred_year = int(m.group(1)) if m else None

    loader = SQLiteLoader(db_path=args.db_path, schema_path=args.schema)
    loader.init_schema()
    counts = loader.load_file(
        file_path=args.file_path,
        filing_id=args.filing_id,
        fiscal_year=inferred_year,
    )

    print("Loaded rows:")
    for name, count in counts.items():
        print(f"- {name}: {count}")
