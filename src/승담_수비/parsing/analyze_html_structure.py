from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

from bs4 import BeautifulSoup, Tag

from .structure_first_utils import decode_html_file, normalize_space


KEYWORDS = {
    "independent_auditor_report": "\ub3c5\ub9bd\ub41c \uac10\uc0ac\uc778\uc758 \uac10\uc0ac\ubcf4\uace0\uc11c",
    "audit_opinion": "\uac10\uc0ac\uc758\uacac",
    "key_audit_matters": "\ud575\uc2ec\uac10\uc0ac\uc0ac\ud56d",
    "internal_control": "\ub0b4\ubd80\ud68c\uacc4\uad00\ub9ac\uc81c\ub3c4",
    "contingent_liabilities": "\uc6b0\ubc1c\ubd80\ucc44",
    "subsequent_events": "\ubcf4\uace0\uae30\uac04 \ud6c4 \uc0ac\uac74",
}


def _table_rows(table: Tag) -> list[Tag]:
    rows: list[Tag] = []
    for tr in table.find_all("tr"):
        parent = tr.find_parent("table")
        if parent is table:
            rows.append(tr)
    return rows


def analyze_file(path: Path) -> dict[str, object]:
    html_text, encoding = decode_html_file(path)
    soup = BeautifulSoup(html_text, "html.parser")
    body = soup.body if soup.body else soup

    class_counter = Counter()
    for tag in body.find_all(True):
        classes = tag.get("class") or []
        for cls in classes:
            class_counter[str(cls)] += 1

    tables = [t for t in body.find_all("table") if t.find_parent("table") is None]
    table_rowspan = 0
    table_colspan = 0
    multi_header_like = 0
    for table in tables:
        if table.find(attrs={"rowspan": True}):
            table_rowspan += 1
        if table.find(attrs={"colspan": True}):
            table_colspan += 1
        header_like = 0
        for row in _table_rows(table)[:4]:
            if row.find("th") is not None:
                header_like += 1
                continue
            class_names = " ".join(str(x).upper() for x in (row.get("class") or []))
            if "TH" in class_names:
                header_like += 1
        if header_like >= 2:
            multi_header_like += 1

    text = normalize_space(body.get_text(" ", strip=True))
    keyword_hits = {k: len(re.findall(re.escape(v), text)) for k, v in KEYWORDS.items()}

    heading_samples: list[str] = []
    for tag in body.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        heading_samples.append(normalize_space(tag.get_text(" ", strip=True)))
    for tag in body.find_all("p"):
        cls = " ".join(str(x).upper() for x in (tag.get("class") or []))
        if "SECTION-" in cls or "COVER-TITLE" in cls:
            heading_samples.append(normalize_space(tag.get_text(" ", strip=True)))

    year_match = re.search(r"(20\d{2})", path.name)
    return {
        "file": path.name,
        "year": int(year_match.group(1)) if year_match else None,
        "encoding": encoding,
        "table_count": len(tables),
        "paragraph_count": len(body.find_all("p")),
        "h_tag_count": len(body.find_all(re.compile(r"^h[1-6]$"))),
        "page_break_count": len(body.find_all(class_=re.compile(r"\bPGBRK\b", flags=re.IGNORECASE))),
        "table_with_rowspan": table_rowspan,
        "table_with_colspan": table_colspan,
        "table_multi_header_like": multi_header_like,
        "top_classes": class_counter.most_common(10),
        "keyword_hits": keyword_hits,
        "heading_samples": heading_samples[:20],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze audit HTML structure differences by year.")
    parser.add_argument("--input-dir", type=Path, default=Path("src/data"))
    parser.add_argument("--output", type=Path, default=Path("src/extend_code/output/structure_analysis.json"))
    args = parser.parse_args()

    rows = [analyze_file(path) for path in sorted(args.input_dir.glob("*.htm"))]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Analyzed files: {len(rows)}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
