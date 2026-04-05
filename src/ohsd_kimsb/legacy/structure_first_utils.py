from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from pathlib import Path

ENCODING_CANDIDATES = ("cp949", "euc-kr", "utf-8", "utf-8-sig")

WS_RE = re.compile(r"\s+")
TAG_RE = re.compile(r"<[^>]+>", flags=re.IGNORECASE | re.DOTALL)
YEAR_RE = re.compile(r"(?:19|20)\d{2}")
DATE_RE = re.compile(r"(20\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일")
NOTE_REF_RE = re.compile(r"주석\s*([0-9]+(?:\.[0-9]+)?)")


def now_iso_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def decode_html_file(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    for enc in ENCODING_CANDIDATES:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw.decode("latin1", errors="replace"), "latin1"


def normalize_space(text: str) -> str:
    return WS_RE.sub(" ", text).strip()


def strip_tags(fragment: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.IGNORECASE)
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text).replace("\u00a0", " ")
    return normalize_space(text)


def parse_numeric(value: str) -> float | None:
    token = value.strip().replace(",", "")
    if not token or token in {"-", "—", "N/A"}:
        return None
    negative = False
    if token.startswith("(") and token.endswith(")"):
        token = token[1:-1]
        negative = True
    try:
        parsed = float(token)
    except ValueError:
        return None
    return -parsed if negative else parsed


def is_numeric_like(value: str) -> bool:
    return parse_numeric(value) is not None


def infer_fiscal_year_from_name(file_name: str) -> int | None:
    m = re.search(r"(20\d{2})", file_name)
    return int(m.group(1)) if m else None


def extract_year_candidates(text: str) -> list[int]:
    years = {int(m.group(0)) for m in YEAR_RE.finditer(text)}
    return sorted(y for y in years if 1990 <= y <= 2100)


def extract_note_refs(text: str) -> list[str]:
    refs = {m.group(1) for m in NOTE_REF_RE.finditer(text)}
    return sorted(refs)


def extract_first_date(text: str) -> str | None:
    m = DATE_RE.search(text)
    if not m:
        return None
    year, month, day = m.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def slugify(value: str) -> str:
    cleaned = normalize_space(value).lower()
    cleaned = re.sub(r"[^0-9a-zA-Z가-힣]+", "_", cleaned).strip("_")
    return cleaned[:80] if cleaned else "na"


def clean_header_token(value: str) -> str:
    return normalize_space(value).strip(":")
