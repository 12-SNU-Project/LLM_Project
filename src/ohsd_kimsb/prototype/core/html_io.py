from __future__ import annotations

from pathlib import Path


ENCODING_CANDIDATES = ("cp949", "euc-kr", "utf-8", "utf-8-sig")


def decode_html_file(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    for enc in ENCODING_CANDIDATES:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw.decode("latin1", errors="replace"), "latin1"
