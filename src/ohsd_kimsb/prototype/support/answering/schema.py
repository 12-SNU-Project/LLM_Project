from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class GeneratedAnswer:
    answer_text: str
    citations: List[Dict[str, Any]] = field(default_factory=list)
    used_sql_rows: int = 0
    used_text_chunks: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
