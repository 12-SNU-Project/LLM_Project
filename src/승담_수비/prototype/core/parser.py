from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup, FeatureNotFound, Tag

try:
    from .models import Block, DocumentMeta, Section
except ImportError:
    from models import Block, DocumentMeta, Section

try:
    from ..utils.structure_first_utils import decode_html_file
except ImportError:
    from utils.structure_first_utils import decode_html_file


def _rule(
    pattern: str,
    section_type: str,
    level: str,
    priority: int,
    parent_types: Optional[Sequence[str]] = None,
) -> Dict[str, object]:
    return {
        "pattern": re.compile(pattern),
        "section_type": section_type,
        "level": level,
        "priority": priority,
        "parent_types": set(parent_types) if parent_types else None,
    }


SECTION_RULES: List[Dict[str, object]] = [
    _rule(r"^독립된감사인의감사보고서$", "independent_auditor_report", "top", 100),
    _rule(r"^\(첨부\)?재무제표$", "attached_financial_statements", "top", 100),
    _rule(r"^주석$", "notes", "top", 100),
    _rule(r"^내부회계관리제도감사또는검토의견$", "internal_control_opinion", "top", 100),
    _rule(r"^외부감사실시내용$", "external_audit_activity", "top", 100),
    _rule(r"^감사의견$", "audit_opinion", "sub", 95, ["independent_auditor_report"]),
    _rule(r"^핵심감사사항$", "key_audit_matters", "sub", 95, ["independent_auditor_report"]),
    _rule(r"^감사의견근거$", "audit_opinion_basis", "sub", 90, ["independent_auditor_report"]),
    _rule(
        r"^재무제표에대한경영진과지배기구의책임$",
        "management_and_governance_responsibility",
        "sub",
        85,
        ["independent_auditor_report"],
    ),
    _rule(
        r"^재무제표에대한경영진의책임$",
        "management_and_governance_responsibility",
        "sub",
        85,
        ["independent_auditor_report"],
    ),
    _rule(
        r"^재무제표감사에대한감사인의책임$",
        "auditor_responsibility",
        "sub",
        85,
        ["independent_auditor_report"],
    ),
    _rule(
        r"^감사인의책임$",
        "auditor_responsibility",
        "sub",
        85,
        ["independent_auditor_report"],
    ),
    _rule(r"^기타사항$", "other_matters", "sub", 80, ["independent_auditor_report"]),
    _rule(
        r"^독립된감사인의내부회계관리제도감사보고서$",
        "internal_control_audit_report",
        "sub",
        95,
        ["internal_control_opinion"],
    ),
    _rule(
        r"^내부회계관리제도에대한감사의견$",
        "internal_control_audit_opinion",
        "sub",
        95,
        ["internal_control_opinion"],
    ),
    _rule(
        r"^내부회계관리제도감사의견근거$",
        "internal_control_audit_basis",
        "sub",
        95,
        ["internal_control_opinion"],
    ),
    _rule(r"^\d+\.감사대상업무$", "external_audit_target_work", "sub", 90, ["external_audit_activity"]),
    _rule(
        r"^\d+\.감사참여자구분별인원수및감사시간$",
        "external_audit_hours",
        "sub",
        90,
        ["external_audit_activity"],
    ),
    _rule(r"^\d+\.주요감사실시내용$", "external_audit_major_content", "sub", 90, ["external_audit_activity"]),
    _rule(
        r"^\d+\.감사\(감사위원회\)와의커뮤니케이션$",
        "external_audit_communication",
        "sub",
        90,
        ["external_audit_activity"],
    ),
    _rule(
        r"^\d+(?:\.\d+)*\.?우발부채와약정사항:?$",
        "contingent_liabilities_and_commitments",
        "sub",
        88,
        ["notes", "attached_financial_statements"],
    ),
    _rule(
        r"^\d+(?:\.\d+)*\.?보고기간후사건:?$",
        "subsequent_events",
        "sub",
        88,
        ["notes", "attached_financial_statements"],
    ),
    _rule(r"^\d+(?:\.\d+)*\..{1,80}$", "note_section", "sub", 20, ["notes"]),
]

SECTION_TITLE_PATTERNS = [rule["pattern"] for rule in SECTION_RULES]

INLINE_PREFIX_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"^(?P<title>감사의견)\s+(?P<body>.+)$"),
    re.compile(r"^(?P<title>핵심감사사항)\s+(?P<body>.+)$"),
    re.compile(r"^(?P<title>감사의견근거)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>재무제표에 대한 경영진과 지배기구의 책임)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>재무제표감사에 대한 감사인의 책임)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>내부회계관리제도 감사 또는 검토의견)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>독립된 감사인의 내부회계관리제도 감사보고서)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>내부회계관리제도에 대한 감사의견)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>내부회계관리제도 감사의견근거)\s*(?P<body>.+)$"),
    re.compile(r"^(?P<title>\d+\.\s*감사대상업무)\s*(?P<body>.*)$"),
    re.compile(r"^(?P<title>\d+\.\s*감사참여자 구분별 인원수 및 감사시간)\s*(?P<body>.*)$"),
    re.compile(r"^(?P<title>\d+\.\s*주요 감사실시내용)\s*(?P<body>.*)$"),
    re.compile(r"^(?P<title>\d+\.\s*감사\(감사위원회\)와의 커뮤니케이션)\s*(?P<body>.*)$"),
    re.compile(r"^(?P<title>\d+(?:\.\d+)*\.?\s*우발부채와 약정사항[:：]?)\s*(?P<body>.*)$"),
    re.compile(r"^(?P<title>\d+(?:\.\d+)*\.?\s*보고기간 후 사건[:：]?)\s*(?P<body>.*)$"),
]

EARLY_INLINE_HEADING = re.compile(
    r"^(?P<prefix>.{1,80}?)(?P<title>감사의견|감사의견근거|핵심감사사항)\s+(?P<body>.+)$"
)

INNER_SPLIT_TITLES = [
    "감사의견",
    "감사의견근거",
    "핵심감사사항",
    "재무제표에 대한 경영진과 지배기구의 책임",
    "재무제표에 대한 경영진의 책임",
    "재무제표감사에 대한 감사인의 책임",
    "감사인의 책임",
    "기타사항",
    "독립된 감사인의 내부회계관리제도 감사보고서",
    "내부회계관리제도에 대한 감사의견",
    "내부회계관리제도 감사의견근거",
    "내부회계관리제도에 대한 경영진과 지배기구의 책임",
    "내부회계관리제도감사에 대한 감사인의 책임",
    "내부회계관리제도의 정의와 고유한계",
    "내부회계관리제도의 고유한계",
]

MERGEABLE_SUBSECTION_TYPES = {
    "audit_opinion",
    "audit_opinion_basis",
    "key_audit_matters",
    "management_and_governance_responsibility",
    "auditor_responsibility",
    "other_matters",
    "internal_control_audit_report",
    "internal_control_audit_opinion",
    "internal_control_audit_basis",
}


class AuditReportParser:
    def __init__(
        self,
        html_content: str,
        parser_backends: Optional[List[str]] = None,
        section_mode: str = "auto",
    ) -> None:
        self.html_content = html_content
        self.parser_backends = parser_backends or ["lxml", "html5lib", "html.parser"]
        self.soup, self.parser_backend = self._make_soup(html_content)
        self.blocks: List[Block] = []
        self.sections: List[Section] = []
        self.document_meta: Optional[DocumentMeta] = None
        self.section_mode = section_mode

    def _make_soup(self, html_content: str) -> Tuple[BeautifulSoup, str]:
        for backend in self.parser_backends:
            try:
                return BeautifulSoup(html_content, backend), backend
            except FeatureNotFound:
                continue
        return BeautifulSoup(html_content, "html.parser"), "html.parser"

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()

    @classmethod
    def _compact_text(cls, text: str) -> str:
        return re.sub(r"\s+", "", cls._normalize_text(text)).strip(" :：.")

    @staticmethod
    def _extract_class_names(element: Tag) -> List[str]:
        classes = element.get("class", [])
        if isinstance(classes, str):
            classes = [classes]
        return [cls.lower() for cls in classes]

    def _get_dom_path(self, element: Tag) -> str:
        nodes: List[str] = []
        cursor: Optional[Tag] = element
        while cursor and isinstance(cursor, Tag):
            same_tag_left = 0
            for sibling in cursor.previous_siblings:
                if isinstance(sibling, Tag) and sibling.name == cursor.name:
                    same_tag_left += 1
            nodes.append(f"{cursor.name}[{same_tag_left + 1}]")
            cursor = cursor.parent if isinstance(cursor.parent, Tag) else None
        return " > ".join(reversed(nodes))

    @staticmethod
    def _is_page_break(element: Tag) -> bool:
        classes = element.get("class", [])
        if isinstance(classes, str):
            classes = [classes]
        style = (element.get("style") or "").lower()
        return "pgbrk" in {cls.lower() for cls in classes} or "page-break" in style

    def _is_cover_text(self, text: str) -> bool:
        compact = self._compact_text(text)
        return any(
            token in compact
            for token in (
                "감사보고서",
                "재무제표에대한",
                "회계법인",
                "제45기",
                "제55기",
                "제56기",
            )
        )

    @staticmethod
    def _is_footnote_text(text: str) -> bool:
        stripped = text.strip()
        return stripped.startswith(("※", "*", "주)", "별첨", "주석 "))

    def _match_section_rules(
        self,
        text: str,
        parent_section_type: Optional[str] = None,
    ) -> List[Dict[str, object]]:
        compact = self._compact_text(text)
        matches: List[Dict[str, object]] = []
        for rule in SECTION_RULES:
            parent_types = rule["parent_types"]
            if parent_types is not None and parent_section_type not in parent_types:
                continue
            if rule["pattern"].search(compact):
                matches.append(rule)
        matches.sort(key=lambda item: int(item["priority"]), reverse=True)
        return matches

    def _match_any_section_rule(self, text: str) -> bool:
        compact = self._compact_text(text)
        return any(rule["pattern"].search(compact) for rule in SECTION_RULES)

    def _looks_like_heading(self, element: Tag, text: str) -> bool:
        if not text:
            return False
        if element.name in {"h1", "h2", "h3"}:
            return True
        class_names = self._extract_class_names(element)
        style = (element.get("style") or "").lower()
        is_bold = element.find(["b", "strong"]) is not None or "font-weight:bold" in style
        if any(name.startswith("section-") or name == "cover-title" for name in class_names):
            return True
        if self._match_any_section_rule(text) and (len(text) <= 120 or is_bold):
            return True
        if is_bold and len(text) <= 60:
            return True
        compact = self._compact_text(text)
        return any(pattern.search(compact) for pattern in SECTION_TITLE_PATTERNS)

    def _is_reference_heading_context(
        self,
        text: str,
        idx: int,
        consumed_len: int,
    ) -> bool:
        before = self._compact_text(text[max(0, idx - 30):idx])
        after = self._compact_text(text[idx + consumed_len: idx + consumed_len + 40])
        if after.startswith(
            (
                "단락에기술",
                "단락에서기술",
                "단락에설명",
                "단락에서설명",
                "단락을참조",
                "단락참조",
            )
        ):
            return True
        return before.endswith(("이감사보고서의", "감사보고서의", "본감사보고서의")) and after.startswith(
            ("단락", "절", "부분")
        )

    def _find_next_inner_heading(self, text: str, start_idx: int = 0) -> Optional[Tuple[int, str, int]]:
        candidates: List[Tuple[int, str, int]] = []
        for title in INNER_SPLIT_TITLES:
            search_from = start_idx
            while True:
                idx = text.find(title, search_from)
                if idx < 0:
                    break
                prev_ok = idx == 0 or text[idx - 1].isspace() or text[idx - 1] in "([{\"'“"
                next_idx = idx + len(title)
                next_ok = next_idx >= len(text) or text[next_idx].isspace()
                search_from = idx + 1
                if not (prev_ok and next_ok):
                    continue
                candidates.append((idx, title, len(title)))
                break

        opinion_phrase = "우리의 의견으로는"
        opinion_idx = text.find(opinion_phrase, start_idx)
        if opinion_idx >= max(start_idx, 80):
            candidates.append((opinion_idx, "감사의견", len(opinion_phrase)))

        candidates.sort(key=lambda item: (item[0], -len(item[1])))
        for idx, title, consumed_len in candidates:
            if self._is_reference_heading_context(text, idx, consumed_len):
                continue
            return idx, title, consumed_len
        return None

    def _split_body_by_inner_headings(self, text: str) -> List[Tuple[str, str, Dict[str, object]]]:
        remaining = self._normalize_text(text)
        if not remaining:
            return []

        parts: List[Tuple[str, str, Dict[str, object]]] = []
        pos = 0
        found_heading = False
        while True:
            hit = self._find_next_inner_heading(remaining, pos)
            if hit is None:
                tail = self._normalize_text(remaining[pos:])
                if tail and found_heading:
                    parts.append(("paragraph", tail, {"synthetic": True, "split_part": "body"}))
                break

            idx, title, consumed_len = hit
            found_heading = True
            lead = self._normalize_text(remaining[pos:idx])
            if lead:
                parts.append(("paragraph", lead, {"synthetic": True, "split_part": "body"}))
            parts.append(("section_heading", title, {"synthetic": True, "split_part": "heading"}))
            pos = idx + consumed_len
        return parts if found_heading else []

    def _split_embedded_heading(self, text: str) -> List[Tuple[str, str, Dict[str, object]]]:
        normalized = self._normalize_text(text)
        if not normalized:
            return []

        if self._match_any_section_rule(normalized) and len(normalized) <= 120:
            return [("section_heading", normalized, {"synthetic": False, "title_only": True})]

        early_match = EARLY_INLINE_HEADING.match(normalized)
        if early_match:
            prefix = self._normalize_text(early_match.group("prefix"))
            title = self._normalize_text(early_match.group("title"))
            body = self._normalize_text(early_match.group("body"))
            parts: List[Tuple[str, str, Dict[str, object]]] = []
            if prefix:
                parts.append(("paragraph", prefix, {"synthetic": True, "split_part": "prefix"}))
            parts.append(("section_heading", title, {"synthetic": True, "split_part": "heading"}))
            parts.extend(self._split_body_by_inner_headings(body))
            return parts

        for pattern in INLINE_PREFIX_PATTERNS:
            match = pattern.match(normalized)
            if not match:
                continue
            title = self._normalize_text(match.group("title")).rstrip(":：")
            body = self._normalize_text(match.groupdict().get("body", ""))
            parts = [("section_heading", title, {"synthetic": True, "split_part": "heading"})]
            parts.extend(self._split_body_by_inner_headings(body))
            return parts

        generic_parts = self._split_body_by_inner_headings(normalized)
        if generic_parts:
            return generic_parts

        return []

    def parse(self) -> List[Block]:
        body = self.soup.find("body")
        if not body:
            self.blocks = []
            self.sections = []
            return self.blocks

        for removable in body.find_all(["script", "style", "meta"]):
            removable.extract()

        elements = body.find_all(["h1", "h2", "h3", "p", "div", "table", "hr"])
        blocks: List[Block] = []
        seen_main_heading = False
        page_index = 1

        def append_block(
            block_type: str,
            text: str,
            element: Tag,
            extra_metadata: Optional[Dict[str, object]] = None,
        ) -> None:
            nonlocal page_index
            if block_type != "table" and block_type != "page_break" and not text:
                return

            metadata = {
                "tag_name": element.name,
                "class_names": self._extract_class_names(element),
                "style": element.get("style", ""),
                "is_bold": element.find(["b", "strong"]) is not None
                or "font-weight:bold" in (element.get("style") or "").lower(),
                "parser_backend": self.parser_backend,
            }
            if extra_metadata:
                metadata.update(extra_metadata)
            if block_type == "table":
                metadata["table_text"] = self._normalize_text(element.get_text(" ", strip=True))

            block = Block(
                block_id=f"b_{len(blocks):05d}",
                block_type=block_type,
                text=text,
                html_fragment=str(element),
                dom_path=self._get_dom_path(element),
                order_index=len(blocks),
                page_index=page_index,
                metadata=metadata,
            )
            if blocks:
                block.prev_block_id = blocks[-1].block_id
                blocks[-1].next_block_id = block.block_id
            blocks.append(block)
            if block_type == "page_break":
                page_index += 1

        for element in elements:
            if element.name != "table" and element.find_parent("table") is not None:
                continue
            if element.name not in {"h1", "h2", "h3"} and element.find_parent(["h1", "h2", "h3"]) is not None:
                continue

            if element.name == "div" and element.find("table"):
                div_text = self._normalize_text(element.get_text(" ", strip=True))
                table_text = self._normalize_text(
                    " ".join(tbl.get_text(" ", strip=True) for tbl in element.find_all("table"))
                )
                if not div_text or div_text == table_text:
                    continue

            if element.name == "table":
                append_block("table", "", element)
                continue

            text = self._normalize_text(element.get_text(" ", strip=True))
            if not text and not self._is_page_break(element):
                continue

            if self._is_page_break(element):
                append_block("page_break", "", element)
                continue

            if "독립된 감사인의 감사보고서" in text:
                seen_main_heading = True

            if not seen_main_heading and self._is_cover_text(text):
                append_block("cover", text, element)
                continue

            if self._is_footnote_text(text):
                append_block("footnote", text, element)
                continue

            split_blocks = self._split_embedded_heading(text)
            if split_blocks:
                for block_type, block_text, extra in split_blocks:
                    append_block(block_type, block_text, element, extra)
                continue

            if self._looks_like_heading(element, text):
                append_block("section_heading", text, element)
            else:
                append_block("paragraph", text, element)

        self.blocks = blocks
        self.sections = []
        return self.blocks

    def build_sections(self, filing_id: str) -> List[Section]:
        if not self.blocks:
            self.parse()
        if not self.blocks:
            self.sections = []
            return self.sections

        sections: List[Section] = []
        root = Section(
            section_id=f"{filing_id}_s000",
            filing_id=filing_id,
            section_type="cover",
            section_title="cover",
            start_block_id=self.blocks[0].block_id,
            order_index=0,
            parent_section_id=None,
            section_level=1,
        )
        sections.append(root)
        current_top = root
        current_leaf = root

        for block in self.blocks:
            if block.block_type == "section_heading":
                top_matches = self._match_section_rules(block.text, None)
                match: Optional[Dict[str, object]] = None
                level: Optional[str] = None

                if top_matches and top_matches[0]["level"] == "top":
                    match = top_matches[0]
                    level = "top"
                else:
                    sub_matches = self._match_section_rules(block.text, current_top.section_type)
                    if sub_matches:
                        match = sub_matches[0]
                        level = str(match["level"])
                    elif top_matches:
                        match = top_matches[0]
                        level = str(match["level"])

                if match is not None:
                    matched_type = str(match["section_type"])
                    is_duplicate_heading = False
                    if level == "top":
                        is_duplicate_heading = (
                            current_leaf.section_type == matched_type
                            and current_leaf.section_title == block.text
                            and current_leaf.parent_section_id is None
                        )
                    else:
                        same_parent = current_leaf.parent_section_id == current_top.section_id
                        same_type = current_leaf.section_type == matched_type
                        same_title = current_leaf.section_title == block.text
                        mergeable_repeat = matched_type in MERGEABLE_SUBSECTION_TYPES and same_type and same_parent
                        is_duplicate_heading = (same_type and same_title and same_parent) or mergeable_repeat
                    if is_duplicate_heading:
                        block.section_id = current_leaf.section_id
                        block.section_type = current_leaf.section_type
                        block.section_title = current_leaf.section_title
                        continue

                    if current_leaf.start_block_id != block.block_id:
                        current_leaf.end_block_id = block.prev_block_id

                    if level == "top":
                        current_top = Section(
                            section_id=f"{filing_id}_s{len(sections):03d}",
                            filing_id=filing_id,
                            section_type=str(match["section_type"]),
                            section_title=block.text,
                            start_block_id=block.block_id,
                            order_index=len(sections),
                            parent_section_id=None,
                            section_level=1,
                        )
                        sections.append(current_top)
                        current_leaf = current_top
                    else:
                        current_leaf = Section(
                            section_id=f"{filing_id}_s{len(sections):03d}",
                            filing_id=filing_id,
                            section_type=str(match["section_type"]),
                            section_title=block.text,
                            start_block_id=block.block_id,
                            order_index=len(sections),
                            parent_section_id=current_top.section_id,
                            section_level=2,
                        )
                        sections.append(current_leaf)

            block.section_id = current_leaf.section_id
            block.section_type = current_leaf.section_type
            block.section_title = current_leaf.section_title

        current_leaf.end_block_id = self.blocks[-1].block_id
        self.sections = sections
        return self.sections

    @staticmethod
    def _extract_korean_date_candidates(text: str) -> List[Tuple[str, Tuple[int, int, int]]]:
        results: List[Tuple[str, Tuple[int, int, int]]] = []
        for match in re.finditer(
            r"((?:19|20)\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일",
            text,
        ):
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                results.append((f"{year:04d}-{month:02d}-{day:02d}", (year, month, day)))
        return results

    @staticmethod
    def _score_date(
        year: int,
        month: int,
        day: int,
        fiscal_year: Optional[int],
        block_text: str,
        order_index: int,
        is_table: bool,
    ) -> int:
        score = 0
        if fiscal_year is not None:
            if year == fiscal_year + 1:
                score += 100
            elif year == fiscal_year:
                score += 40
        if 1 <= month <= 4:
            score += 20
        if month == 12 and day == 31:
            score -= 40
        if "귀중" in block_text:
            score += 80
        if "감사보고서일" in block_text:
            score += 60
        if "회계법인" in block_text or "대표이사" in block_text:
            score += 20
        if is_table:
            score += 15
        if order_index < 20:
            score += 20
        return score

    def extract_document_meta(
        self,
        filing_id: str,
        source_file: Optional[str] = None,
        source_encoding: Optional[str] = None,
    ) -> DocumentMeta:
        if not self.blocks:
            self.parse()

        text_candidates: List[str] = []
        for block in self.blocks[:60]:
            if block.text:
                text_candidates.append(block.text)
            table_text = block.metadata.get("table_text")
            if isinstance(table_text, str) and table_text:
                text_candidates.append(table_text)

        joined = "\n".join(text_candidates + [block.text for block in self.blocks[60:] if block.text])

        company_candidates: List[str] = []
        for text in text_candidates[:30]:
            compact = self._compact_text(text)
            company_candidates.extend(re.findall(r"([가-힣A-Za-z0-9()㈜]+주식회사)", compact))
        company_name = min(company_candidates, key=len) if company_candidates else "UNKNOWN"

        auditor_candidates: List[str] = []
        for text in text_candidates[:40]:
            compact = self._compact_text(text)
            auditor_candidates.extend(re.findall(r"([가-힣A-Za-z]{2,20}회계법인)", compact))
        auditor_name = min(set(auditor_candidates), key=len) if auditor_candidates else None

        year_candidates = {
            int(match.group(1))
            for match in re.finditer(r"((?:19|20)\d{2})\s*년\s*12\s*월\s*31\s*일", joined)
        }
        if not year_candidates:
            year_candidates = {int(year) for year in re.findall(r"(?:19|20)\d{2}", joined)}
        fiscal_year = max(year_candidates) if year_candidates else None

        attached_idx = next(
            (
                idx for idx, block in enumerate(self.blocks)
                if block.section_type == "attached_financial_statements"
                or self._compact_text(block.text) == "(첨부)재무제표"
            ),
            len(self.blocks),
        )
        report_blocks = self.blocks[:attached_idx] if attached_idx > 0 else self.blocks[:30]

        scored_dates: List[Tuple[int, str]] = []
        for block in report_blocks:
            block_text = block.text or str(block.metadata.get("table_text", "")).strip()
            for date_iso, (year, month, day) in self._extract_korean_date_candidates(block_text):
                scored_dates.append(
                    (
                        self._score_date(
                            year=year,
                            month=month,
                            day=day,
                            fiscal_year=fiscal_year,
                            block_text=block_text,
                            order_index=block.order_index,
                            is_table=block.block_type == "table",
                        ),
                        date_iso,
                    )
                )
        auditor_report_date = max(scored_dates, key=lambda item: item[0])[1] if scored_dates else None

        meta = DocumentMeta(
            filing_id=filing_id,
            company_name=company_name,
            fiscal_year=fiscal_year,
            report_type="감사보고서",
            auditor_name=auditor_name,
            auditor_report_date=auditor_report_date,
            source_file=source_file,
            source_encoding=source_encoding,
            parser_backend=self.parser_backend,
        )
        self.document_meta = meta
        return meta
