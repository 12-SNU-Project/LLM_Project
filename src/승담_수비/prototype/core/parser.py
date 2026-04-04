from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup, FeatureNotFound, Tag

from models import Block, DocumentMeta, Section

from utils.structure_first_utils import decode_html_file

SECTION_TITLE_PATTERNS = (
    r"독립된\s*감사인의\s*감사보고서",
    r"\(첨부\)\s*재\s*무\s*제\s*표",
    r"^주석$",
    r"내부회계관리제도\s*(감사\s*또는\s*검토의견|검토의견)",
    r"^외부감사\s*실시내용$",
    r"^감사의견$",
    r"^핵심감사사항$",
    r"^\d+\.\s*감사대상업무$",
    r"^\d+(\.\d+)*\s*보고기간\s*후\s*사건$",
)

SECTION_RULES = [
    # ---------- 하위 섹션(구체적 패턴 우선) ----------
    {
        "pattern": r"^감사의견$",
        "section_type": "audit_opinion",
        "level": "sub",
        "priority": 100,
        "parent_types": {"independent_auditor_report"},
    },
    {
        "pattern": r"^핵심감사사항$",
        "section_type": "key_audit_matters",
        "level": "sub",
        "priority": 100,
        "parent_types": {"independent_auditor_report"},
    },
    {
        "pattern": r"내부회계관리제도에대한감사의견",
        "section_type": "internal_control_audit_opinion",
        "level": "sub",
        "priority": 120,
        "parent_types": {"internal_control_opinion"},
    },
    {
        "pattern": r"내부회계관리제도감사의견근거",
        "section_type": "internal_control_audit_basis",
        "level": "sub",
        "priority": 120,
        "parent_types": {"internal_control_opinion"},
    },
    {
        "pattern": r"^\d+\.\s*감사대상업무$",
        "section_type": "external_audit_target_work",
        "level": "sub",
        "priority": 110,
        "parent_types": {"external_audit_activity"},
    },
    {
        "pattern": r"^\d+\.\s*감사참여자구분별인원수및감사시간$",
        "section_type": "external_audit_hours",
        "level": "sub",
        "priority": 110,
        "parent_types": {"external_audit_activity"},
    },
    {
        "pattern": r"^\d+(\.\d+)*\s*보고기간후사건$",
        "section_type": "subsequent_events",
        "level": "sub",
        "priority": 105,
        "parent_types": {"notes", "attached_financial_statements", "other"},
    },
    {
        "pattern": r"^\d+(\.\d+)*\s*우발부채와약정사항$",
        "section_type": "contingent_liabilities_and_commitments",
        "level": "sub",
        "priority": 105,
        "parent_types": {"notes", "attached_financial_statements", "other"},
    },

    # ---------- 상위 섹션 ----------
    {
        "pattern": r"독립된감사인의감사보고서",
        "section_type": "independent_auditor_report",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"\(첨부\)재무제표",
        "section_type": "attached_financial_statements",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"^주석$",
        "section_type": "notes",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"내부회계관리제도(감사또는검토의견|검토의견)",
        "section_type": "internal_control_opinion",
        "level": "top",
        "priority": 60,
        "parent_types": None,
    },
    {
        "pattern": r"^외부감사실시내용$",
        "section_type": "external_audit_activity",
        "level": "top",
        "priority": 60,
        "parent_types": None,
    },
]

CONSERVATIVE_RULES = [
    # 상위 섹션
    {
        "pattern": r"독립된감사인의감사보고서",
        "section_type": "independent_auditor_report",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"\(첨부\)재무제표",
        "section_type": "attached_financial_statements",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"^주석$",
        "section_type": "notes",
        "level": "top",
        "priority": 50,
        "parent_types": None,
    },
    {
        "pattern": r"내부회계관리제도(감사또는검토의견|검토의견)",
        "section_type": "internal_control_opinion",
        "level": "top",
        "priority": 60,
        "parent_types": None,
    },
    {
        "pattern": r"^외부감사실시내용$",
        "section_type": "external_audit_activity",
        "level": "top",
        "priority": 60,
        "parent_types": None,
    },

    # 보수적 모드에서도 확실한 하위 섹션은 허용
    {
        "pattern": r"^\d+(\.\d+)*보고기간후사건$",
        "section_type": "subsequent_events",
        "level": "sub",
        "priority": 90,
        "parent_types": {"notes", "attached_financial_statements", "cover", "other"},
    },
    {
        "pattern": r"^\d+\.\s*감사대상업무$",
        "section_type": "external_audit_target_work",
        "level": "sub",
        "priority": 95,
        "parent_types": {"external_audit_activity"},
    },
    {
        "pattern": r"^\d+\.\s*감사참여자구분별인원수및감사시간$",
        "section_type": "external_audit_hours",
        "level": "sub",
        "priority": 95,
        "parent_types": {"external_audit_activity"},
    },
]

AGGRESSIVE_RULES = CONSERVATIVE_RULES + [
    {
        "pattern": r"^감사의견$",
        "section_type": "audit_opinion",
        "level": "sub",
        "priority": 100,
        "parent_types": {"independent_auditor_report"},
    },
    {
        "pattern": r"^핵심감사사항$",
        "section_type": "key_audit_matters",
        "level": "sub",
        "priority": 100,
        "parent_types": {"independent_auditor_report"},
    },
    {
        "pattern": r"내부회계관리제도에대한감사의견",
        "section_type": "internal_control_audit_opinion",
        "level": "sub",
        "priority": 120,
        "parent_types": {"internal_control_opinion"},
    },
    {
        "pattern": r"내부회계관리제도감사의견근거",
        "section_type": "internal_control_audit_basis",
        "level": "sub",
        "priority": 120,
        "parent_types": {"internal_control_opinion"},
    },
]

class AuditReportParser:
    """Structure-first HTML 블록 파서."""
    
    def __init__(self, 
                html_content: str,
                parser_backends: Optional[List[str]] = None, 
                section_mode: str = "auto" # "auto" | "conservative" | "aggressive"
                ):   
        self.html_content = html_content
        self.parser_backends = parser_backends or ["lxml", "html5lib", "html.parser"]
        self.soup, self.parser_backend = self._make_soup(html_content)
        self.blocks: List[Block] = []
        self.sections: List[Section] = []

        # doucument meta 정보를 바탕으로 섹션 헤더 해석을 다르게 하도록 모드 추가
        self.document_meta: Optional[DocumentMeta] = None
        self.section_mode= section_mode

    def _make_soup(self, html_content: str) -> Tuple[BeautifulSoup, str]:
        for backend in self.parser_backends:
            try:
                return BeautifulSoup(html_content, backend), backend
            except FeatureNotFound:
                continue
        return BeautifulSoup(html_content, "html.parser"), "html.parser"

    @staticmethod
    def _normalize_text(text: str) -> str:
        compact = text.replace("\xa0", " ")
        compact = re.sub(r"\s+", " ", compact)
        return compact.strip()

    def _get_dom_path(self, element: Tag) -> str:
        if not isinstance(element, Tag):
            return ""

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
    def _extract_class_names(element: Tag) -> List[str]:
        classes = element.get("class", [])
        if isinstance(classes, str):
            classes = [classes]
        return [cls.lower() for cls in classes]

    def _is_section_title_text(self, text: str) -> bool:
        if not text:
            return False
        compact = self._compact_heading_text(text)

        return any(re.search(pattern, compact) for pattern in SECTION_TITLE_PATTERNS)

    def _detect_heading_level(
            self,
            element: Tag,
            text: str,
            parent_section_type: Optional[str] = None,
    ) -> Optional[str]:
        if not text:
            return None
        normalized = self._normalize_heading_text(text)
        class_names = self._extract_class_names(element)
        style = (element.get("style") or "").lower()
        is_bold = element.find("b") is not None or "font-weight:bold" in style
        is_heading_tag = element.name in {"h1", "h2", "h3"}
        is_section_class = any(name.startswith("section-") for name in class_names)
        matches = self._match_section_rules(text, parent_section_type)
        if not matches:
            return None
        best = matches[0]
        if len(normalized) > 80 and not (is_bold or is_heading_tag or is_section_class):
            return None
        
        return best["level"]

    def _is_cover_text(self, text: str) -> bool:
        if not text:
            return False
        no_space = text.replace(" ", "")
        return any(
            token in no_space
            for token in ("감사보고서", "재무제표에대한", "제56기", "제55기", "제54기", "제53기")
        )

    @staticmethod
    def _is_page_break(element: Tag) -> bool:
        class_names = element.get("class", [])
        if isinstance(class_names, str):
            class_names = [class_names]
        class_names = {name.lower() for name in class_names}
        style = (element.get("style") or "").lower()
        return "pgbrk" in class_names or "page-break" in style

    @staticmethod
    def _is_footnote_text(text: str) -> bool:
        if not text:
            return False
        return any(
            text.startswith(prefix)
            for prefix in ("별첨 주석은 본 재무제표의 일부입니다", "※", "*", "주)")
        )

    def _infer_block_type(self, element: Tag, text: str, seen_main_heading: bool) -> str:
        if element.name == "table":
            return "table"

        if self._is_page_break(element):
            return "page_break"

        if not text:
            return "empty"

        class_names = self._extract_class_names(element)
        style = (element.get("style") or "").lower()
        is_bold = element.find("b") is not None or "font-weight:bold" in style

        if element.name in {"h1", "h2", "h3"}:
            return "section_heading"

        if self._is_section_title_text(text):
            return "section_heading"

        if any(name.startswith("section-") for name in class_names):
            return "section_heading"

        if "cover-title" in class_names:
            return "cover"

        if not seen_main_heading and (self._is_cover_text(text) or "font-size:18pt" in style):
            return "cover"

        if self._is_footnote_text(text):
            return "footnote"

        return "paragraph"

    def _infer_section_type(
        self,
        title: str,
        parent_section_type: Optional[str] = None,
        ) -> str:
        matches = self._match_section_rules(title, parent_section_type)
        if matches:
            return matches[0]["section_type"]

        normalized = self._compact_heading_text(title)
        if "감사보고서" in normalized:
            return "cover"
        return "other"

    def parse(self) -> List[Block]:
        """문서 순서를 보존한 block IR 생성."""
        body = self.soup.find("body")
        if not body:
            self.blocks = []
            self.sections = []
            return self.blocks

        for ext in body.find_all(["script", "style", "meta"]):
            ext.extract()

        elements = body.find_all(["h1", "h2", "h3", "p", "div", "table", "hr"])
        blocks: List[Block] = []
        seen_main_heading = False

        for idx, elem in enumerate(elements):
            if elem.name != "table" and elem.find_parent("table") is not None:
                continue
            if elem.name not in {"h1", "h2", "h3"} and elem.find_parent(["h1", "h2", "h3"]) is not None:
                continue

            if elem.name == "div" and elem.find("table"):
                div_text = self._normalize_text(elem.get_text(" ", strip=True))
                table_text = self._normalize_text(
                    " ".join(tbl.get_text(" ", strip=True) for tbl in elem.find_all("table"))
                )
                if not div_text or div_text == table_text:
                    continue

            text = "" if elem.name == "table" else self._normalize_text(elem.get_text(" ", strip=True))
            block_type = self._infer_block_type(elem, text, seen_main_heading)
            if block_type == "empty":
                continue

            if "독립된 감사인의 감사보고서" in text:
                seen_main_heading = True

            block_id = f"b_{len(blocks):05d}"
            metadata = {
                "tag_name": elem.name,
                "class_names": self._extract_class_names(elem),
                "style": elem.get("style", ""),
                "is_bold": elem.find("b") is not None
                or "font-weight:bold" in (elem.get("style") or "").lower(),
                "parser_backend": self.parser_backend,
            }

            block = Block(
                block_id=block_id,
                block_type=block_type,
                text=text,
                html_fragment=str(elem),
                dom_path=self._get_dom_path(elem),
                order_index=len(blocks),
                metadata=metadata,
            )

            if blocks:
                block.prev_block_id = blocks[-1].block_id
                blocks[-1].next_block_id = block.block_id
            blocks.append(block)

        self.blocks = blocks
        self.sections = []
        return self.blocks

    def build_sections(self, filing_id: str) -> List[Section]:
        if not self.blocks:
            self.parse()
        if not self.document_meta:
            self.document_meta = self.extract_document_meta(filing_id)

        sections: List[Section] = []

        root = Section(
            section_id=f"{filing_id}_s000",
            filing_id=filing_id,
            section_type="cover",
            section_title="표지",
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
                heading_text = block.text or "제목없음"
                elem = BeautifulSoup(block.html_fragment, "html.parser").find()
                level = self._detect_heading_level(elem,
                    heading_text,
                    current_top.section_type if current_top else None,)

                if level == "top":
                    if current_leaf.start_block_id != block.block_id:
                        current_leaf.end_block_id = block.prev_block_id

                    new_section = Section(
                        section_id=f"{filing_id}_s{len(sections):03d}",
                        filing_id=filing_id,
                        section_type=self._infer_section_type(heading_text, None),
                        section_title=heading_text,
                        start_block_id=block.block_id,
                        order_index=len(sections),
                        parent_section_id=None,
                        section_level=1,
                    )
                    sections.append(new_section)
                    current_top = new_section
                    current_leaf = new_section

                elif level == "sub":
                    if current_leaf.start_block_id != block.block_id:
                        current_leaf.end_block_id = block.prev_block_id

                    new_section = Section(
                        section_id=f"{filing_id}_s{len(sections):03d}",
                        filing_id=filing_id,
                        section_type=self._infer_section_type(
                            heading_text,
                            current_top.section_type if current_top else None,
                        ),
                        section_title=heading_text,
                        start_block_id=block.block_id,
                        order_index=len(sections),
                        parent_section_id=current_top.section_id if current_top else None,
                        section_level=2,
                    )
                    sections.append(new_section)
                    current_leaf = new_section

            block.section_id = current_leaf.section_id
            block.section_type = current_leaf.section_type
            block.section_title = current_leaf.section_title

        current_leaf.end_block_id = self.blocks[-1].block_id
        self.sections = sections
        return sections

    def build_sections(self, filing_id: str) -> List[Section]:
        """블록 시퀀스를 섹션 단위로 라벨링."""
        if not self.blocks:
            self.parse()
        if not self.blocks:
            self.sections = []
            return self.sections

        sections: List[Section] = []

        current_section = Section(
            section_id=f"{filing_id}_s000",
            filing_id=filing_id,
            section_type="cover",
            section_title="표지",
            start_block_id=self.blocks[0].block_id,
            order_index=0,
            parent_section_id=None,
            section_level=1,
        )
        sections.append(current_section)

        current_top = current_section
        current_leaf = current_section

        for block in self.blocks:
            if block.block_type == "section_heading":
                heading_text = block.text or "제목없음"
                if current_section.start_block_id != block.block_id:
                    current_section.end_block_id = block.prev_block_id

                section = Section(
                    section_id=f"{filing_id}_s{len(sections):03d}",
                    filing_id=filing_id,
                    section_type=self._infer_section_type(heading_text),
                    section_title=heading_text,
                    start_block_id=block.block_id,
                    order_index=len(sections),
                )
                sections.append(section)
                current_section = section

            block.section_id = current_section.section_id
            block.section_type = current_section.section_type
            block.section_title = current_section.section_title

        current_section.end_block_id = self.blocks[-1].block_id
        self.sections = sections
        return self.sections

    def extract_document_meta(
        self,
        filing_id: str,
        source_file: Optional[str] = None,
        source_encoding: Optional[str] = None,
    ) -> DocumentMeta:
        """문서 레벨 메타데이터 추출."""
        if not self.blocks:
            self.parse()

        joined = "\n".join(block.text for block in self.blocks if block.text)

        company_match = re.search(r"([가-힣A-Za-z0-9]+주식회사)", joined)
        company_name = company_match.group(1) if company_match else "UNKNOWN"

        auditor_name = None
        auditor_candidates: List[str] = []
        for line in joined.splitlines():
            compact = re.sub(r"\s+", "", line)
            match = re.search(r"([가-힣A-Za-z]{2,20}회계법인)", compact)
            if match:
                auditor_candidates.append(match.group(1))
        if auditor_candidates:
            # 일반적으로 감사인명은 짧은 형태(예: 삼정회계법인)로 등장
            auditor_name = sorted(set(auditor_candidates), key=len)[0]

        year_candidates = {int(year) for year in re.findall(r"(19\d{2}|20\d{2})년\s*12월\s*31일", joined)}
        fiscal_year = max(year_candidates) if year_candidates else None

        report_date = None
        report_section_text = self._collect_section_text("independent_auditor_report")
        date_candidates = self._extract_korean_date_candidates(report_section_text)
        if date_candidates:
            picked = self._pick_best_report_date(date_candidates, fiscal_year=fiscal_year)
            report_date = picked[0] if picked else None
        else:
            # 보조 fallback: 문서 전체에서 날짜 후보를 찾되 최신 날짜를 선택
            all_dates = self._extract_korean_date_candidates(joined)
            if all_dates:
                picked = self._pick_best_report_date(all_dates, fiscal_year=fiscal_year)
                report_date = picked[0] if picked else None

        return DocumentMeta(
            filing_id=filing_id,
            company_name=company_name,
            fiscal_year=fiscal_year,
            report_type="감사보고서",
            auditor_name=auditor_name,
            auditor_report_date=report_date,
            source_file=source_file,
            source_encoding=source_encoding,
            parser_backend=self.parser_backend,
        )

    @staticmethod
    def _extract_korean_date_candidates(text: str) -> List[Tuple[str, Tuple[int, int, int]]]:
        candidates: List[Tuple[str, Tuple[int, int, int]]] = []
        for match in re.finditer(r"((19|20)\d{2})년\s*(\d{1,2})월\s*(\d{1,2})\s*일", text):
            year = int(match.group(1))
            month = int(match.group(3))
            day = int(match.group(4))
            if 1 <= month <= 12 and 1 <= day <= 31:
                full = f"{year}년 {month}월 {day}일"
                candidates.append((full, (year, month, day)))
        return candidates

    @staticmethod
    def _pick_best_report_date(
        candidates: List[Tuple[str, Tuple[int, int, int]]],
        fiscal_year: Optional[int],
    ) -> Optional[Tuple[str, Tuple[int, int, int]]]:
        if not candidates:
            return None

        def score(item: Tuple[str, Tuple[int, int, int]]) -> Tuple[int, int, int, int]:
            _, (year, month, day) = item
            s = 0
            if fiscal_year is not None:
                if year == fiscal_year + 1:
                    s += 100
                elif year == fiscal_year:
                    s += 60
                elif year == fiscal_year - 1:
                    s += 20
            # 감사보고서일은 통상 다음연도 1~4월에 집중
            if 1 <= month <= 4:
                s += 25
            if month == 12 and day == 31:
                s -= 35
            return (s, year, month, day)

        return max(candidates, key=score)

    def _collect_section_text(self, section_type: str) -> str:
        if not self.sections:
            return "\n".join(
                block.text for block in self.blocks if block.section_type == section_type and block.text
            )

        target_sections = [section for section in self.sections if section.section_type == section_type]
        if not target_sections:
            return "\n".join(
                block.text for block in self.blocks if block.section_type == section_type and block.text
            )

        block_idx = {block.block_id: idx for idx, block in enumerate(self.blocks)}
        texts: List[str] = []

        for section in target_sections:
            start_idx = block_idx.get(section.start_block_id)
            end_idx = block_idx.get(section.end_block_id or "", start_idx)
            if start_idx is None or end_idx is None:
                continue
            for idx in range(start_idx, end_idx + 1):
                block = self.blocks[idx]
                if block.text:
                    texts.append(block.text)
                if block.block_type == "table":
                    table_text = self._normalize_text(
                        BeautifulSoup(block.html_fragment, "html.parser").get_text(" ", strip=True)
                    )
                    if table_text:
                        texts.append(table_text)
        return "\n".join(texts)

    # Heading 정규화 내용 추가
    @staticmethod
    def _normalize_heading_text(text: str) -> str:
        text = text.replace("\xa0"," ")
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"[：:]+$", "", text)
        text = re.sub(r"[.]+$", "", text)
        return text 
    
    @classmethod
    def _compact_heading_text(cls, text: str) -> str:
        return re.sub(r"\s+","",cls._normalize_heading_text(text))

    def _choose_section_rulse(self):
        fiscal_year = self.document_meta.fiscal_year if self.extract_document_meta else None
        if self.section_mode == "conservative":
            return CONSERVATIVE_RULES
        if self.section_mode == "aggressive":
            return AGGRESSIVE_RULES
        
        if fiscal_year is not None and fiscal_year <= 2018:
            return CONSERVATIVE_RULES
        return AGGRESSIVE_RULES
    
    def _match_section_rules(
            self,
            text: str,
            parent_section_type: Optional[str] = None,
    ):
        compact = self._compact_heading_text(text)
        matches = []

        for rule in self._choose_section_rulse():
            if re.search(rule["pattern"], compact):
                parent_types = rule["parent_types"]
                if parent_types is None or parent_section_type in parent_types:
                    matches.append(rule)
        matches.sort(key=lambda r: (r["priority"], len(r["pattern"])), reverse=True)
        return matches