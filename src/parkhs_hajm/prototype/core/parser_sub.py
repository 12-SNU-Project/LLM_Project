"""감사보고서 통합 파서

[실행 순서]
  1. AuditReportParser  — 종속/관계/공동기업 표 파싱
     → SQLite (subsidiaries, investment_changes)
     → 벡터DB audit_company 컬렉션

  2. AuditNoteParser    — KAM + 관계/종속기업 주석 파싱
     → 벡터DB audit_notes 컬렉션
     → SQLite table_meta를 앵커로 참조 (1 완료 후 실행)

[헤더 처리 수정사항]
  - row1에 HEADER_MAP 키워드가 있고,
    row0에 당기/전기/당기말/전기말 상위 헤더 흔적이 있을 때 2단 헤더 확정
    (요약 재무정보 표의 자산/부채/매출액/당기순이익 파싱 지원)
  - "당기"/"당기말" 접두사 → 제거 후 매핑
  - "전기"/"전기말" 포함 헤더 → 해당 컬럼만 스킵

[안정화 수정사항]
  - "기타", "합계", "총계" 등 비회사 행 제외
  - reset_db 기본/실행값을 False로 두어 매번 전체 초기화 방지
"""

from bs4 import BeautifulSoup
from chromadb import PersistentClient
from chromadb.api.types import EmbeddingFunction
from typing import List, Dict, Optional, Tuple
import sqlite3, re, os, shutil, json, hashlib


BASE = "/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리"


# ============================================================
# 공통 상수
# ============================================================

COUNTRY_MAP = {
    "Korea": "한국", "Seoul": "한국",
    "Vietnam": "베트남", "Viet": "베트남", "THAINGUYEN": "베트남",
    "China": "중국", "Suzhou": "중국", "Tianjin": "중국", "Shanghai": "중국",
    "Shenzhen": "중국", "Beijing": "중국", "Dongguan": "중국", "Huizhou": "중국",
    "America": "미국", "USA": "미국", "India": "인도", "Japan": "일본",
    "Brazil": "브라질", "Mexico": "멕시코", "Hungary": "헝가리",
    "UK": "영국", "Poland": "폴란드", "Russia": "러시아", "Australia": "호주",
    "Canada": "캐나다", "Singapore": "싱가포르", "Thailand": "태국",
    "Indonesia": "인도네시아", "Malaysia": "말레이시아", "Philippines": "필리핀",
    "Czech": "체코", "Slovakia": "슬로바키아", "Ukraine": "우크라이나",
    "Turkey": "튀르키예", "Egypt": "이집트", "Kazakhstan": "카자흐스탄",
    "Germany": "독일", "France": "프랑스", "Netherlands": "네덜란드",
    "Sweden": "스웨덴", "Austria": "오스트리아", "Romania": "루마니아",
    "Serbia": "세르비아", "Taiwan": "대만", "HongKong": "홍콩",
}

HEADER_MAP = {
    "기업명": "company_name", "구분": "company_name",
    "소유지분율": "ownership_rate", "보유지분": "ownership_rate", "지분율": "ownership_rate",
    "주사업장": "location", "소재지": "location", "소재국": "location", "지역": "location",
    "관계의성격": "industry", "주요사업": "industry", "사업내용": "industry", "업종": "industry",
    "매출액": "revenue_table",
    "매출": "revenue",
    "매입등": "purchase", "매입": "purchase",
    "채권": "receivable", "채무": "payable",
    "비유동자산처분": "asset_noncurrent_disposal",
    "취득": "investment_acquisition", "처분": "investment_disposal",
    "유동자산": "asset_current", "비유동자산": "asset_noncurrent", "자산": "asset_total",
    "유동부채": "liability_current", "비유동부채": "liability_noncurrent", "부채": "liability_total",
    "당기순이익손실": "net_income", "당기순손실": "net_income", "당기순이익": "net_income",
    "손실": "net_income",
    "세후중단영업손익": "profit_discontinued", "계속영업손익": "profit_continuing",
    "기타포괄손익": "profit_other_ci", "총포괄손익": "profit_total_ci",
    "기초잔액": "investment_beginning",
    "증감액": "investment_change",
    "기말잔액": "investment_ending",
}

COMPANY_SIGNAL_HEADERS = {"지분율", "소유지분율", "보유지분", "소재지", "소재국", "주사업장", "업종", "기업명"}
CHANGE_KW    = {"기초", "취득", "처분", "손상", "손상환입", "기말", "증감", "증감액"}
AGGREGATE_KW = {"계", "합계"}
NON_COMPANY_ROW_EXACT = {
    "기타", "합계", "총계", "계", "소계", "합산", "총합", "합계금액", "총금액"
}
NON_COMPANY_ROW_CONTAINS = {
    "합계", "총계", "소계", "총합", "합산"
}
PERIOD_HEADER_KW = {"당기", "전기", "당기말", "전기말"}

KAM_START_KW = {"핵심감사사항", "Key Audit Matters", "KAM"}
KAM_END_STRONG = {
    "감사인의 책임", "경영진의 책임", "재무제표에 대한",
    "독립된 감사인의 보고서", "기타사항", "법규",
    "내부회계관리제도", "전자공시시스템",
}
RELATION_KW = {
    "종속기업", "관계기업", "공동기업",
    "subsidiaries", "subsidiary", "associates", "associate",
    "joint ventures", "joint venture",
}
RELATION_END_KW = {
    "특수관계자", "재고자산", "충당부채", "법인세", "영업권", "무형자산",
    "유형자산", "리스", "금융상품", "수익", "주식기준보상",
    "핵심감사사항", "감사의견",
}
SECTION_LIKE_KW = {
    "주석", "핵심감사사항", "특수관계자", "종속기업", "관계기업", "공동기업",
    "재무제표에 대한", "경영진의 책임", "감사인의 책임",
}


# ============================================================
# 공통 유틸 함수
# ============================================================

def _norm_header(h: str) -> str:
    h = re.sub(r"\s+", "", h)
    h = re.sub(r"\(\*\d*\)|[()]", "", h)
    h = re.sub(r"등$", "", h)
    return h


def _map_col(raw: str) -> Optional[str]:
    n = _norm_header(raw)
    if re.search(r"전기말?", n):
        return None
    n = re.sub(r"^당기말?_?", "", n)
    if n in HEADER_MAP:
        return HEADER_MAP[n]
    for key in sorted(HEADER_MAP, key=len, reverse=True):
        if key in n:
            return HEADER_MAP[key]
    return None


def _to_num(s: str) -> Optional[float]:
    s = s.strip().replace(",", "").replace("△", "-").replace(" ", "")
    m = re.fullmatch(r"\((-?\d+(?:\.\d+)?)\)", s)
    if m:
        s = f"-{m.group(1)}"
    try:
        return float(s)
    except ValueError:
        return None


def _location(name: str, raw: Dict) -> Optional[str]:
    for eng, kor in COUNTRY_MAP.items():
        if eng in name:
            return kor
    for k, v in raw.items():
        if any(x in _norm_header(k) for x in ["지역", "소재지", "소재국", "주사업장"]):
            for eng, kor in COUNTRY_MAP.items():
                if eng in str(v):
                    return kor
    return None


def _is_non_company_row(name: str) -> bool:
    c = re.sub(r"\s+", "", name)
    if not c:
        return True
    if c in NON_COMPANY_ROW_EXACT:
        return True
    if any(k in c for k in NON_COMPANY_ROW_CONTAINS):
        return True
    return False


def _is_b_row(name: str, raw: Dict) -> bool:
    c = re.sub(r"\s+", "", name)
    if _is_non_company_row(c):
        return True
    if any(k in c for k in CHANGE_KW | AGGREGATE_KW):
        return True
    has_signal    = any(_map_col(k) in {"ownership_rate", "location", "industry"} for k in raw)
    has_only_nums = all(isinstance(v, float) for v in raw.values() if v)
    return has_only_nums and not has_signal


def _read_html(path: str) -> str:
    for enc in ["euc-kr", "cp949", "latin1"]:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except Exception:
            continue
    raise ValueError(f"읽기 실패: {path}")


def _parse_merged(table) -> List[List[str]]:
    rows = table.find_all("tr")
    max_cols = max(
        (sum(int(c.get("colspan", 1)) for c in r.find_all(["td", "th"])) for r in rows),
        default=0)
    data: List[List[str]] = []
    for ri, row in enumerate(rows):
        if ri >= len(data): data.append([""] * max_cols)
        ci = 0
        for cell in row.find_all(["td", "th"]):
            while ci < max_cols and data[ri][ci] != "": ci += 1
            if ci >= max_cols: break
            v  = cell.get_text(strip=True)
            cs = int(cell.get("colspan", 1))
            rs = int(cell.get("rowspan", 1))
            for r in range(rs):
                if ri + r >= len(data): data.append([""] * max_cols)
                for c in range(cs):
                    if ci + c < max_cols: data[ri + r][ci + c] = v
            ci += cs
    return data


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _norm_anchor(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).lower()


def _file_hash(path: str) -> str:
    return hashlib.md5(path.encode()).hexdigest()[:8]


# ============================================================
# 마크다운 생성 (벡터DB 저장용)
# ============================================================

def _company_md(year: int, name: str, f: Dict) -> str:
    g = lambda k: f.get(k, "null")
    rel = {"subsidiary": "종속기업", "affiliate": "관계기업",
           "joint_venture": "공동기업"}.get(f.get("relation_type", ""), "미분류")
    return (
        f"# {name} — {year}년 삼성전자 감사보고서\n"
        f"- 구분: {rel}\n- 지분율: {g('ownership_rate')}%\n"
        f"- 소재지: {g('location')}\n- 업종: {g('industry')}\n\n"
        f"## 내부거래 (백만원)\n"
        f"- 매출: {g('revenue')}\n- 매입: {g('purchase')}\n"
        f"- 채권: {g('receivable')}\n- 채무: {g('payable')}\n"
        f"- 비유동자산 처분: {g('asset_noncurrent_disposal')}\n"
        f"- 취득: {g('investment_acquisition')}\n- 처분: {g('investment_disposal')}\n\n"
        f"## 요약 재무상태표 (백만원)\n"
        f"- 유동자산: {g('asset_current')}\n- 비유동자산: {g('asset_noncurrent')}\n"
        f"- 자산: {g('asset_total')}\n- 유동부채: {g('liability_current')}\n"
        f"- 비유동부채: {g('liability_noncurrent')}\n- 부채: {g('liability_total')}\n\n"
        f"## 요약 포괄손익계산서 (백만원)\n"
        f"- 매출액: {g('revenue_table')}\n- 당기순이익(손실): {g('net_income')}\n"
        f"- 포괄손익계산서 매출: {g('revenue_is')}\n"
        f"- 계속영업손익: {g('profit_continuing')}\n"
        f"- 세후중단영업손익: {g('profit_discontinued')}\n"
        f"- 기타포괄손익: {g('profit_other_ci')}\n- 총포괄손익: {g('profit_total_ci')}\n\n"
        f"## 투자 변동 (백만원)\n"
        f"- 기초잔액: {g('investment_beginning')}\n"
        f"- 증감액: {g('investment_change')}\n"
        f"- 기말잔액: {g('investment_ending')}"
    )


def _table_summary_md(year: int, rel: str, sec: str, headers: List[str],
                      company_names: List[str]) -> str:
    rel_kor = {"subsidiary": "종속기업", "affiliate": "관계기업",
               "joint_venture": "공동기업"}.get(rel, "미분류")
    return (
        f"# {year}년 삼성전자 {rel_kor} 표 요약\n"
        f"- 섹션: {sec}\n"
        f"- 헤더: {', '.join(h for h in headers if h)}\n"
        f"- 기업 수: {len(company_names)}\n"
        f"- 기업 목록: {', '.join(company_names[:20])}"
    )


def _changes_md(year: int, rows: List[Dict], section: str) -> str:
    lines = [
        f"# {year}년 삼성전자 종속/관계/공동기업 투자 변동",
        f"- 섹션: {section}", "",
        "| 항목 | 당기 |", "|---|---|",
    ]
    for r in rows:
        lines.append(f"| {r['item']} | {r.get('amount', 'null')} |")
    return "\n".join(lines)


def _is_section_like(text: str, is_bold: bool) -> bool:
    if not is_bold:
        return False
    if any(k in text for k in SECTION_LIKE_KW):
        return True
    if re.match(r"^([0-9]{1,2}\.|[가-힣]{1,2}\.)", text):
        return True
    return False


def _get_note_relation_type(section_title: str, bold_title: str) -> str:
    for text in [bold_title, section_title]:
        has_sub = "종속기업" in text
        has_asc = "관계기업" in text
        has_jv  = "공동기업" in text
        count   = sum([has_sub, has_asc, has_jv])
        if count > 1: return "mixed"
        if has_sub:   return "subsidiary"
        if has_asc:   return "associate"
        if has_jv:    return "joint_venture"
    return "mixed"


def _flatten_table(data: List[List[str]], max_rows: int = 12, max_chars: int = 500) -> str:
    lines = []
    for row in data[:max_rows]:
        line = " | ".join(v for v in row if v.strip())
        if line:
            lines.append(line)
    return "\n".join(lines)[:max_chars]


def _chunk_text(blocks_text: List[str], max_len: int = 800, min_len: int = 120) -> List[str]:
    chunks: List[str] = []
    buf = ""
    for text in blocks_text:
        if not text.strip():
            continue
        if buf and len(buf) + len(text) + 1 > max_len:
            if len(buf) >= min_len:
                chunks.append(buf.strip())
                buf = text
            else:
                buf = buf + " " + text
        else:
            buf = (buf + " " + text).strip() if buf else text
    if buf.strip() and len(buf.strip()) >= min_len:
        chunks.append(buf.strip())
    elif buf.strip() and chunks:
        chunks[-1] = chunks[-1] + " " + buf.strip()
    return chunks


# ============================================================
# AuditReportParser — 종속/관계/공동기업 표 파싱
# ============================================================

class AuditReportParser:

    def __init__(self, vector_db_path: str, sqlite_db_path: str,
                 embedding_fn: EmbeddingFunction, reset_db: bool = False):

        if reset_db and os.path.exists(vector_db_path):
            shutil.rmtree(vector_db_path)
            print(f"  ✓ 벡터DB 초기화: {vector_db_path}")

        self._chroma = PersistentClient(path=vector_db_path)
        self.col = self._chroma.get_or_create_collection(
            name="audit_company", embedding_function=embedding_fn)

        self.db = sqlite3.connect(sqlite_db_path)
        cur = self.db.cursor()
        if reset_db:
            cur.execute("DROP TABLE IF EXISTS subsidiaries")
            cur.execute("DROP TABLE IF EXISTS investment_changes")
            print("  ✓ SQLite 초기화")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS subsidiaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER, table_id TEXT, source_section TEXT,
                bold_title TEXT, raw_headers TEXT, header_type TEXT, source_html TEXT,
                company_name TEXT, relation_type TEXT,
                ownership_rate REAL, location TEXT, industry TEXT,
                revenue REAL, purchase REAL, receivable REAL, payable REAL,
                asset_noncurrent_disposal REAL,
                investment_acquisition REAL, investment_disposal REAL,
                asset_current REAL, asset_noncurrent REAL, asset_total REAL,
                liability_current REAL, liability_noncurrent REAL, liability_total REAL,
                revenue_table REAL, net_income REAL, revenue_is REAL,
                profit_continuing REAL, profit_discontinued REAL,
                profit_other_ci REAL, profit_total_ci REAL,
                investment_beginning REAL, investment_change REAL, investment_ending REAL
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS investment_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER, table_id TEXT, source_section TEXT,
                item TEXT, amount REAL
            )""")
        self.db.commit()
        print(f"✓ 벡터DB: {vector_db_path} | audit_company={self.col.count()}")
        print(f"✓ SQLite:  {sqlite_db_path}")

    def _build_headers(self, data: List[List[str]]) -> Tuple[List[str], int, str]:
        if not data:
            return [], 0, "single"
        row0 = data[0]
        if len(data) < 2:
            return row0, 1, "single"
        row1 = data[1]

        row0_norms = [_norm_header(v) for v in row0 if v]
        row1_norms = [_norm_header(v) for v in row1 if v]
        has_row1_header_kw = any(any(key in n for key in HEADER_MAP) for n in row1_norms)
        has_row0_period_kw = any(any(pk in n for pk in PERIOD_HEADER_KW) for n in row0_norms)

        if has_row1_header_kw and has_row0_period_kw:
            headers = []
            for h0, h1 in zip(row0, row1):
                if not h0 and not h1:   headers.append("")
                elif not h1 or h0 == h1: headers.append(h0)
                elif not h0:            headers.append(h1)
                else:                   headers.append(f"{h0}_{h1}")
            return headers, 2, "double"

        non_text = [v for v in row1 if v and not re.fullmatch(r"[\d,.()\-△%\s]+", v)]
        if len(non_text) < len(row1) * 0.3 or row0 == row1:
            return row0, 1, "single"
        if sum(1 for v in row1 if v and (re.match(r"^[A-Z]", v) or "Samsung" in v)) >= 1:
            return row0, 1, "single"
        if {_norm_header(h) for h in row0} & {"기업명", "지분율", "소유지분율", "소재지", "주사업장", "업종"}:
            return row0, 1, "single"

        headers = []
        for h0, h1 in zip(row0, row1):
            if not h0 and not h1:   headers.append("")
            elif not h1 or h0 == h1: headers.append(h0)
            elif not h0:            headers.append(h1)
            else:                   headers.append(f"{h0}_{h1}")
        return headers, 2, "double"

    def _get_name_col(self, headers: List[str]) -> Optional[int]:
        for i, h in enumerate(headers):
            if "기업명" in h:
                return i
        norm_headers = {_norm_header(h) for h in headers}
        if norm_headers & COMPANY_SIGNAL_HEADERS:
            for i, h in enumerate(headers):
                if "구분" in h:
                    return i
        return None

    def _get_relation_type(self, sec: str, bold: str, flat: str) -> str:
        for text in [sec, bold, flat]:
            if "종속기업" in text: return "subsidiary"
            if "관계기업" in text: return "affiliate"
            if "공동기업" in text: return "joint_venture"
        return "unknown"

    def _traverse(self, html_path: str, year: int) -> List[Dict]:
        soup = BeautifulSoup(_read_html(html_path), "lxml")
        section, bold, candidates, ti = "", "", [], 0
        for tag in (soup.body or soup).find_all(["p", "table"], recursive=True):
            if tag.name == "p":
                if "SECTION-1" in (tag.get("class") or []):
                    continue
                text  = re.sub(r"\s+", " ", tag.get_text(" ")).strip()
                style = (tag.get("style") or "").replace(" ", "").lower()
                is_bold = "font-weight:bold" in style or "font-weight:700" in style
                is_sec  = any(k in text for k in ["종속기업", "관계기업", "공동기업"]) or \
                          bool(re.match(r"^([가-힣]{1,2}\.|[0-9]{1,2}\.)", text))
                if is_bold:
                    bold = text
                    if is_sec: section = text
                elif is_sec:
                    section = text
            elif tag.name == "table":
                if "nb" in (tag.get("class") or []):
                    continue
                candidates.append({
                    "year": year, "table_index": ti, "table": tag,
                    "section_title": section, "bold_title": bold,
                    "html_path": html_path,
                })
                ti += 1
        print(f"  ✓ 표 후보 {len(candidates)}개")
        return candidates

    def _parse_tables(self, candidates: List[Dict], year: int):
        ctx_kw   = {"종속기업", "관계기업", "공동기업"}
        inner_kw = {"기업명"} | ctx_kw

        sqlite_company_rows: List[Tuple] = []
        sqlite_change_rows:  List[Tuple] = []
        vec_docs:  List[str]  = []
        vec_metas: List[Dict] = []
        vec_ids:   List[str]  = []

        for c in candidates:
            data = _parse_merged(c["table"])
            if len(data) < 2:
                continue
            sec       = c["section_title"]
            bold      = c["bold_title"]
            html_path = c["html_path"]
            tid       = f"company_{year}_{c['table_index']}"

            flat = " ".join(" ".join(r) for r in data[:20])
            if not (any(k in f"{sec} {bold}" for k in ctx_kw) or
                    any(k in flat for k in inner_kw)):
                continue

            headers, data_start, header_type = self._build_headers(data)
            rel      = self._get_relation_type(sec, bold, flat)
            name_col = self._get_name_col(headers)
            if name_col is None:
                continue

            raw_headers_json = json.dumps(headers, ensure_ascii=False)
            change_rows:  List[Dict] = []
            company_rows: List[Tuple[str, Dict]] = []

            for row in data[data_start:]:
                if not any(v.strip() for v in row):
                    continue
                name = row[name_col].strip() if name_col < len(row) else ""
                if not name or len(name) < 2 or _is_non_company_row(name):
                    continue

                raw: Dict = {}
                for i, v in enumerate(row):
                    if i >= len(headers) or not v.strip() or v.strip() in {"-", "－"}:
                        continue
                    num = _to_num(v)
                    raw[headers[i]] = num if num is not None else v.strip()

                if _is_b_row(name, raw):
                    amount = None
                    for h, val in raw.items():
                        hn = _norm_header(h)
                        if re.search(r"^당기말?", hn) and amount is None:
                            amount = val if isinstance(val, float) else _to_num(str(val))
                    if amount is None:
                        for h, val in raw.items():
                            if not re.search(r"전기말?", _norm_header(h)) and isinstance(val, float):
                                amount = val
                                break
                    change_rows.append({"item": name, "amount": amount})
                else:
                    fixed: Dict = {}
                    for raw_key, val in raw.items():
                        mapped = _map_col(raw_key)
                        if mapped and mapped not in fixed:
                            fixed[mapped] = val
                    fixed["relation_type"] = rel
                    fixed["location"]      = _location(name, raw)
                    company_rows.append((name, fixed))

            for r in change_rows:
                sqlite_change_rows.append((year, tid, sec, r["item"], r["amount"]))
            if change_rows:
                vec_docs.append(_changes_md(year, change_rows, sec))
                vec_metas.append({"year": year, "doc_type": "investment_changes",
                                  "table_id": tid, "section": sec})
                vec_ids.append(f"{tid}_changes")

            company_names = [name for name, _ in company_rows]
            if company_rows:
                vec_docs.append(_table_summary_md(year, rel, sec, headers, company_names))
                vec_metas.append({"year": year, "doc_type": "table_summary", "relation_type": rel,
                                  "table_id": tid, "section": sec, "bold_title": bold})
                vec_ids.append(f"{tid}_summary")

            for ri, (name, fixed) in enumerate(company_rows):
                sqlite_company_rows.append((
                    year, tid, sec, bold, raw_headers_json, header_type, html_path,
                    name, fixed.get("relation_type"),
                    fixed.get("ownership_rate"), fixed.get("location"), fixed.get("industry"),
                    fixed.get("revenue"),        fixed.get("purchase"),
                    fixed.get("receivable"),     fixed.get("payable"),
                    fixed.get("asset_noncurrent_disposal"),
                    fixed.get("investment_acquisition"), fixed.get("investment_disposal"),
                    fixed.get("asset_current"),  fixed.get("asset_noncurrent"),  fixed.get("asset_total"),
                    fixed.get("liability_current"), fixed.get("liability_noncurrent"), fixed.get("liability_total"),
                    fixed.get("revenue_table"),  fixed.get("net_income"),  fixed.get("revenue_is"),
                    fixed.get("profit_continuing"), fixed.get("profit_discontinued"),
                    fixed.get("profit_other_ci"), fixed.get("profit_total_ci"),
                    fixed.get("investment_beginning"), fixed.get("investment_change"), fixed.get("investment_ending"),
                ))
                vec_docs.append(_company_md(year, name, fixed))
                vec_metas.append({
                    "year": year, "doc_type": "company", "relation_type": rel,
                    "table_id": tid, "table_index": c["table_index"],
                    "company_name": name, "location": fixed.get("location") or "",
                    "bold_title": bold, "section": sec,
                })
                vec_ids.append(f"{tid}_r{ri}")

            print(f"  ✓ {rel} | {sec[:50]} | 기업 {len(company_rows)} / 변동 {len(change_rows)}")

        cur = self.db.cursor()
        if sqlite_company_rows:
            cur.executemany("""
                INSERT INTO subsidiaries (
                    year, table_id, source_section, bold_title,
                    raw_headers, header_type, source_html,
                    company_name, relation_type,
                    ownership_rate, location, industry,
                    revenue, purchase, receivable, payable, asset_noncurrent_disposal,
                    investment_acquisition, investment_disposal,
                    asset_current, asset_noncurrent, asset_total,
                    liability_current, liability_noncurrent, liability_total,
                    revenue_table, net_income, revenue_is,
                    profit_continuing, profit_discontinued, profit_other_ci, profit_total_ci,
                    investment_beginning, investment_change, investment_ending
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, sqlite_company_rows)
        if sqlite_change_rows:
            cur.executemany(
                "INSERT INTO investment_changes (year,table_id,source_section,item,amount) VALUES (?,?,?,?,?)",
                sqlite_change_rows)
        self.db.commit()
        if vec_docs:
            self.col.add(documents=vec_docs, metadatas=vec_metas, ids=vec_ids)

        print(f"  → 기업: {len(sqlite_company_rows)} / 투자 변동: {len(sqlite_change_rows)}")
        print(f"  → 벡터DB 배치: {len(vec_docs)}개 문서")

    def process_year(self, html_path: str, year: int):
        print(f"\n{'='*60}\n[표 파싱] {year}년\n{'='*60}")
        try:
            self._parse_tables(self._traverse(html_path, year), year)
        except Exception as e:
            import traceback; print(f"✗ {year}년 실패: {e}"); traceback.print_exc()

    def process_all(self, base_path: str, years: List[int]):
        for year in years:
            path = f"{base_path}/감사보고서_{year}.htm"
            if os.path.exists(path): self.process_year(path, year)
            else: print(f"✗ {year}년 파일 없음")

    def close(self):
        self.db.close()


# ============================================================
# AuditNoteParser — KAM + 관계/종속기업 주석 파싱
# ============================================================

class AuditNoteParser:
    """핵심감사사항 + 종속/관계기업 주석 전용 파서.

    DB 초기화 없음. audit_notes 컬렉션에만 저장.
    AuditReportParser 실행 완료 후 실행.
    """

    def __init__(self, vector_db_path: str, sqlite_db_path: str,
                 embedding_fn: EmbeddingFunction):

        self._chroma = PersistentClient(path=vector_db_path)
        self.col = self._chroma.get_or_create_collection(
            name="audit_notes", embedding_function=embedding_fn)
        self._sqlite_path = sqlite_db_path

        print(f"✓ 벡터DB: {vector_db_path} | audit_notes={self.col.count()}")
        print(f"✓ SQLite 참조: {sqlite_db_path}")

    def _load_table_meta_all_years(self) -> Dict[int, List[Dict]]:
        db = sqlite3.connect(self._sqlite_path)
        db.row_factory = sqlite3.Row
        try:
            rows = db.execute("""
                SELECT DISTINCT year, table_id, source_section, bold_title, relation_type
                FROM subsidiaries ORDER BY year, table_id
            """).fetchall()
        except Exception:
            return {}
        finally:
            db.close()

        result: Dict[int, List[Dict]] = {}
        for r in rows:
            yr = r["year"]
            if yr not in result:
                result[yr] = []
            result[yr].append({
                "table_id":       r["table_id"],
                "source_section": r["source_section"] or "",
                "bold_title":     r["bold_title"]     or "",
                "relation_type":  r["relation_type"]  or "unknown",
            })
        return result

    def _traverse_blocks(self, html_path: str) -> List[Dict]:
        soup   = BeautifulSoup(_read_html(html_path), "lxml")
        blocks: List[Dict] = []
        for tag in (soup.body or soup).find_all(["p", "table"], recursive=True):
            if tag.name == "p":
                if "SECTION-1" in (tag.get("class") or []):
                    continue
                text    = _norm(tag.get_text(" "))
                style   = (tag.get("style") or "").replace(" ", "").lower()
                is_bold = "font-weight:bold" in style or "font-weight:700" in style
                blocks.append({
                    "type":         "p",
                    "text":         text,
                    "is_bold":      is_bold,
                    "section_like": _is_section_like(text, is_bold),
                })
            elif tag.name == "table":
                if "nb" in (tag.get("class") or []):
                    continue
                blocks.append({
                    "type":         "table",
                    "text":         _flatten_table(_parse_merged(tag)),
                    "is_bold":      False,
                    "section_like": False,
                })
        return blocks

    def _extract_kam_blocks(self, blocks: List[Dict]) -> List[Dict]:
        results: List[Dict] = []
        i = 0
        while i < len(blocks):
            b = blocks[i]
            if not any(k in b["text"] for k in KAM_START_KW):
                i += 1
                continue

            section_title = b["text"]
            bold_title    = ""
            kam_blocks    = [b]
            pending_end   = False
            i += 1

            while i < len(blocks):
                b2 = blocks[i]
                if any(k in b2["text"] for k in KAM_END_STRONG):
                    break
                if b2["section_like"] and not any(k in b2["text"] for k in KAM_START_KW):
                    if re.match(r"^([0-9]{1,2}\.|[가-힣]{1,2}\.)", b2["text"]):
                        if pending_end:
                            break
                        pending_end = True
                    else:
                        break
                else:
                    pending_end = False
                if b2["is_bold"] and len(b2["text"]) > 4:
                    bold_title = b2["text"]
                kam_blocks.append(b2)
                i += 1

            results.append({
                "section_title": section_title,
                "bold_title":    bold_title,
                "blocks":        kam_blocks,
            })
        return results

    def _extract_relation_blocks(self, blocks: List[Dict],
                                  table_meta: List[Dict]) -> List[Dict]:
        anchor_sections = {
            _norm_anchor(m["source_section"])
            for m in table_meta if len(m["source_section"]) >= 5
        }
        anchor_bolds = {
            _norm_anchor(m["bold_title"])
            for m in table_meta if len(m["bold_title"]) >= 5
        }

        def _is_start(text: str) -> bool:
            n = _norm_anchor(text)
            if any(a == n or (len(a) >= 8 and a in n) for a in anchor_sections):
                return True
            if any(a == n or (len(a) >= 8 and a in n) for a in anchor_bolds):
                return True
            return any(k in text for k in RELATION_KW)

        def _related_ids(section_title: str, bold_title: str) -> List[str]:
            sn, bn = _norm_anchor(section_title), _norm_anchor(bold_title)
            ids = []
            for m in table_meta:
                mn = _norm_anchor(m["source_section"])
                mb = _norm_anchor(m["bold_title"])
                if (mn and len(mn) >= 5 and (mn == sn or mn in sn or sn in mn)) or \
                   (mb and len(mb) >= 5 and (mb == bn or mb in bn)):
                    ids.append(m["table_id"])
            return list(set(ids))

        results:       List[Dict] = []
        visited_range: set        = set()
        i = 0

        while i < len(blocks):
            if i in visited_range or not _is_start(blocks[i]["text"]):
                i += 1
                continue

            b             = blocks[i]
            section_title = b["text"]
            bold_title    = b["text"] if b["is_bold"] else ""
            rel_blocks    = [b]
            start_i       = i
            i += 1

            while i < len(blocks):
                b2 = blocks[i]
                if any(k in b2["text"] for k in RELATION_END_KW):
                    break
                if b2["section_like"]:
                    if any(k in b2["text"] for k in RELATION_KW):
                        if b2["is_bold"]:
                            bold_title = b2["text"]
                        rel_blocks.append(b2)
                        i += 1
                        continue
                    break
                if b2["is_bold"] and len(b2["text"]) > 4:
                    bold_title = b2["text"]
                rel_blocks.append(b2)
                i += 1

            for idx in range(start_i, i):
                visited_range.add(idx)

            results.append({
                "section_title":     section_title,
                "bold_title":        bold_title,
                "blocks":            rel_blocks,
                "related_table_ids": _related_ids(section_title, bold_title),
            })
        return results

    def _chunk_and_save(self, sections: List[Dict], doc_type: str,
                        year: int, html_path: str) -> int:
        file_hash = _file_hash(html_path)
        vec_docs, vec_metas, vec_ids = [], [], []

        for si, sec in enumerate(sections):
            block_texts = [b["text"] for b in sec["blocks"] if b["text"].strip()]
            for ci, chunk in enumerate(_chunk_text(block_texts)):
                doc_id = f"{doc_type}_{year}_{file_hash}_{si}_{ci}"
                if doc_type == "kam_note":
                    meta = {
                        "year": year, "doc_type": "kam_note",
                        "section_title": sec["section_title"],
                        "bold_title":    sec["bold_title"],
                        "chunk_index":   ci, "source_html": html_path,
                    }
                else:
                    meta = {
                        "year": year, "doc_type": "relation_note",
                        "relation_type":     _get_note_relation_type(sec["section_title"], sec["bold_title"]),
                        "section_title":     sec["section_title"],
                        "bold_title":        sec["bold_title"],
                        "chunk_index":       ci, "source_html": html_path,
                        "related_table_ids": ",".join(sec.get("related_table_ids", [])),
                    }
                vec_docs.append(chunk)
                vec_metas.append(meta)
                vec_ids.append(doc_id)

        if not vec_docs:
            return 0

        failed = 0
        try:
            self.col.add(documents=vec_docs, metadatas=vec_metas, ids=vec_ids)
        except Exception:
            for d, m, vid in zip(vec_docs, vec_metas, vec_ids):
                try:
                    self.col.add(documents=[d], metadatas=[m], ids=[vid])
                except Exception:
                    failed += 1
        if failed:
            print(f"  ⚠ {doc_type} 저장 실패: {failed}개 스킵")
        return len(vec_docs)

    def process_year(self, html_path: str, year: int, table_meta: List[Dict]):
        print(f"\n{'='*60}\n[주석 파싱] {year}년\n{'='*60}")
        try:
            blocks = self._traverse_blocks(html_path)
            print(f"  ✓ 블록 {len(blocks)}개")
            kam_sections      = self._extract_kam_blocks(blocks)
            relation_sections = self._extract_relation_blocks(blocks, table_meta)
            print(f"  ✓ KAM 구간: {len(kam_sections)}개 / 관계/종속 구간: {len(relation_sections)}개")
            kam_count = self._chunk_and_save(kam_sections,      "kam_note",      year, html_path)
            rel_count = self._chunk_and_save(relation_sections, "relation_note", year, html_path)
            print(f"  → KAM chunk: {kam_count} / 관계/종속 chunk: {rel_count}")
        except Exception as e:
            import traceback; print(f"✗ {year}년 실패: {e}"); traceback.print_exc()

    def process_all(self, base_path: str, years: List[int]):
        table_meta_by_year = self._load_table_meta_all_years()
        print(f"✓ table_meta 로딩: {len(table_meta_by_year)}개 연도")
        for year in years:
            path = f"{base_path}/감사보고서_{year}.htm"
            if not os.path.exists(path):
                print(f"✗ {year}년 파일 없음")
                continue
            self.process_year(path, year, table_meta_by_year.get(year, []))
        print(f"\n✓ 완료 | audit_notes={self.col.count()}")


# ============================================================
# 임베딩 함수
# ============================================================

class OllamaEmbeddingFunction(EmbeddingFunction):
    def __init__(self, model_name: str = "qwen3-embedding:4b",
                 ollama_url: str = "http://localhost:11434"):
        import requests as _req
        self.model_name = model_name
        self.ollama_url = ollama_url
        self._req       = _req
        print(f"✓ 임베딩: {model_name} @ {ollama_url}")

    def name(self) -> str:
        return "ollama_qwen3_embedding"

    def __call__(self, input: List[str]) -> List[List[float]]:
        resp = self._req.post(
            f"{self.ollama_url}/api/embed",
            json={"model": self.model_name, "input": list(input)},
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"]


# ============================================================
# 실행
# ============================================================

if __name__ == "__main__":
    VECTOR_PATH = f"{BASE}/chroma_db"
    SQLITE_PATH = f"{BASE}/audit_data.db"
    DATA_PATH   = f"{BASE}/삼성전자_감사보고서_2014_2024"
    YEARS       = list(range(2014, 2025))

    ef = OllamaEmbeddingFunction()

    table_parser = AuditReportParser(
        vector_db_path=VECTOR_PATH,
        sqlite_db_path=SQLITE_PATH,
        embedding_fn=ef,
        reset_db=True,
    )
    table_parser.process_all(DATA_PATH, YEARS)

    cur = table_parser.db.cursor()
    cur.execute("SELECT year, COUNT(*) FROM subsidiaries GROUP BY year ORDER BY year")
    print("\n[subsidiaries]")
    for r in cur.fetchall(): print(f"  {r[0]}년: {r[1]}개")
    cur.execute("SELECT year, COUNT(*) FROM investment_changes GROUP BY year ORDER BY year")
    print("\n[investment_changes]")
    for r in cur.fetchall(): print(f"  {r[0]}년: {r[1]}행")
    print(f"\n[벡터DB] audit_company={table_parser.col.count()}")
    table_parser.close()

    note_parser = AuditNoteParser(
        vector_db_path=VECTOR_PATH,
        sqlite_db_path=SQLITE_PATH,
        embedding_fn=ef,
    )
    note_parser.process_all(DATA_PATH, YEARS)
