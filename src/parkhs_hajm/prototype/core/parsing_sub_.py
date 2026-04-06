"""감사보고서 통합 파서

[실행 순서]
  1. AuditReportParser  — 종속/관계/공동기업 표 파싱
     → SQLite (subsidiaries, investment_changes)
     → 벡터DB audit_company 컬렉션

  2. AuditNoteParser    — KAM + 관계/종속기업 주석 파싱
     → 벡터DB audit_notes 컬렉션
     → SQLite table_meta를 앵커로 참조 (1 완료 후 실행)

[헤더 처리 수정사항]
  - row1에 HEADER_MAP 키워드가 하나라도 있으면 2단 헤더 확정
    (요약 재무정보 표의 자산/부채/매출액/당기순이익 파싱 지원)
  - "당기"/"당기말" 접두사 → 제거 후 매핑
  - "전기"/"전기말" 포함 헤더 → 해당 컬럼만 스킵

[초기화]
  - reset_db=True: 벡터DB 폴더 삭제 + SQLite 테이블 DROP
  - 이 파일에서만 처리
"""

from bs4 import BeautifulSoup
from chromadb import PersistentClient
from chromadb.api.types import EmbeddingFunction
from typing import List, Dict, Optional, Tuple
import sqlite3, re, os, shutil, json, hashlib


BASE = "LLM_Project/data"


# ============================================================
# 공통 상수
# ============================================================

# 영문 국가/도시명을 한국어 국가명으로 느슨하게 매핑한다.
# 회사명이나 원본 행 데이터에 포함된 문자열을 보고 소재지를 추정할 때 사용한다.
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

# 헤더 → 고정 컬럼 매핑
# 수정2: "손실", "매입등" 추가
# 표 헤더를 내부 고정 컬럼명으로 바꾸기 위한 매핑이다.
# 연도별로 헤더 표현이 조금 달라도 같은 필드로 정규화하기 위해 사용한다.
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
    "손실": "net_income",  # 수정2: 단독 등장 케이스 추가
    "세후중단영업손익": "profit_discontinued", "계속영업손익": "profit_continuing",
    "기타포괄손익": "profit_other_ci", "총포괄손익": "profit_total_ci",
    "기초잔액": "investment_beginning",
    "증감액": "investment_change",
    "기말잔액": "investment_ending",
}

# 회사 목록 표로 볼 수 있는 대표 신호 헤더들이다.
COMPANY_SIGNAL_HEADERS = {"지분율", "소유지분율", "보유지분", "소재지", "소재국", "주사업장", "업종", "기업명"}
# 투자 변동 표의 행 이름에서 자주 나오는 키워드들이다.
CHANGE_KW    = {"기초", "취득", "처분", "손상", "손상환입", "기말", "증감", "증감액"}
# 집계/합계 행을 구분할 때 쓰는 키워드다.
AGGREGATE_KW = {"계", "합계"}

# KAM 관련 상수
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

# 헤더 문자열 비교 전에 공백/괄호/불필요한 접미사를 제거한다.
def _norm_header(h: str) -> str:
    h = re.sub(r"\s+", "", h)
    h = re.sub(r"\(\*\d*\)|[()]", "", h)
    h = re.sub(r"등$", "", h)
    return h


# 원본 헤더를 내부 표준 컬럼명으로 매핑한다.
# 전기 컬럼은 버리고, 당기 접두사는 제거한 뒤 HEADER_MAP 기준으로 찾는다.
def _map_col(raw: str) -> Optional[str]:
    """전기/전기말 → None, 당기/당기말 접두사 제거 후 매핑"""
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


# 문자열 숫자를 float로 바꾼다.
# 쉼표 제거, △를 음수로 처리, (123) 형태도 음수로 해석한다.
def _to_num(s: str) -> Optional[float]:
    s = s.strip().replace(",", "").replace("△", "-").replace(" ", "")
    m = re.fullmatch(r"\((-?\d+(?:\.\d+)?)\)", s)
    if m:
        s = f"-{m.group(1)}"
    try:
        return float(s)
    except ValueError:
        return None


# 회사명과 원본 행 데이터에서 국가명을 찾아 소재지를 추정한다.
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


# 기업 개별 행이 아니라 투자 변동/합계 행인지 판별한다.
def _is_b_row(name: str, raw: Dict) -> bool:
    c = re.sub(r"\s+", "", name)
    if any(k in c for k in CHANGE_KW | AGGREGATE_KW):
        return True
    has_signal    = any(_map_col(k) in {"ownership_rate", "location", "industry"} for k in raw)
    has_only_nums = all(isinstance(v, float) for v in raw.values() if v)
    return has_only_nums and not has_signal


# 감사보고서 HTML은 인코딩이 제각각일 수 있어 순서대로 시도해서 읽는다.
def _read_html(path: str) -> str:
    for enc in ["euc-kr", "cp949", "latin1"]:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except Exception:
            continue
    raise ValueError(f"읽기 실패: {path}")


# rowspan/colspan이 있는 표를 평탄화해서 2차원 배열로 만든다.
def _parse_merged(table) -> List[List[str]]:
    """병합 셀 처리 → 2차원 배열"""
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


# 일반 텍스트 비교용 정규화 함수다. 공백을 1칸으로 줄인다.
def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


# 섹션 제목 앵커 비교용 정규화 함수다. 공백 제거 + 소문자화만 수행한다.
def _norm_anchor(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).lower()


# 같은 연도 파일이라도 파일 경로 기준으로 안정적인 짧은 해시를 만든다.
# 주석 chunk 문서 ID 충돌 방지용이다.
def _file_hash(path: str) -> str:
    return hashlib.md5(path.encode()).hexdigest()[:8]


# ============================================================
# 마크다운 생성 (벡터DB 저장용)
# ============================================================

# 회사 1개 행을 벡터DB에 넣기 좋은 마크다운 문서로 변환한다.
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


# 표 전체를 대표하는 요약 문서를 만든다.
# 개별 회사 문서 외에 "이 표가 어떤 표인지" 검색되도록 하기 위한 용도다.
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


# 투자 변동 행들을 표 형태의 마크다운으로 묶어 저장한다.
def _changes_md(year: int, rows: List[Dict], section: str) -> str:
    lines = [
        f"# {year}년 삼성전자 종속/관계/공동기업 투자 변동",
        f"- 섹션: {section}", "",
        "| 항목 | 당기 |", "|---|---|",
    ]
    for r in rows:
        lines.append(f"| {r['item']} | {r.get('amount', 'null')} |")
    return "\n".join(lines)


# 굵은 문단 중에서 섹션 제목처럼 보이는지 판별한다.
def _is_section_like(text: str, is_bold: bool) -> bool:
    if not is_bold:
        return False
    if any(k in text for k in SECTION_LIKE_KW):
        return True
    if re.match(r"^([0-9]{1,2}\.|[가-힣]{1,2}\.)", text):
        return True
    return False


# 주석 구간의 제목을 보고 종속/관계/공동기업 유형을 추정한다.
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


# 주석 파싱 단계에서 표 자체를 짧은 텍스트 블록으로 축약할 때 사용한다.
def _flatten_table(data: List[List[str]], max_rows: int = 12, max_chars: int = 500) -> str:
    lines = []
    for row in data[:max_rows]:
        line = " | ".join(v for v in row if v.strip())
        if line:
            lines.append(line)
    return "\n".join(lines)[:max_chars]


# 연속된 텍스트 블록을 적당한 길이의 chunk로 묶는다.
# 너무 짧은 chunk는 앞뒤와 합쳐 검색 품질이 떨어지지 않게 한다.
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

# 감사보고서의 표를 읽어 구조화 데이터와 벡터DB 문서를 함께 만드는 파서다.
class AuditReportParser:

        # 표 파싱 결과를 참조하되, 초기화 없이 audit_notes 컬렉션만 준비한다.
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

        # 1단/2단 헤더를 판별하고 실제 데이터가 시작되는 행 번호를 반환한다.
    def _build_headers(self, data: List[List[str]]) -> Tuple[List[str], int, str]:
        """2단 헤더 판정.

        수정1: row1에 HEADER_MAP 키워드가 하나라도 있으면 2단 헤더 확정.
        기존 조건(텍스트 비중 30%)이 통과 못 해도 이 조건이 우선 적용.
        → 요약 재무정보 표의 자산/부채/매출액/당기순이익 파싱 지원.
        """
        if not data:
            return [], 0, "single"
        row0 = data[0]
        if len(data) < 2:
            return row0, 1, "single"
        row1 = data[1]

        # 수정1: row1에 HEADER_MAP 키워드 있으면 2단 헤더 확정
        row1_norms = [_norm_header(v) for v in row1 if v]
        if any(any(key in n for key in HEADER_MAP) for n in row1_norms):
            headers = []
            for h0, h1 in zip(row0, row1):
                if not h0 and not h1:   headers.append("")
                elif not h1 or h0==h1:  headers.append(h0)
                elif not h0:            headers.append(h1)
                else:                   headers.append(f"{h0}_{h1}")
            return headers, 2, "double"

        # 기존 로직
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
            elif not h1 or h0==h1:  headers.append(h0)
            elif not h0:            headers.append(h1)
            else:                   headers.append(f"{h0}_{h1}")
        return headers, 2, "double"

        # 기업명을 담고 있는 컬럼 인덱스를 찾는다.
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

        # 표 주변 문맥과 표 내용 일부를 보고 종속/관계/공동기업 유형을 정한다.
    def _get_relation_type(self, sec: str, bold: str, flat: str) -> str:
        for text in [sec, bold, flat]:
            if "종속기업" in text: return "subsidiary"
            if "관계기업" in text: return "affiliate"
            if "공동기업" in text: return "joint_venture"
        return "unknown"

        # HTML을 위에서 아래로 순회하며 표 후보와 그 직전 문맥을 함께 수집한다.
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

        # 수집한 표 후보를 실제 회사 표/투자 변동 표로 해석해서 저장한다.
        # SQLite에는 정형 데이터로, 벡터DB에는 검색용 문서로 넣는다.
    def _parse_tables(self, candidates: List[Dict], year: int):
        """표 파싱 후 연도 단위 배치로 SQLite/벡터DB 저장"""
        ctx_kw   = {"종속기업", "관계기업", "공동기업"}
        inner_kw = {"기업명"} | ctx_kw

        sqlite_company_rows: List[Tuple] = []
        sqlite_change_rows:  List[Tuple] = []
        vec_docs:  List[str]  = []
        vec_metas: List[Dict] = []
        vec_ids:   List[str]  = []

        for c in candidates:
            # 병합 셀을 먼저 펼쳐야 헤더/행 파싱을 안정적으로 할 수 있다.
            data = _parse_merged(c["table"])
            if len(data) < 2:
                continue
            sec       = c["section_title"]
            bold      = c["bold_title"]
            html_path = c["html_path"]
            tid       = f"company_{year}_{c['table_index']}"

            flat = " ".join(" ".join(r) for r in data[:20])
            # 표 앞 문맥이나 표 내부에 관련 키워드가 없으면 회사 표 후보에서 제외한다.
            if not (any(k in f"{sec} {bold}" for k in ctx_kw) or
                    any(k in flat for k in inner_kw)):
                continue

            # 헤더 구조를 먼저 확정한 뒤, 그 기준으로 각 행을 해석한다.
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
                if not name or len(name) < 2:
                    continue

                # 원본 행을 "헤더: 값" 형태로 읽고 숫자는 가능한 범위에서 float로 변환한다.
                raw: Dict = {}
                for i, v in enumerate(row):
                    if i >= len(headers) or not v.strip() or v.strip() in {"-", "－"}:
                        continue
                    num = _to_num(v)
                    raw[headers[i]] = num if num is not None else v.strip()

                    # 기업 행이 아니라 투자 변동/합계 행으로 판단되면 금액만 뽑아 별도 저장한다.
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
                    # 일반 기업 행은 표준 컬럼명으로 정규화해서 회사 데이터로 저장한다.
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

        # 임베딩 호출이 한 번에 너무 커지지 않도록 배치로 나눠 넣는다.
        # 벡터DB 배치 저장 — 50개씩 나눠서 Ollama 타임아웃 방지
        BATCH = 50
        for batch_start in range(0, len(vec_docs), BATCH):
            self.col.add(
                documents=vec_docs [batch_start:batch_start + BATCH],
                metadatas=vec_metas[batch_start:batch_start + BATCH],
                ids=vec_ids        [batch_start:batch_start + BATCH],
            )

        print(f"  → 기업: {len(sqlite_company_rows)} / 투자 변동: {len(sqlite_change_rows)}")
        print(f"  → 벡터DB 배치: {len(vec_docs)}개 문서")

        # 연도별 표 파싱 진입점이다.
    def process_year(self, html_path: str, year: int):
        print(f"\n{'='*60}\n[표 파싱] {year}년\n{'='*60}")
        try:
            self._parse_tables(self._traverse(html_path, year), year)
        except Exception as e:
            import traceback; print(f"✗ {year}년 실패: {e}"); traceback.print_exc()

        # 전체 연도에 대해 table_meta를 먼저 불러오고 주석 파싱을 순차 실행한다.
    def process_all(self, base_path: str, years: List[int]):
        for year in years:
            path = f"{base_path}/감사보고서_{year}.htm"
            if os.path.exists(path): self.process_year(path, year)
            else: print(f"✗ {year}년 파일 없음")

        # SQLite 연결을 종료한다.
    def close(self):
        self.db.close()


# ============================================================
# AuditNoteParser — KAM + 관계/종속기업 주석 파싱
# ============================================================

# KAM과 관계/종속기업 관련 주석 텍스트를 chunk 단위로 벡터DB에 저장하는 파서다.
class AuditNoteParser:
    """핵심감사사항 + 종속/관계기업 주석 전용 파서.

    DB 초기화 없음. audit_notes 컬렉션에만 저장.
    AuditReportParser 실행 완료 후 실행.
    """

        # 표 파싱 결과를 참조하되, 초기화 없이 audit_notes 컬렉션만 준비한다.
    def __init__(self, vector_db_path: str, sqlite_db_path: str,
                 embedding_fn: EmbeddingFunction):

        self._chroma = PersistentClient(path=vector_db_path)
        self.col = self._chroma.get_or_create_collection(
            name="audit_notes", embedding_function=embedding_fn)
        self._sqlite_path = sqlite_db_path

        print(f"✓ 벡터DB: {vector_db_path} | audit_notes={self.col.count()}")
        print(f"✓ SQLite 참조: {sqlite_db_path}")

        # 표 파싱에서 저장한 메타를 읽어 주석 구간의 시작 앵커로 활용한다.
    def _load_table_meta_all_years(self) -> Dict[int, List[Dict]]:
        """subsidiaries 테이블에서 연도별 표 메타 로딩 (process_all에서 한 번만)"""
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

        # HTML 전체를 문단/표 블록 단위로 평탄화한다.
    def _traverse_blocks(self, html_path: str) -> List[Dict]:
        """HTML 위→아래 순회 → 블록 리스트 생성 (표는 앞 12행, 최대 500자)"""
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

        # 핵심감사사항 시작/종료 규칙을 이용해 KAM 구간을 추출한다.
    def _extract_kam_blocks(self, blocks: List[Dict]) -> List[Dict]:
        """KAM 구간 추출.

        종료:
          1. KAM_END_STRONG → 즉시
          2. section_like + 비번호 대제목 → 즉시
          3. 번호 패턴 bold → pending_end 두 번 연속이면 종료
        """
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
                    # 일반 기업 행은 표준 컬럼명으로 정규화해서 회사 데이터로 저장한다.
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

        # 종속/관계/공동기업 관련 주석 구간을 추출한다.
        # 표 메타에서 읽은 제목 앵커를 우선 활용하고, 키워드는 보조 신호로 쓴다.
    def _extract_relation_blocks(self, blocks: List[Dict],
                                  table_meta: List[Dict]) -> List[Dict]:
        """관계/종속기업 주석 구간 추출.

        시작: table_meta 앵커 우선 (5자 이상) → RELATION_KW fallback
        종료: RELATION_END_KW 즉시 / 비관련 대제목
        중복: 구간 끝 인덱스까지 visited 처리
        """
        anchor_sections = {
            _norm_anchor(m["source_section"])
            for m in table_meta if len(m["source_section"]) >= 5
        }
        anchor_bolds = {
            _norm_anchor(m["bold_title"])
            for m in table_meta if len(m["bold_title"]) >= 5
        }

            # 현재 블록이 관련 주석 구간의 시작점인지 판별한다.
        def _is_start(text: str) -> bool:
            n = _norm_anchor(text)
            if any(a == n or (len(a) >= 8 and a in n) for a in anchor_sections):
                return True
            if any(a == n or (len(a) >= 8 and a in n) for a in anchor_bolds):
                return True
            return any(k in text for k in RELATION_KW)

            # 주석 제목과 비슷한 표 메타를 찾아 연결된 table_id 목록을 만든다.
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

        # 추출한 구간을 chunk로 나눈 뒤 벡터DB에 저장한다.
    def _chunk_and_save(self, sections: List[Dict], doc_type: str,
                        year: int, html_path: str) -> int:
        """구간 → chunk 분할 → 벡터DB 배치 저장 (doc_id에 파일 해시 포함)"""
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
                    # 일반 기업 행은 표준 컬럼명으로 정규화해서 회사 데이터로 저장한다.
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

        # 배치 저장 실패 시에는 개별 재시도로 일부라도 최대한 살린다.
        # 50개씩 나눠서 Ollama 타임아웃 방지
        BATCH  = 50
        failed = 0
        for batch_start in range(0, len(vec_docs), BATCH):
            try:
                self.col.add(
                    documents=vec_docs [batch_start:batch_start + BATCH],
                    metadatas=vec_metas[batch_start:batch_start + BATCH],
                    ids=vec_ids        [batch_start:batch_start + BATCH],
                )
            except Exception:
                # 배치 실패 시 개별 재시도
                for d, m, vid in zip(
                    vec_docs [batch_start:batch_start + BATCH],
                    vec_metas[batch_start:batch_start + BATCH],
                    vec_ids  [batch_start:batch_start + BATCH],
                ):
                    try:
                        self.col.add(documents=[d], metadatas=[m], ids=[vid])
                    except Exception:
                        failed += 1
        if failed:
            print(f"  ⚠ {doc_type} 저장 실패: {failed}개 스킵")
        return len(vec_docs)

        # 연도별 주석 파싱 진입점이다.
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

        # 전체 연도에 대해 table_meta를 먼저 불러오고 주석 파싱을 순차 실행한다.
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

# Chroma에서 사용할 Ollama 임베딩 래퍼다.
class OllamaEmbeddingFunction(EmbeddingFunction):
        # requests를 지연 import하고, 사용할 모델명과 엔드포인트를 저장한다.
    def __init__(self, model_name: str = "qwen3-embedding:4b",
                 ollama_url: str = "http://localhost:11434"):
        import requests as _req
        self.model_name = model_name
        self.ollama_url = ollama_url
        self._req       = _req
        print(f"✓ 임베딩: {model_name} @ {ollama_url}")

        # 임베딩 함수 식별자다.
    def name(self) -> str:
        return "ollama_qwen3_embedding"

        # Ollama /api/embed를 호출해 여러 입력 문장의 임베딩 벡터를 반환한다.
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

# 스크립트 단독 실행 시 표 파싱 → 주석 파싱 순서로 전체 연도를 처리한다.
if __name__ == "__main__":
    VECTOR_PATH = f"{BASE}/chroma_db"
    SQLITE_PATH = f"{BASE}/audit_data.db"
    DATA_PATH   = f"{BASE}/삼성전자_감사보고서_2014_2024"
    YEARS       = list(range(2014, 2025))

    # 표 파서와 주석 파서가 공통으로 사용할 임베딩 함수다.
    ef = OllamaEmbeddingFunction()

    # 먼저 표를 구조화해야 주석 파서가 table_meta를 앵커로 활용할 수 있다.
    # 1단계: 표 파싱 (reset_db=True → 초기화 후 전체 재실행)
    table_parser = AuditReportParser(
        vector_db_path=VECTOR_PATH,
        sqlite_db_path=SQLITE_PATH,
        embedding_fn=ef,
        reset_db=True,
    )
    table_parser.process_all(DATA_PATH, YEARS)

    # 저장 결과를 연도별 개수로 확인한다.
    cur = table_parser.db.cursor()
    cur.execute("SELECT year, COUNT(*) FROM subsidiaries GROUP BY year ORDER BY year")
    print("\n[subsidiaries]")
    for r in cur.fetchall(): print(f"  {r[0]}년: {r[1]}개")
    cur.execute("SELECT year, COUNT(*) FROM investment_changes GROUP BY year ORDER BY year")
    print("\n[investment_changes]")
    for r in cur.fetchall(): print(f"  {r[0]}년: {r[1]}행")
    print(f"\n[벡터DB] audit_company={table_parser.col.count()}")
    table_parser.close()

    # 표 파싱이 끝난 뒤에만 주석 파싱을 실행해야 관련 표 메타를 참조할 수 있다.
    # 2단계: 주석 파싱 (표 파싱 완료 후 실행)
    note_parser = AuditNoteParser(
        vector_db_path=VECTOR_PATH,
        sqlite_db_path=SQLITE_PATH,
        embedding_fn=ef,
    )
    note_parser.process_all(DATA_PATH, YEARS)