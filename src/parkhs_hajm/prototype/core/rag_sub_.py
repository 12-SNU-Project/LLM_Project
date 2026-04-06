"""감사보고서 RAG 시스템

[흐름]
  ask()
    → _parse_query()          : 연도 / 약어 / 회사명 / 국가명 / fields 추출
    → _get_company_rows()     : 회사명·약어 있으면 SQLite 직접 조회 우선
                                없으면 벡터DB로 후보 찾은 뒤 SQLite 병합
    → _format_direct()        : fields에 해당하는 값이 모두 있으면 LLM 없이 즉시 반환
    → _search_notes()         : 설명형 질문일 때만 벡터DB notes 검색
    → _call_llm()             : direct로 답 못 낼 때만 호출

[설계 원칙]
  - 회사명/약어 있는 질문 → SQLite 직접 조회, 벡터 검색 건너뜀
  - 숫자 필드 질문 + 값 있음 → LLM 없이 포맷팅 답변
  - 국가명 질문 → location 컬럼 직접 조회
  - 설명형(KAM 등) → 벡터DB notes 검색 + LLM
"""

import sqlite3, re, requests
from chromadb import PersistentClient
from chromadb.api.types import EmbeddingFunction
from typing import List, Dict, Optional
from collections import defaultdict


# 프로젝트 기준 경로/모델 상수
BASE        = "LLM_Project/data"
VECTOR_PATH = f"{BASE}/chroma_db"
SQLITE_PATH = f"{BASE}/audit_data.db"
OLLAMA_URL  = "http://localhost:11434"
EMBED_MODEL = "qwen3-embedding:4b"
LLM_MODEL   = "qwen2.5:7b"


# ── 국가명 (location 검색 감지용)
# 사용자가 회사명 대신 국가명으로 물었을 때 location 기반 조회에 사용
COUNTRY_NAMES = {
    "한국", "미국", "중국", "일본", "베트남", "인도", "브라질", "멕시코",
    "헝가리", "영국", "폴란드", "러시아", "호주", "캐나다", "싱가포르",
    "태국", "인도네시아", "말레이시아", "필리핀", "체코", "슬로바키아",
    "우크라이나", "튀르키예", "이집트", "카자흐스탄", "독일", "프랑스",
    "네덜란드", "스웨덴", "오스트리아", "루마니아", "세르비아", "대만", "홍콩",
}

# ── 회사명 추출 금지어
# 한국어 회사명 후보를 뽑을 때 일반 명사/질문어가 회사명으로 잡히는 것을 막음
COMPANY_NAME_STOPWORDS = {
    "종속기업", "관계기업", "공동기업", "핵심감사사항", "감사", "자산", "부채",
    "매출", "손상", "평가", "리스크", "위험", "판단", "회계처리", "지분율",
    "소재지", "업종", "투자", "변동", "내부거래", "채권", "채무", "비교",
    "추이", "최근", "연도", "알려줘", "얼마", "뭐야", "보여줘",
} | COUNTRY_NAMES

# ── notes 우선 키워드 (설명형 질문 감지)
# 숫자/목록보다 주석 설명 검색이 더 적합한 질문을 판별할 때 사용
NOTES_KW = {
    "핵심감사사항", "kam", "감사의견", "손상", "리스크", "위험", "왜",
    "판단", "평가", "회계처리", "중요한", "이슈",
}

# ── 추이/변화 질문 키워드
# 최근 3개 연도 데이터를 모아야 하는 질문 감지용
TREND_KW = {"변화", "추이", "최근", "증감", "비교", "트렌드"}

# ── field 키워드 → SQLite 컬럼명 (길이 역순으로 순회해서 구체적인 것 우선)
# 사용자의 질문 문구를 내부 컬럼명으로 바꾸기 위한 사전
FIELD_MAP = {
    "비유동자산":   "asset_noncurrent",
    "비유동부채":   "liability_noncurrent",
    "유동자산":     "asset_current",
    "유동부채":     "liability_current",
    "당기순이익":   "net_income",
    "매출액":       "revenue_table",
    "매출등":       "revenue",
    "매출":         "revenue",
    "매입등":       "purchase",
    "매입":         "purchase",
    "순이익":       "net_income",
    "지분율":       "ownership_rate",
    "소재지":       "location",
    "업종":         "industry",
    "자산":         "asset_total",
    "부채":         "liability_total",
    "채권":         "receivable",
    "채무":         "payable",
    "취득":         "investment_acquisition",
    "처분":         "investment_disposal",
    "기초잔액":     "investment_beginning",
    "기말잔액":     "investment_ending",
    "증감액":       "investment_change",
}

# ── 컨텍스트에 항상 포함할 기본 필드
# 현재 직접 사용되진 않지만, 공통 컨텍스트 설계용 기본 필드 목록
BASE_FIELDS = ["relation_type", "ownership_rate", "location", "industry"]

# ── SQLite 컬럼명 → 한국어 표시명
# direct 답변/LLM 컨텍스트에서 사람이 읽는 라벨로 변환할 때 사용
COL_LABEL = {
    "asset_total":            "자산",
    "asset_current":          "유동자산",
    "asset_noncurrent":       "비유동자산",
    "liability_total":        "부채",
    "liability_current":      "유동부채",
    "liability_noncurrent":   "비유동부채",
    "revenue_table":          "매출액",
    "net_income":             "당기순이익(손실)",
    "revenue":                "매출(내부거래)",
    "purchase":               "매입(내부거래)",
    "receivable":             "채권",
    "payable":                "채무",
    "ownership_rate":         "지분율",
    "location":               "소재지",
    "industry":               "업종",
    "relation_type":          "구분",
    "investment_acquisition": "취득",
    "investment_disposal":    "처분",
    "investment_beginning":   "기초잔액",
    "investment_ending":      "기말잔액",
    "investment_change":      "증감액",
}

# relation_type 내부값을 한글로 출력하기 위한 매핑
REL_KOR = {
    "subsidiary":   "종속기업",
    "affiliate":    "관계기업",
    "joint_venture":"공동기업",
    "unknown":      "미분류",
}


# 값이 사실상 비어 있는지 판정하는 공통 유틸
# 문자열 null/unknown 도 빈값으로 취급

def _is_empty(v) -> bool:
    return v is None or str(v).strip() in {"", "null", "unknown", "None"}


# 숫자 출력 포맷 통일용
# 정수형 콤마 포맷으로 보여주고, 비어 있으면 null 반환

def _fmt(v) -> str:
    """숫자 → 천단위 콤마 정수. 비어있으면 'null'."""
    if _is_empty(v):
        return "null"
    try:
        return f"{int(float(v)):,}"
    except (ValueError, TypeError):
        return str(v)


# Chroma에서 사용할 Ollama 임베딩 함수 래퍼
class OllamaEmbeddingFunction(EmbeddingFunction):
    def __init__(self):
        print(f"✓ 임베딩: {EMBED_MODEL} @ {OLLAMA_URL}")

    def name(self) -> str:
        return "ollama_qwen3_embedding"

    def __call__(self, input: List[str]) -> List[List[float]]:
        # Ollama embed API 호출
        resp = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": list(input)},
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"]


# 감사보고서 질의 응답 메인 클래스
# SQLite + Chroma + LLM을 묶어서 ask() 하나로 응답
class AuditRAG:

    def __init__(self):
        # 임베딩 함수 준비
        ef = OllamaEmbeddingFunction()

        # 벡터DB 컬렉션 연결
        chroma = PersistentClient(path=VECTOR_PATH)
        self.col_company = chroma.get_or_create_collection(
            "audit_company", embedding_function=ef)
        self.col_notes = chroma.get_or_create_collection(
            "audit_notes", embedding_function=ef)

        # 구조화 데이터(SQLite) 연결
        self.db = sqlite3.connect(SQLITE_PATH)
        self.db.row_factory = sqlite3.Row

        # max(year) 반복 조회를 줄이기 위한 캐시
        self._max_year: Optional[int] = None  # 캐시

        print(f"✓ audit_company: {self.col_company.count()}")
        print(f"✓ audit_notes:   {self.col_notes.count()}")
        print(f"✓ SQLite subsidiaries: "
              f"{self.db.execute('SELECT COUNT(*) FROM subsidiaries').fetchone()[0]}행")

    # ------------------------------------------------------------------ 유틸

    def _get_max_year(self) -> int:
        # subsidiaries 테이블에서 최신 연도 1회 조회 후 캐시
        if self._max_year is None:
            self._max_year = self.db.execute(
                "SELECT MAX(year) FROM subsidiaries").fetchone()[0]
        return self._max_year

    def _fetch(self, sql: str, params: list) -> List[Dict]:
        # 공통 SQL 실행 결과를 dict 리스트로 변환
        return [dict(r) for r in self.db.execute(sql, params).fetchall()]

    def _merge_rows(self, rows: List[Dict]) -> Dict:
        """여러 row를 하나로 병합. relation_type 명확한 것 우선, null 적은 것 우선."""
        # 같은 회사/연도에서 여러 행이 잡힐 수 있으므로 정보가 많은 행 중심으로 병합
        rows = sorted(rows, key=lambda r: (
            0 if r.get("relation_type") not in ("unknown", None, "") else 1,
            -sum(1 for v in r.values() if not _is_empty(v)),
        ))
        merged: Dict = {}
        for row in rows:
            for col, val in row.items():
                if col not in merged or _is_empty(merged[col]):
                    if not _is_empty(val):
                        merged[col] = val
        return merged

    # ------------------------------------------------------------------ 쿼리 파싱

    def _parse_query(self, query: str) -> Dict:
        """질문에서 연도 / 약어 / 회사명 / 국가명 / fields / intent 추출"""
        q = query.lower()

        # 연도
        # 2024년 / 24년 형태 둘 다 지원
        year = None
        m = re.search(r"(20\d{2})년?", query)
        if m:
            year = int(m.group(1))
        else:
            m = re.search(r"(?<!\d)(\d{2})년", query)
            if m:
                yy = int(m.group(1))
                if 10 <= yy <= 30:
                    year = 2000 + yy

        # 괄호 안 약어 (SIEL, SEA, SEUK 등 2~6자 대문자)
        abbr = None
        abbr_m = re.search(r"\(([A-Z]{2,6})\)", query)
        if abbr_m:
            abbr = abbr_m.group(1)

        # 회사명 — 약어 괄호 포함 전체 구간을 우선 추출
        company_name = None
        if abbr:
            # "Samsung India Electronics Private Ltd. (SIEL)" 같은 패턴 전체 추출
            cn_m = re.search(r"[A-Z][a-zA-Z\s&\-\.]+\(" + abbr + r"\)", query)
            if cn_m:
                company_name = cn_m.group(0).strip()
        if not company_name:
            # 일반 영문 회사명 추출
            eng = re.findall(r"[A-Z][a-zA-Z\s&\-\.]+(?:\([A-Z]{2,6}\))?", query)
            if eng:
                company_name = max(eng, key=len).strip()
            else:
                # 한글 회사명 후보 추출 후 금지어 제거
                kor = re.findall(r"[가-힣㈜]{2,}", query)
                cands = [t for t in kor if t not in COMPANY_NAME_STOPWORDS]
                if cands:
                    company_name = max(cands, key=len)

        # 국가명
        location = next((c for c in COUNTRY_NAMES if c in query), None)

        # intent
        # 설명형 키워드가 있고 회사명/약어가 없으면 notes 질문으로 판단
        has_notes = any(k in q for k in NOTES_KW)
        intent    = "notes" if has_notes and not company_name and not abbr else "company"

        # 추이
        is_trend = any(k in q for k in TREND_KW)

        # fields — 길이 역순으로 순회해 구체적인 키워드 우선 매칭
        # 예: '자산'보다 '유동자산'을 먼저 잡기 위해 길이 역순 사용
        fields: List[str] = []
        seen: set = set()
        for kw in sorted(FIELD_MAP, key=len, reverse=True):
            if kw in query:
                col = FIELD_MAP[kw]
                if col not in seen:
                    fields.append(col)
                    seen.add(col)

        return {
            "year":         year,
            "abbr":         abbr,
            "company_name": company_name,
            "location":     location,
            "intent":       intent,
            "is_trend":     is_trend,
            "fields":       fields,
        }

    # ------------------------------------------------------------------ SQLite 조회

    def _sqlite_by_company(self, company_name: Optional[str],
                           abbr: Optional[str],
                           year: Optional[int]) -> List[Dict]:
        """회사명·약어로 SQLite 직접 조회. 정확 → LIKE → 약어 순."""
        # 연도 미지정이면 최신 연도 기준으로 조회
        target_yr = year or self._get_max_year()

        # 1. 약어 정확 매칭 (SIEL → %(SIEL)%)
        if abbr:
            rows = self._fetch(
                "SELECT * FROM subsidiaries WHERE company_name LIKE ? AND year = ?",
                [f"%({abbr})%", target_yr])
            if rows:
                return rows

        # 2. 회사명 정확 매칭
        if company_name:
            rows = self._fetch(
                "SELECT * FROM subsidiaries WHERE company_name = ? AND year = ?",
                [company_name, target_yr])
            if rows:
                return rows

            # 3. 회사명 LIKE
            rows = self._fetch(
                "SELECT * FROM subsidiaries WHERE company_name LIKE ? AND year = ?",
                [f"%{company_name}%", target_yr])
            if rows:
                return rows

        return []

    def _sqlite_trend(self, company_name: Optional[str],
                      abbr: Optional[str], n: int = 3) -> List[Dict]:
        """추이 질문용 — 최근 N개 연도 데이터 반환"""
        # 해당 기업의 연도 목록 조회
        if abbr:
            years = [r[0] for r in self.db.execute(
                "SELECT DISTINCT year FROM subsidiaries "
                "WHERE company_name LIKE ? ORDER BY year DESC LIMIT ?",
                [f"%({abbr})%", n]).fetchall()]
        elif company_name:
            years = [r[0] for r in self.db.execute(
                "SELECT DISTINCT year FROM subsidiaries "
                "WHERE company_name LIKE ? ORDER BY year DESC LIMIT ?",
                [f"%{company_name}%", n]).fetchall()]
        else:
            return []

        # 각 연도별 조회 결과를 병합하여 최근 N개 연도 행 반환
        rows = []
        for yr in years:
            yr_rows = self._sqlite_by_company(company_name, abbr, yr)
            if yr_rows:
                rows.append(self._merge_rows(yr_rows))
        return rows

    def _sqlite_by_location(self, location: str,
                            year: Optional[int]) -> List[Dict]:
        """국가명으로 location 컬럼 직접 조회"""
        target_yr = year or self._get_max_year()
        rows = self._fetch(
            "SELECT * FROM subsidiaries WHERE location = ? AND year = ? ORDER BY company_name",
            [location, target_yr])
        if not rows:
            return []

        # 같은 회사가 여러 행일 수 있으므로 회사명 기준 병합
        by_company: Dict = defaultdict(list)
        for r in rows:
            by_company[r["company_name"]].append(r)
        return [self._merge_rows(v) for v in by_company.values()]

    # ------------------------------------------------------------------ 벡터 검색 (회사명 없을 때)

    def _vector_search_company(self, query: str, year: Optional[int]) -> List[Dict]:
        """회사명/약어 없을 때 벡터DB로 후보 찾고 SQLite 병합"""
        # company 문서만 검색, 연도 지정 시 where 조건 추가
        where = {"$and": [{"doc_type": {"$eq": "company"}},
                          {"year":     {"$eq": year}}]} if year \
                else {"doc_type": {"$eq": "company"}}

        res = self.col_company.query(query_texts=[query], n_results=10, where=where)

        # year 필터 결과 부족하면 필터 없이 재검색
        if not res["ids"][0] and year:
            res = self.col_company.query(
                query_texts=[query], n_results=10,
                where={"doc_type": {"$eq": "company"}})

        # 벡터 검색 결과의 company_name/year로 SQLite 원본 행을 다시 가져와 병합
        seen: set = set()
        merged_list: List[Dict] = []
        for i in range(len(res["ids"][0])):
            meta      = res["metadatas"][0][i]
            cname     = meta.get("company_name", "")
            cyr       = meta.get("year", 0)
            if not cname or (cname, cyr) in seen:
                continue
            seen.add((cname, cyr))

            rows = self._fetch(
                "SELECT * FROM subsidiaries WHERE company_name = ? AND year = ?",
                [cname, cyr])
            if rows:
                merged_list.append(self._merge_rows(rows))

        # year 없으면 최신 연도만
        if not year and merged_list:
            max_yr = self._get_max_year()
            merged_list = [r for r in merged_list if r.get("year") == max_yr]

        return merged_list[:5]

    # ------------------------------------------------------------------ 통합 company 조회

    def _get_company_rows(self, query: str, parsed: Dict) -> List[Dict]:
        """회사명/약어 있으면 SQLite 직접 조회, 없으면 벡터 검색"""
        company_name = parsed["company_name"]
        abbr         = parsed["abbr"]
        year         = parsed["year"]
        location     = parsed["location"]
        is_trend     = parsed["is_trend"]

        # 국가명 질문
        if location and not company_name and not abbr:
            return self._sqlite_by_location(location, year)

        # 회사명 또는 약어 있음 → SQLite 직접 조회
        if company_name or abbr:
            if is_trend:
                return self._sqlite_trend(company_name, abbr, n=3)
            rows = self._sqlite_by_company(company_name, abbr, year)
            if rows:
                merged = self._merge_rows(rows)
                return [merged]
            # 해당 연도 없으면 전체 연도에서 최신 1개
            if year:
                rows = self._sqlite_by_company(company_name, abbr, None)
                if rows:
                    return [self._merge_rows(rows)]
            return []

        # 회사명 없음 → 벡터 검색
        return self._vector_search_company(query, year)

    # ------------------------------------------------------------------ notes 검색

    def _search_notes(self, query: str, parsed: Dict,
                      company_rows: List[Dict]) -> List[Dict]:
        """설명형 질문 또는 mixed일 때 audit_notes 검색"""
        intent = parsed["intent"]
        # 회사 숫자 질문은 notes 불필요
        if intent == "company" and (parsed["company_name"] or parsed["abbr"]):
            return []

        # 설명형 질문이면 더 많이, 그 외에는 보조적으로만 적게 검색
        top_k = 5 if intent == "notes" else 2

        # 회사 결과 있으면 쿼리 보강
        enhanced = query
        if company_rows:
            names = list({r.get("company_name", "") for r in company_rows if r.get("company_name")})
            years = list({str(r.get("year", "")) for r in company_rows if r.get("year")})
            enhanced = f"{query} {' '.join(names[:2])} {' '.join(years[:1])}"

        year  = parsed["year"]
        where = {"year": {"$eq": year}} if year else None
        res   = self.col_notes.query(
            query_texts=[enhanced], n_results=top_k, where=where)

        # LLM 컨텍스트에 넣기 쉬운 형태로 정리
        return [
            {
                "section_title": res["metadatas"][0][i].get("section_title", ""),
                "bold_title":    res["metadatas"][0][i].get("bold_title", ""),
                "year":          res["metadatas"][0][i].get("year", ""),
                "text":          res["documents"][0][i][:400],
            }
            for i in range(len(res["ids"][0]))
        ]

    # ------------------------------------------------------------------ direct 포맷팅

    def _format_direct(self, company_rows: List[Dict],
                       fields: List[str]) -> Optional[str]:
        """fields에 해당하는 값이 있으면 LLM 없이 바로 답변 문자열 반환.
        값이 하나라도 없으면 None 반환 → LLM으로 넘김."""
        if not fields or not company_rows:
            return None

        lines = []
        for merged in company_rows:
            name = merged.get("company_name", "")
            yr   = merged.get("year", "")
            header = f"{name} ({yr}년)" if yr else name
            field_lines = []
            for col in fields:
                val = merged.get(col)
                label = COL_LABEL.get(col, col)
                if not _is_empty(val):
                    # relation_type은 한국어로
                    if col == "relation_type":
                        field_lines.append(f"{label}: {REL_KOR.get(str(val), str(val))}")
                    elif col in ("ownership_rate",):
                        field_lines.append(f"{label}: {_fmt(val)}%")
                    elif col in ("location", "industry"):
                        field_lines.append(f"{label}: {val}")
                    else:
                        field_lines.append(f"{label}: {_fmt(val)} 백만원")
                else:
                    field_lines.append(f"{label}: 데이터 없음")
            if field_lines:
                lines.append(f"**{header}**")
                lines.extend(field_lines)

        return "\n".join(lines) if lines else None

    # ------------------------------------------------------------------ LLM

    def _build_context(self, query: str, company_rows: List[Dict],
                       notes: List[Dict], parsed: Dict) -> str:
        # LLM에게 넘길 마크다운 컨텍스트 구성
        parts = [f"## 질문\n{query}\n"]

        if company_rows:
            parts.append("## 회사 데이터")
            for m in company_rows[:10]:
                name = m.get("company_name", "")
                yr   = m.get("year", "")
                parts.append(f"### {name} ({yr}년)")
                parts.append(f"- 구분: {REL_KOR.get(m.get('relation_type',''), '미분류')}")
                parts.append(f"- 지분율: {_fmt(m.get('ownership_rate'))}%")
                parts.append(f"- 소재지: {m.get('location', 'null')}")
                parts.append(f"- 업종: {m.get('industry', 'null')}")
                parts.append(f"- 자산: {_fmt(m.get('asset_total'))} 백만원")
                parts.append(f"- 부채: {_fmt(m.get('liability_total'))} 백만원")
                parts.append(f"- 매출액: {_fmt(m.get('revenue_table'))} 백만원")
                parts.append(f"- 당기순이익(손실): {_fmt(m.get('net_income'))} 백만원")
                parts.append(f"- 채권: {_fmt(m.get('receivable'))} 백만원")
                parts.append(f"- 채무: {_fmt(m.get('payable'))} 백만원")
                parts.append(f"- 매출(내부거래): {_fmt(m.get('revenue'))} 백만원")
                parts.append(f"- 매입(내부거래): {_fmt(m.get('purchase'))} 백만원")
            parts.append("")

        if notes:
            parts.append("## 관련 주석")
            for n in notes:
                parts.append(f"### {n.get('section_title','')} ({n.get('year','')}년)")
                parts.append(n.get("text", ""))
            parts.append("")

        # 답변 형식/범위를 강하게 제한해서 hallucination을 줄임
        fields_asked = parsed.get("fields", [])
        parts.append("## 답변 지침")
        parts.append("- 반드시 한국어로만 답변")
        parts.append("- 컨텍스트에 있는 정보만 사용, 근거 없는 추정 금지")
        parts.append("- 숫자 단위: 백만원")
        parts.append("- 데이터 없으면 '데이터 없음'으로 답변")
        if fields_asked:
            labels = [COL_LABEL.get(f, f) for f in fields_asked]
            parts.append(f"- 질문이 {', '.join(labels)}을(를) 물었으므로 해당 값만 답변, 묻지 않은 항목 언급 금지")

        return "\n".join(parts)

    def _call_llm(self, context: str) -> str:
        # 최종 자연어 답변 생성
        prompt = (
            "당신은 삼성전자 감사보고서 전문 분석가입니다.\n"
            "아래 컨텍스트만 근거로 질문에 정확하고 간결하게 답변하세요.\n"
            "반드시 한국어로만 답변하세요. 다른 언어 사용 금지.\n\n"
            f"{context}\n\n**답변:**"
        )
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False,
                      "options": {"num_predict": 500, "temperature": 0.1}},
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()["response"]
            return f"[LLM 오류] {resp.status_code}"
        except requests.exceptions.Timeout:
            return "[LLM 오류] 타임아웃"
        except Exception as e:
            return f"[LLM 오류] {e}"

    # ------------------------------------------------------------------ 출처

    def _build_sources(self, company_rows: List[Dict],
                       notes: List[Dict]) -> str:
        # 회사 데이터/주석 데이터에서 연도와 섹션명을 뽑아 출처 문자열 구성
        sources = []
        for yr, sec in sorted({
            (r.get("year"), (r.get("source_section") or "")[:40])
            for r in company_rows if r.get("year")
        }):
            sources.append(f"  - 삼성전자 {yr}년 감사보고서" + (f" — {sec}" if sec else ""))
        for yr, sec in sorted({
            (n.get("year"), (n.get("section_title") or "")[:40])
            for n in notes if n.get("year")
        }):
            sources.append(f"  - 삼성전자 {yr}년 감사보고서 주석" + (f" — {sec}" if sec else ""))
        return ("\n\n[출처]\n" + "\n".join(sources)) if sources else ""

    # ------------------------------------------------------------------ 통합 인터페이스

    def ask(self, query: str) -> str:
        # 1) 질문 파싱
        parsed       = self._parse_query(query)
        # 2) 회사 데이터 조회
        company_rows = self._get_company_rows(query, parsed)
        # 3) 필요 시 notes 검색
        notes        = self._search_notes(query, parsed, company_rows)

        # 숫자 필드 질문이고 값이 있으면 LLM 없이 즉시 반환
        direct = self._format_direct(company_rows, parsed["fields"])
        if direct:
            return direct + self._build_sources(company_rows, notes)

        # 데이터 없음
        if not company_rows and not notes:
            return "관련 데이터를 찾지 못했습니다."

        # LLM 호출
        context = self._build_context(query, company_rows, notes, parsed)
        answer  = self._call_llm(context)
        return answer + self._build_sources(company_rows, notes)

    def close(self):
        # SQLite 연결 종료
        self.db.close()


# CLI 실행 진입점
if __name__ == "__main__":
    rag = AuditRAG()
    print("\n" + "="*60)
    print("RAG 시스템 | 종료: exit")
    print("="*60)

    # 터미널 질의 루프
    while True:
        try:
            q = input("\n질문: ").strip()
            if q.lower() in {"exit", "quit", "종료"}:
                break
            if not q:
                continue
            print(f"\n답변:\n{rag.ask(q)}")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"오류: {e}")

    rag.close()
