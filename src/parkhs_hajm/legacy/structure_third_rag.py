"""감사보고서 RAG 시스템

흐름:
  ask() → _parse_query() → _search_company() → _search_notes()
       → _extract_direct() → _build_context() → _call_llm()

역할 분리:
  벡터DB : 후보 찾기
  SQLite : 병합·정답 확인
  notes  : 설명 보강

수정사항:
  3. FIELD_MAP 보강 + 매출액 우선순위 수정 (길이 역순 순회)
  4. 국가명 검색 — location 컬럼 조회 추가
  5. 시계열 최근 N년 — SQLite DISTINCT year 직접 조회
  6. LLM 프롬프트 답변 범위 제한
"""

import sqlite3, re, requests
from chromadb import PersistentClient
from chromadb.api.types import EmbeddingFunction
from typing import List, Dict, Optional, Tuple


BASE        = "/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리"
VECTOR_PATH = f"{BASE}/chroma_db"
SQLITE_PATH = f"{BASE}/audit_data.db"
OLLAMA_URL  = "http://localhost:11434"
EMBED_MODEL = "qwen3-embedding:4b"
LLM_MODEL   = "qwen2.5:14b"

# ── 국가명 셋 (location 검색 감지용)
COUNTRY_NAMES = {
    "한국", "미국", "중국", "일본", "베트남", "인도", "브라질", "멕시코",
    "헝가리", "영국", "폴란드", "러시아", "호주", "캐나다", "싱가포르",
    "태국", "인도네시아", "말레이시아", "필리핀", "체코", "슬로바키아",
    "우크라이나", "튀르키예", "이집트", "카자흐스탄", "독일", "프랑스",
    "네덜란드", "스웨덴", "오스트리아", "루마니아", "세르비아", "대만", "홍콩",
}

# ── company_name 추출 금지어
COMPANY_NAME_STOPWORDS = {
    "종속기업", "관계기업", "공동기업", "핵심감사사항", "감사", "자산", "부채",
    "매출", "손상", "평가", "리스크", "위험", "판단", "회계처리", "지분율",
    "소재지", "업종", "투자", "변동", "내부거래", "채권", "채무", "비교",
    "추이", "최신", "최근", "연도", "알려줘", "얼마", "뭐야", "보여줘",
} | COUNTRY_NAMES  # 국가명도 기업명으로 오탐되지 않도록 금지어에 포함

# ── notes 우선 키워드
NOTES_KW = {
    "핵심감사사항", "kam", "감사", "손상", "리스크", "위험", "왜",
    "판단", "평가", "회계처리", "중요한", "이슈",
}

# ── company 우선 키워드
COMPANY_KW = {
    "종속기업", "관계기업", "공동기업", "지분율", "소재지", "업종",
    "매출", "자산", "부채", "투자변동", "취득", "처분",
}

# ── 추이/변화 질문 키워드
TREND_KW = {"변화", "추이", "최근", "증감", "비교", "트렌드"}

# ── field 키워드 → SQLite 컬럼명
# 수정3: 구체적인 키워드를 앞에 배치. 순회 시 길이 역순으로 처리해서 "매출액"이 "매출"보다 먼저 매칭
FIELD_MAP = {
    "비유동자산":    "asset_noncurrent",
    "비유동부채":    "liability_noncurrent",
    "유동자산":      "asset_current",
    "유동부채":      "liability_current",
    "당기순이익":    "net_income",
    "매출액":        "revenue_table",      # "매출"보다 구체적이므로 먼저
    "매출등":        "revenue",
    "매출":          "revenue",
    "매입등":        "purchase",
    "매입":          "purchase",
    "순이익":        "net_income",
    "지분율":        "ownership_rate",
    "소재지":        "location",
    "업종":          "industry",
    "자산":          "asset_total",
    "부채":          "liability_total",
    "채권":          "receivable",
    "채무":          "payable",
    "취득":          "investment_acquisition",
    "처분":          "investment_disposal",
    "기초잔액":      "investment_beginning",
    "기말잔액":      "investment_ending",
    "증감액":        "investment_change",
}

# 항상 기본 노출할 핵심 필드
BASE_FIELDS = ["relation_type", "ownership_rate", "location", "industry"]


def _is_empty(v) -> bool:
    return v is None or str(v).strip() in {"", "null", "unknown", "None"}


def _fmt(v) -> str:
    """숫자 값을 천단위 콤마 정수 문자열로 변환"""
    if _is_empty(v):
        return "null"
    try:
        return f"{int(float(v)):,}"
    except (ValueError, TypeError):
        return str(v)


class OllamaEmbeddingFunction(EmbeddingFunction):
    def __init__(self):
        print(f"✓ 임베딩: {EMBED_MODEL} @ {OLLAMA_URL}")

    def name(self) -> str:
        return "ollama_qwen3_embedding"

    def __call__(self, input: List[str]) -> List[List[float]]:
        resp = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": list(input)},
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"]


class AuditRAG:

    def __init__(self):
        ef = OllamaEmbeddingFunction()

        chroma = PersistentClient(path=VECTOR_PATH)
        self.col_company = chroma.get_or_create_collection(
            "audit_company", embedding_function=ef)
        self.col_notes = chroma.get_or_create_collection(
            "audit_notes", embedding_function=ef)

        self.db = sqlite3.connect(SQLITE_PATH)
        self.db.row_factory = sqlite3.Row

        print(f"✓ audit_company: {self.col_company.count()}")
        print(f"✓ audit_notes:   {self.col_notes.count()}")
        print(f"✓ SQLite: {self.db.execute('SELECT COUNT(*) FROM subsidiaries').fetchone()[0]}행")

    # ------------------------------------------------------------------ 쿼리 파싱

    def _parse_query(self, query: str) -> Dict:
        """질문에서 intent / year / company_name / location / fields 추출"""
        q = query.lower()

        # intent 분류
        has_notes   = any(k in q for k in NOTES_KW)
        has_company = any(k in q for k in COMPANY_KW)
        if has_notes and has_company:
            intent = "mixed"
        elif has_notes:
            intent = "notes"
        else:
            intent = "company"

        # 연도 추출
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

        # 추이/변화 질문 여부
        is_trend = any(k in q for k in TREND_KW)

        # 수정4: 국가명 감지 → location 검색용
        location = next((c for c in COUNTRY_NAMES if c in query), None)

        # company_name 후보 추출 (국가명은 금지어로 걸러짐)
        company_name = None
        eng = re.findall(r"[A-Z][a-zA-Z\s&\-\.\(\)㈜]+", query)
        if eng:
            company_name = max(eng, key=len).strip()
        else:
            kor = re.findall(r"[가-힣㈜]{2,}", query)
            candidates = [t for t in kor if t not in COMPANY_NAME_STOPWORDS and len(t) >= 2]
            if candidates:
                company_name = max(candidates, key=len)

        # 수정3: field 추출 시 길이 역순 순회로 구체적인 키워드 우선 매칭
        fields = []
        seen_cols = set()
        for kw in sorted(FIELD_MAP.keys(), key=len, reverse=True):
            if kw in query:
                col = FIELD_MAP[kw]
                if col not in seen_cols:
                    fields.append(col)
                    seen_cols.add(col)

        return {
            "intent":       intent,
            "year":         year,
            "is_trend":     is_trend,
            "company_name": company_name,
            "location":     location,   # 수정4: 국가명 추가
            "fields":       fields,
        }

    # ------------------------------------------------------------------ company 검색 + 병합

    def _search_company(self, query: str, parsed: Dict) -> List[Dict]:
        """벡터 검색 → SQLite 병합 → 최종 기업 리스트 반환"""
        year         = parsed["year"]
        company_name = parsed["company_name"]
        location     = parsed["location"]
        is_trend     = parsed["is_trend"]

        # 수정4: 국가명 질문이면 location 컬럼으로 직접 SQLite 조회
        if location and not company_name:
            return self._search_by_location(location, year, is_trend)

        # 벡터 검색 1단계: doc_type=company + year 필터
        where = {"$and": [{"doc_type": {"$eq": "company"}}, {"year": {"$eq": year}}]} if year \
                else {"doc_type": {"$eq": "company"}}
        res = self.col_company.query(query_texts=[query], n_results=10, where=where)

        candidates: List[Tuple[str, int]] = []
        for i in range(len(res["ids"][0])):
            meta = res["metadatas"][0][i]
            candidates.append((meta.get("company_name", ""), meta.get("year", 0)))

        # fallback: 결과 부족하면 year 필터 없이 재검색
        if len(candidates) < 3:
            res2 = self.col_company.query(
                query_texts=[query], n_results=10,
                where={"doc_type": {"$eq": "company"}})
            for i in range(len(res2["ids"][0])):
                meta = res2["metadatas"][0][i]
                pair = (meta.get("company_name", ""), meta.get("year", 0))
                if pair not in candidates:
                    candidates.append(pair)

        # 재정렬: company_name 포함 + 최신 연도 가산점
        def score(pair):
            name, yr = pair
            s = 0
            if company_name and company_name in name: s += 10
            if company_name and name in company_name: s += 5
            s += yr / 1000
            return s

        candidates = sorted(set(candidates), key=score, reverse=True)[:5]

        # SQLite 조회 + 병합
        merged_list: List[Dict] = []
        for cname, yr in candidates:
            if not cname:
                continue

            # 수정5: 추이 질문이면 SQLite에서 연속 연도 직접 조회
            if is_trend and not year:
                years = self._get_trend_years(cname, n=3)
                for ty in years:
                    rows = self._sqlite_query(cname, ty)
                    if rows:
                        merged_list.append(self._merge_rows(rows))
                continue

            rows = self._sqlite_query(cname, yr if yr else year)
            if not rows:
                continue
            merged_list.append(self._merge_rows(rows))

        # 추이 아닌 경우: year 없으면 최신 1개만
        if not year and not is_trend and merged_list:
            max_yr     = self.db.execute("SELECT MAX(year) FROM subsidiaries").fetchone()[0]
            merged_list = [r for r in merged_list if r.get("year") == max_yr]

        return merged_list

    def _search_by_location(self, location: str, year: Optional[int],
                            is_trend: bool) -> List[Dict]:
        """수정4: 국가명으로 location 컬럼 직접 조회"""
        def fetch(sql, params):
            return [dict(r) for r in self.db.execute(sql, params).fetchall()]

        if year:
            rows = fetch(
                "SELECT * FROM subsidiaries WHERE location = ? AND year = ? ORDER BY company_name",
                [location, year])
        else:
            max_yr = self.db.execute("SELECT MAX(year) FROM subsidiaries").fetchone()[0]
            target_yr = max_yr
            rows = fetch(
                "SELECT * FROM subsidiaries WHERE location = ? AND year = ? ORDER BY company_name",
                [location, target_yr])

        if not rows:
            return []

        # company_name + year 기준으로 병합
        from collections import defaultdict
        by_company: Dict = defaultdict(list)
        for r in rows:
            by_company[r["company_name"]].append(r)

        return [self._merge_rows(v) for v in by_company.values()]

    def _get_trend_years(self, company_name: str, n: int = 3) -> List[int]:
        """수정5: SQLite에서 연속된 최근 N개 연도 직접 조회"""
        rows = self.db.execute(
            "SELECT DISTINCT year FROM subsidiaries WHERE company_name LIKE ? ORDER BY year DESC LIMIT ?",
            [f"%{company_name}%", n]
        ).fetchall()
        return [r[0] for r in rows]

    def _sqlite_query(self, company_name: str, year: Optional[int]) -> List[Dict]:
        """정확 매칭 우선, 없으면 LIKE, 그래도 없으면 MAX(year) fallback"""
        def fetch(sql, params):
            return [dict(r) for r in self.db.execute(sql, params).fetchall()]

        if year:
            rows = fetch("SELECT * FROM subsidiaries WHERE company_name = ? AND year = ?",
                         [company_name, year])
            if rows: return rows
            rows = fetch("SELECT * FROM subsidiaries WHERE company_name LIKE ? AND year = ?",
                         [f"%{company_name}%", year])
            if rows: return rows

        max_year = self.db.execute("SELECT MAX(year) FROM subsidiaries").fetchone()[0]
        rows = fetch("SELECT * FROM subsidiaries WHERE company_name LIKE ? AND year = ?",
                     [f"%{company_name}%", max_year])
        if rows: return rows
        rows = fetch("SELECT * FROM subsidiaries WHERE company_name LIKE ? ORDER BY year DESC LIMIT 20",
                     [f"%{company_name}%"])
        return rows

    def _merge_rows(self, rows: List[Dict]) -> Dict:
        """company_name + year 기준 row 병합"""
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

    # ------------------------------------------------------------------ notes 검색

    def _search_notes(self, query: str, parsed: Dict,
                      company_results: List[Dict]) -> List[Dict]:
        """audit_notes 검색. intent에 따라 top_k 조정"""
        intent = parsed["intent"]
        top_k  = 5 if intent == "notes" else (3 if intent == "mixed" else 2)

        enhanced_query = query
        if company_results:
            names = list({r.get("company_name", "") for r in company_results if r.get("company_name")})
            years = list({str(r.get("year", ""))    for r in company_results if r.get("year")})
            enhanced_query = f"{query} {' '.join(names[:2])} {' '.join(years[:1])}"

        year  = parsed["year"]
        where = {"year": {"$eq": year}} if year else None
        res   = self.col_notes.query(
            query_texts=[enhanced_query], n_results=top_k, where=where)

        return [
            {
                "doc_type":      res["metadatas"][0][i].get("doc_type", ""),
                "section_title": res["metadatas"][0][i].get("section_title", ""),
                "bold_title":    res["metadatas"][0][i].get("bold_title", ""),
                "year":          res["metadatas"][0][i].get("year", ""),
                "text":          res["documents"][0][i][:400],
                "distance":      res["distances"][0][i],
            }
            for i in range(len(res["ids"][0]))
        ]

    # ------------------------------------------------------------------ 직접 추출

    def _extract_direct(self, merged_list: List[Dict], fields: List[str]) -> List[Dict]:
        """병합된 row에서 field 값 직접 추출. 질문 field 우선, BASE_FIELDS 자동 포함"""
        target_cols = list(dict.fromkeys(fields + BASE_FIELDS))
        result = []
        for merged in merged_list:
            extracted = {
                "company_name": merged.get("company_name", ""),
                "year":         merged.get("year", ""),
            }
            for col in target_cols:
                val = merged.get(col)
                if not _is_empty(val):
                    extracted[col] = val
            result.append(extracted)
        return result

    # ------------------------------------------------------------------ 컨텍스트 구성

    def _build_context(self, query: str, merged_list: List[Dict],
                       notes: List[Dict], direct: List[Dict],
                       parsed: Dict) -> str:
        """LLM용 마크다운 컨텍스트 고정 구조"""
        parts = [f"## 질문\n{query}\n"]

        # 확인된 수치
        if direct:
            parts.append("## 확인된 수치")
            for d in direct:
                name = d.get("company_name", "")
                yr   = d.get("year", "")
                vals = {k: v for k, v in d.items() if k not in {"company_name", "year"}}
                if vals:
                    parts.append(f"### {name} ({yr}년)")
                    for k, v in vals.items():
                        parts.append(f"- {k}: {v}")
            parts.append("")

        # 회사 검색 결과
        if merged_list:
            parts.append("## 회사 검색 결과")
            rel_kor = {"subsidiary": "종속기업", "affiliate": "관계기업",
                       "joint_venture": "공동기업", "unknown": "미분류"}
            for m in merged_list[:10]:
                name = m.get("company_name", "")
                yr   = m.get("year", "")
                parts.append(f"### {name} ({yr}년)")
                parts.append(f"- 구분: {rel_kor.get(m.get('relation_type',''), '미분류')}")
                parts.append(f"- 지분율: {_fmt(m.get('ownership_rate'))}%")
                parts.append(f"- 소재지: {m.get('location', 'null')}")
                parts.append(f"- 업종: {m.get('industry', 'null')}")
                parts.append(f"- 자산: {_fmt(m.get('asset_total'))}")
                parts.append(f"- 부채: {_fmt(m.get('liability_total'))}")
                parts.append(f"- 매출액: {_fmt(m.get('revenue_table'))}")
                parts.append(f"- 당기순이익: {_fmt(m.get('net_income'))}")
                parts.append(f"- 채권: {_fmt(m.get('receivable'))}")
                parts.append(f"- 채무: {_fmt(m.get('payable'))}")
                parts.append(f"- 매출(내부거래): {_fmt(m.get('revenue'))}")
                parts.append(f"- 매입(내부거래): {_fmt(m.get('purchase'))}")
            parts.append("")

        # 관련 주석
        if notes:
            parts.append("## 관련 주석")
            for n in notes:
                parts.append(f"### {n.get('section_title','')} ({n.get('year','')}년) — {n.get('bold_title','')}")
                parts.append(n.get("text", ""))
            parts.append("")

        # 수정6: 답변 범위 제한 지침 추가
        fields_asked = parsed.get("fields", [])
        parts.append("## 답변 지침")
        parts.append("- 컨텍스트에 있는 정보만 사용")
        parts.append("- 숫자는 그대로 제시 (단위: 백만원)")
        parts.append("- 데이터 없으면 '데이터 없음'으로 답변")
        parts.append("- 근거 없는 추정 금지")
        if fields_asked:
            # 질문이 특정 필드를 명시한 경우 해당 값만 간결하게 답변
            parts.append(f"- 질문이 특정 항목({', '.join(fields_asked)})을 물었으므로 해당 값만 답변하고 묻지 않은 항목은 언급하지 않음")

        return "\n".join(parts)

    # ------------------------------------------------------------------ 출처 생성

    def _build_sources(self, merged_list: List[Dict], notes: List[Dict]) -> str:
        """참조한 감사보고서 연도 및 출처 정리"""
        sources = []

        table_sources = sorted({
            (r.get("year"), r.get("source_section", "")[:40])
            for r in merged_list if r.get("year")
        })
        for yr, sec in table_sources:
            sources.append(f"  - 삼성전자 {yr}년 감사보고서 — {sec}" if sec
                           else f"  - 삼성전자 {yr}년 감사보고서")

        note_sources = sorted({
            (n.get("year"), n.get("section_title", "")[:40])
            for n in notes if n.get("year")
        })
        for yr, sec in note_sources:
            sources.append(f"  - 삼성전자 {yr}년 감사보고서 주석 — {sec}" if sec
                           else f"  - 삼성전자 {yr}년 감사보고서 주석")

        if not sources:
            return ""
        return "\n\n[출처]\n" + "\n".join(sources)

    # ------------------------------------------------------------------ LLM 호출

    def _call_llm(self, context: str) -> str:
        prompt = (
            "당신은 삼성전자 감사보고서 전문 분석가입니다.\n"
            "아래 컨텍스트만 근거로 질문에 정확하고 간결하게 답변하세요.\n\n"
            f"{context}\n\n**답변:**"
        )
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False,
                      "options": {"num_predict": 500, "temperature": 0.3}},
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()["response"]
            return f"[LLM 오류] {resp.status_code}"
        except requests.exceptions.Timeout:
            return "[LLM 오류] 타임아웃"
        except Exception as e:
            return f"[LLM 오류] {e}"

    # ------------------------------------------------------------------ 통합 인터페이스

    def ask(self, query: str) -> str:
        parsed         = self._parse_query(query)
        company_result = self._search_company(query, parsed)
        notes          = self._search_notes(query, parsed, company_result)
        direct         = self._extract_direct(company_result, parsed["fields"])
        context        = self._build_context(query, company_result, notes, direct, parsed)
        answer         = self._call_llm(context)
        sources        = self._build_sources(company_result, notes)
        return answer + sources

    def close(self):
        self.db.close()


if __name__ == "__main__":
    rag = AuditRAG()
    print("\n" + "="*60)
    print("RAG 시스템 | 종료: exit")
    print("="*60)

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