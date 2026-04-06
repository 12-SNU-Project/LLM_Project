"""감사보고서 RAG 시스템

검색 구조:
1. 질문 임베딩
2. 요약 문서 검색 → 관련 table_id 후보 확보
3. 해당 table_id의 세부 문서 검색
4. 애매할 때만 리랭커 호출
5. Qwen2.5-14B로 최종 답변 생성

모델:
- 임베딩: Qwen3-Embedding-4B (외부 주입)
- 리랭커: Qwen3-Reranker-0.6B (로컬)
- LLM:    Qwen2.5-14B (Ollama)
"""

import sqlite3
import json
import re
import requests
from typing import List, Dict, Optional
from chromadb import PersistentClient
from chromadb.api.types import EmbeddingFunction


class AuditReportRAG:

    def __init__(
        self,
        sqlite_path: str,
        vector_path: str,
        embedding_fn: EmbeddingFunction,
        llm_model: str = "qwen2.5:14b",
        ollama_url: str = "http://localhost:11434",
    ):
        self.db = sqlite3.connect(sqlite_path)
        self.db.row_factory = sqlite3.Row

        self._chroma = PersistentClient(path=vector_path)
        kw = {"embedding_function": embedding_fn}
        self.col_company = self._chroma.get_or_create_collection("audit_company",       **kw)
        self.col_bs      = self._chroma.get_or_create_collection("audit_balance_sheet", **kw)

        self.llm_model  = llm_model
        self.ollama_url = ollama_url

        print(f"✓ SQLite: {sqlite_path}")
        print(f"✓ 벡터DB: company={self.col_company.count()} bs={self.col_bs.count()}")
        print(f"✓ LLM: {llm_model} @ {ollama_url}")

    # ------------------------------------------------------------------ 리랭커

    def _needs_rerank(self, results: List[Dict]) -> bool:
        return False  # 리랭커 비활성화

    # ------------------------------------------------------------------ 유틸

    def _norm(self, text) -> str:
        return re.sub(r"\s+", "", str(text or "").lower().replace("년도","년"))

    def _extract_year(self, query: str) -> Optional[int]:
        m = re.search(r"(20\d{2})년?", query)
        if m:
            return int(m.group(1))
        m = re.search(r"(?<!\d)(\d{2})년", query)
        if m:
            yy = int(m.group(1))
            if 10 <= yy <= 30:
                return 2000 + yy
        return None

    def _extract_account(self, query: str, year: Optional[int]) -> Optional[str]:
        try:
            sql    = "SELECT DISTINCT account_name FROM balance_sheet"
            params = []
            if year:
                sql += " WHERE year = ?"
                params.append(year)
            accounts = [r[0] for r in self.db.execute(sql, params).fetchall()]
        except Exception:
            return None
        q = self._norm(query)

        def norm_acc(a):
            a = re.sub(r"\s+", "", str(a or "").lower())
            return re.sub(r"^[ⅠⅡⅢⅣⅤⅥⅰⅱⅲⅳⅴⅵ0-9.\-·•]+", "", a)

        candidates = [norm_acc(a) for a in accounts if norm_acc(a) and norm_acc(a) in q]
        return sorted(set(candidates), key=len, reverse=True)[0] if candidates else None

    # ------------------------------------------------------------------ 검색

    def _search(self, col, query: str, where: Optional[Dict], n: int) -> List[Dict]:
        """벡터DB 검색 → 통일된 결과 포맷 반환"""
        results = col.query(query_texts=[query], n_results=n, where=where)
        return [
            {
                "text":     results["documents"][0][i],
                "metadata": results["metadatas"][0][i],
                "distance": results["distances"][0][i],
            }
            for i in range(len(results["ids"][0]))
        ]

    def _where(self, **kwargs) -> Optional[Dict]:
        """ChromaDB where 조건 생성 - 조건 2개 이상이면 $and로 감쌈"""
        conditions = {k: v for k, v in kwargs.items() if v is not None}
        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions
        return {"$and": [{k: v} for k, v in conditions.items()]}

    def _search_company(self, query: str) -> List[Dict]:
        """회사표 2단계 검색

        1단계: 요약 문서로 관련 table_id 후보 확보
        2단계: 해당 table_id의 행 문서 검색
        """
        year = self._extract_year(query)

        # 연도 없으면 최신 연도 사용
        if not year:
            row = self.db.execute("SELECT MAX(year) FROM subsidiaries").fetchone()
            year = row[0] if row else None

        # 1단계: 요약 문서 검색
        summaries = self._search(self.col_company, query,
                                 self._where(year=year, doc_type="summary"), n=3)
        if not summaries:
            return []

        # 관련 table_id 추출
        table_ids = [s["metadata"].get("table_id") for s in summaries if s.get("metadata")]

        # 2단계: 해당 table_id 행 문서 검색
        rows = []
        for tid in table_ids:
            rows += self._search(self.col_company, query,
                                 self._where(table_id=tid, doc_type="row"), n=20)

        if self._needs_rerank(rows):
            rows = self._rerank(query, rows)

        return rows[:15]

    def _search_bs(self, query: str) -> List[Dict]:
        """재무상태표 2단계 검색

        1단계: 요약 문서로 관련 table_id 후보 확보
        2단계: 해당 table_id의 계정 문서 검색
        """
        year    = self._extract_year(query)
        account = self._extract_account(query, year)

        # 1단계: 요약 문서 검색
        summaries = self._search(self.col_bs, query,
                                 self._where(year=year, doc_type="summary", is_primary=1), n=2)
        if not summaries:
            return []

        table_ids = [s["metadata"].get("table_id") for s in summaries if s.get("metadata")]

        # 2단계: 계정 문서 검색
        results = []
        for tid in table_ids:
            results += self._search(self.col_bs, query,
                                    self._where(table_id=tid, doc_type="account"), n=10)

        # 계정명 필터
        if account:
            filtered = [r for r in results
                        if r.get("metadata", {}).get("account_name", "").startswith(account)]
            if filtered:
                results = filtered

        if self._needs_rerank(results):
            results = self._rerank(query, results)

        return results[:10]

    # ------------------------------------------------------------------ 쿼리 분류

    def _classify(self, query: str) -> str:
        q = query.lower()
        if any(re.search(p, q) for p in [
            r"종속기업|관계기업|공동기업", r"지분율|소재지",
            r"(제일|가장|상위|top|큰|높은|많은).*(매출|자산)",
            r"(매출|자산).*(제일|가장|상위|top|큰|높은|많은)",
            r"samsung|삼성",  # 회사명 있으면 company
        ]):
            return "company"
        if any(re.search(p, q) for p in [
            r"얼마|금액|수치", r"자산총계|부채총계|자본총계|순이익|영업이익",
            r"유동자산|비유동자산|유동부채|비유동부채",
        ]):
            return "balance_sheet"
        return "company" if "회사" in q or "기업" in q else "company"

    # ------------------------------------------------------------------ 컨텍스트

    def _build_context(self, query: str) -> str:
        qtype = self._classify(query)
        docs  = self._search_company(query) if qtype == "company" else self._search_bs(query)

        if not docs:
            return ""

        lines = [f"=== {'종속기업/관계기업' if qtype == 'company' else '재무상태표'} 검색 결과 ==="]
        for d in docs:
            lines.append(d["text"][:300])
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------ LLM

    def _call_llm(self, question: str, context: str, source_year: Optional[int]) -> str:
        year_note = f"(※ {source_year}년 감사보고서 기준)" if source_year else ""
        prompt = (
            "당신은 삼성전자 감사보고서 전문 분석가입니다. "
            "주어진 데이터만 근거로 질문에 정확하고 간결하게 답변하세요.\n\n"
            f"**질문:** {question}\n\n"
            f"**참고 데이터:**\n{context}\n\n"
            "**답변 지침:**\n"
            "- 주어진 데이터만 근거로 답변\n"
            "- 숫자는 정확하게 (단위: 백만원)\n"
            f"- 답변 마지막에 반드시 '{year_note}' 표시\n"
            "- 간결하게\n\n"
            "**답변:**"
        )
        try:
            resp = requests.post(
                f"{self.ollama_url}/api/generate",
                json={"model": self.llm_model, "prompt": prompt, "stream": False,
                      "options": {"num_predict": 300, "temperature": 0.3}},
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()["response"]
            return f"[LLM 오류] {resp.status_code}"
        except requests.exceptions.Timeout:
            return "[LLM 오류] 타임아웃. 연도를 명시해서 다시 질문해주세요."
        except Exception as e:
            return f"[LLM 오류] {e}"

    # ------------------------------------------------------------------ 인터페이스

    def ask(self, question: str) -> str:
        qtype   = self._classify(question)
        context = self._build_context(question)
        if not context:
            return "관련 데이터를 찾지 못했습니다."

        # 답변에 표시할 연도 추출
        source_year = self._extract_year(question)
        if not source_year:
            row = self.db.execute("SELECT MAX(year) FROM subsidiaries").fetchone()
            source_year = row[0] if row else None

        return self._call_llm(question, context, source_year)

    def close(self):
        self.db.close()


# ============================================================
# 임베딩 함수 (parser.py와 동일)
# ============================================================

class Qwen3EmbeddingFunction(EmbeddingFunction):
    """Qwen3-Embedding-4B HuggingFace 로컬 로드"""

    def __init__(self, model_name="Qwen/Qwen3-Embedding-4B", device="cpu"):
        from sentence_transformers import SentenceTransformer
        self.model = SentenceTransformer(model_name, device=device)
        print(f"✓ 임베딩 모델: {model_name} ({device})")

    def name(self) -> str:
        return "qwen3_embedding"

    def __call__(self, input):
        return self.model.encode(list(input), normalize_embeddings=True).tolist()


# ============================================================
# 실행
# ============================================================

if __name__ == "__main__":

    ef = Qwen3EmbeddingFunction(
        model_name="Qwen/Qwen3-Embedding-4B",
        device="mps",
    )

    rag = AuditReportRAG(
        sqlite_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/audit_data.db",
        vector_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/chroma_sections",
        embedding_fn=ef,
        llm_model="qwen2.5:14b",
    )

    print("\n" + "="*60)
    print("RAG 시스템 | 종료: 'exit'")
    print("="*60)

    while True:
        try:
            q = input("\n질문: ").strip()
            if q.lower() in ["exit", "quit", "종료"]:
                break
            if not q:
                continue
            print(f"\n답변:\n{rag.ask(q)}")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"오류: {e}")

    rag.close()