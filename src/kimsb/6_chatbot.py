import os
import re
import sqlite3
import pickle
import faiss
from sentence_transformers import SentenceTransformer

# ============================================
# 0. 경로 설정
# ============================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = os.path.join(BASE_DIR, "finance.db")
INDEX_PATH = os.path.join(BASE_DIR, "vector.index")
META_PATH = os.path.join(BASE_DIR, "metadata.pkl")


# ============================================
# 1. 공통 유틸
# ============================================
def clean_text(text):
    if text is None:
        return ""
    text = str(text).replace("\xa0", " ")
    text = " ".join(text.split())
    return text.strip()


def extract_years(query):
    return [int(y) for y in re.findall(r"(20\d{2}|19\d{2})", query)]


def normalize_text(text):
    return clean_text(text).replace(" ", "")


def sentence_split(text):
    text = clean_text(text)
    if not text:
        return []
    parts = re.split(r'(?<=[.!?])\s+|(?<=다)\s+|(?<=요)\s+|\n+', text)
    return [clean_text(p) for p in parts if clean_text(p)]


def format_number(value):
    if value is None:
        return "값 없음"

    try:
        if isinstance(value, float) and value.is_integer():
            value = int(value)
    except Exception:
        pass

    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


# ============================================
# 2. 질문 분석
# ============================================
NUMERIC_TRIGGER_WORDS = [
    "얼마", "몇", "금액", "수치", "비율", "증가율", "감소율"
]

ACCOUNT_CANDIDATES = [
    "유동자산", "비유동자산", "자산총계", "자산",
    "유동부채", "비유동부채", "부채총계", "부채",
    "자본총계", "자본",
    "매출액", "매출", "매출총이익", "영업이익",
    "당기순이익", "순이익",
    "현금및현금성자산", "재고자산", "매출채권", "영업권",
    "기타유동자산", "기타비유동자산"
]

LOOKUP_KEYWORDS = [
    "감사의견", "경영진의 책임", "감사인의 책임",
    "핵심감사사항", "주석", "회계정책", "설명", "의미", "내용"
]

LOOKUP_PRIORITY_KEYWORDS = [
    "감사의견", "경영진의 책임", "감사인의 책임", "핵심감사사항", "주석"
]

STATEMENT_KEYWORDS = {
    "재무상태표": ["재무상태표", "상태표"],
    "손익계산서": ["손익계산서", "포괄손익계산서", "손익"],
    "현금흐름표": ["현금흐름표", "현금흐름"],
    "자본변동표": ["자본변동표"]
}


def detect_statement_type_from_query(query):
    query = clean_text(query)
    for statement_type, keywords in STATEMENT_KEYWORDS.items():
        if any(k in query for k in keywords):
            return statement_type
    return None


def detect_account_from_query(query):
    q = normalize_text(query)

    matched = []
    for account in sorted(ACCOUNT_CANDIDATES, key=len, reverse=True):
        if normalize_text(account) in q:
            matched.append(account)

    if not matched:
        return None

    return matched[0]


def classify_query(query):
    query = clean_text(query)

    has_lookup_keyword = any(k in query for k in LOOKUP_KEYWORDS)
    has_numeric_trigger = any(k in query for k in NUMERIC_TRIGGER_WORDS)
    detected_account = detect_account_from_query(query)
    statement_type = detect_statement_type_from_query(query)

    # 설명형 키워드가 있으면 lookup 우선
    if has_lookup_keyword:
        return "lookup"

    # 계정명이 있고, 수치 의도나 재무제표 의도가 있으면 numeric
    if detected_account and (has_numeric_trigger or statement_type):
        return "numeric"

    return "lookup"


# ============================================
# 3. 리소스 로드
# ============================================
def load_resources():
    if not os.path.exists(INDEX_PATH):
        raise FileNotFoundError(f"❌ vector index 파일이 없습니다: {INDEX_PATH}")

    if not os.path.exists(META_PATH):
        raise FileNotFoundError(f"❌ metadata 파일이 없습니다: {META_PATH}")

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"❌ SQLite DB 파일이 없습니다: {DB_PATH}")

    print("🔹 임베딩 모델 로딩 중...")
    embed_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

    print("🔹 FAISS index 로딩 중...")
    index = faiss.read_index(INDEX_PATH)

    print("🔹 metadata 로딩 중...")
    with open(META_PATH, "rb") as f:
        metadata = pickle.load(f)

    print("🔹 SQLite 연결 중...")
    conn = sqlite3.connect(DB_PATH)

    return embed_model, index, metadata, conn


# ============================================
# 4. Numeric 처리
# ============================================
def score_account_match(query_account, db_account):
    if not query_account or not db_account:
        return 0

    q = normalize_text(query_account)
    d = normalize_text(db_account)

    # 정확 일치 최우선
    if q == d:
        return 20

    # 부분 일치는 아주 약하게만 인정
    if q in d or d in q:
        return 1

    return 0


def search_financial_facts(query, conn, top_k=5):
    years = extract_years(query)
    target_year = years[0] if years else None
    query_account = detect_account_from_query(query)
    statement_type = detect_statement_type_from_query(query)

    cursor = conn.cursor()

    sql = """
    SELECT
        file_name,
        doc_year,
        table_id,
        row_id,
        statement_type,
        account_name,
        target_year,
        value,
        raw_row
    FROM financial_facts
    WHERE 1=1
    """
    params = []

    if target_year is not None:
        sql += " AND target_year = ? "
        params.append(target_year)

    if statement_type is not None:
        sql += " AND statement_type = ? "
        params.append(statement_type)

    rows = cursor.execute(sql, params).fetchall()

    results = []
    for row in rows:
        item = {
            "file_name": row[0],
            "doc_year": row[1],
            "table_id": row[2],
            "row_id": row[3],
            "statement_type": clean_text(row[4]),
            "account_name": clean_text(row[5]),
            "target_year": row[6],
            "value": row[7],
            "raw_row": clean_text(row[8])
        }

        score = 0

        if target_year is not None and item["target_year"] == target_year:
            score += 10

        if statement_type is not None and item["statement_type"] == statement_type:
            score += 5

        score += score_account_match(query_account, item["account_name"])

        results.append((score, item))

    results.sort(key=lambda x: x[0], reverse=True)
    final_results = [item for score, item in results if score > 0][:top_k]

    return {
        "query_year": target_year,
        "query_account": query_account,
        "query_statement_type": statement_type,
        "results": final_results
    }


def generate_numeric_answer(query, search_result):
    results = search_result["results"]

    if not results:
        return {
            "answer": "해당 수치를 정확히 찾지 못했습니다. 연도와 계정명을 조금 더 구체적으로 질문해 주세요.",
            "evidence": [],
            "sources": []
        }

    best = results[0]

    account_name = clean_text(best.get("account_name"))
    target_year = best.get("target_year")
    value = best.get("value")
    statement_type = clean_text(best.get("statement_type"))
    file_name = clean_text(best.get("file_name"))

    answer = f"{target_year}년 {account_name}은(는) {format_number(value)}입니다"
    if statement_type:
        answer += f" ({statement_type})"
    answer += "."

    evidence = [f"{account_name} | {target_year} | {format_number(value)}"]
    sources = [file_name] if file_name else []

    return {
        "answer": answer,
        "evidence": evidence[:1],
        "sources": sources[:1]
    }


# ============================================
# 5. Vector 처리
# ============================================
def vector_search(query, embed_model, index, metadata, top_k=12):
    q_emb = embed_model.encode([query], convert_to_numpy=True).astype("float32")
    distances, indices = index.search(q_emb, top_k)

    results = []
    for rank, idx in enumerate(indices[0]):
        if idx < 0 or idx >= len(metadata):
            continue

        item = metadata[idx].copy()
        item["distance"] = float(distances[0][rank])
        results.append(item)

    return results


def rerank_vector_results(query, results):
    query = clean_text(query)
    years = extract_years(query)
    target_year = years[0] if years else None

    reranked = []

    for item in results:
        score = 0
        content = clean_text(item.get("content"))
        section_hint = clean_text(item.get("section_hint"))
        source_type = clean_text(item.get("source_type"))
        file_name = clean_text(item.get("file_name"))
        year = item.get("year")
        distance = item.get("distance", 9999.0)

        # 연도 일치
        if target_year is not None and year == target_year:
            score += 10

        # text source 선호
        if source_type == "text":
            score += 5
        else:
            score -= 3

        # 핵심 lookup 키워드 강하게 반영
        for kw in LOOKUP_PRIORITY_KEYWORDS:
            if kw in query:
                if kw in section_hint:
                    score += 20
                elif kw in content:
                    score += 10
                else:
                    score -= 10

        # 거리 보정
        score += max(0, 8 - distance)

        reranked.append({
            **item,
            "score": score,
            "content": content,
            "section_hint": section_hint,
            "file_name": file_name
        })

    reranked.sort(key=lambda x: x["score"], reverse=True)

    dedup = []
    seen = set()
    for item in reranked:
        key = clean_text(item.get("content"))
        if key and key not in seen:
            seen.add(key)
            dedup.append(item)

    return dedup


def generate_lookup_answer(query, reranked_results):
    if not reranked_results:
        return {
            "answer": "관련 내용을 찾지 못했습니다. 질문을 조금 더 구체적으로 바꿔 주세요.",
            "evidence": [],
            "sources": []
        }

    best = reranked_results[0]
    best_content = clean_text(best.get("content", ""))
    best_section = clean_text(best.get("section_hint", ""))
    best_file = clean_text(best.get("file_name", ""))

    sentences = sentence_split(best_content)
    query_tokens = [tok for tok in re.split(r"\s+", clean_text(query)) if len(tok) >= 2]

    selected = []
    for sent in sentences:
        if any(tok in sent for tok in query_tokens):
            selected.append(sent)
        if len(selected) >= 2:
            break

    if not selected:
        selected = sentences[:2]

    answer_text = " ".join(selected).strip()
    if not answer_text:
        answer_text = best_content[:180]

    if best_section:
        answer_text = f"[{best_section}] {answer_text}"

    preview = best_content[:120]
    if len(best_content) > 120:
        preview += "..."

    evidence = [preview] if preview else []
    sources = [best_file] if best_file else []

    return {
        "answer": answer_text,
        "evidence": evidence[:1],
        "sources": sources[:1]
    }


# ============================================
# 6. 최종 응답
# ============================================
def answer_query(query, conn, embed_model, index, metadata):
    q_type = classify_query(query)

    if q_type == "numeric":
        search_result = search_financial_facts(query, conn, top_k=5)
        response = generate_numeric_answer(query, search_result)
    else:
        vector_results = vector_search(query, embed_model, index, metadata, top_k=12)
        reranked = rerank_vector_results(query, vector_results)
        response = generate_lookup_answer(query, reranked)

    response["query_type"] = q_type
    return response


def print_response(response):
    print("\n" + "=" * 60)
    print(f"질문 유형 : {response['query_type']}")
    print("-" * 60)

    print("답변")
    print(response["answer"])

    if response.get("evidence"):
        print("\n근거")
        for i, ev in enumerate(response["evidence"], 1):
            print(f"{i}. {ev}")

    if response.get("sources"):
        print("\n출처")
        for src in response["sources"]:
            print(f"- {src}")

    print("=" * 60 + "\n")


# ============================================
# 7. 챗봇 실행
# ============================================
def run_chatbot():
    embed_model, index, metadata, conn = load_resources()

    print("🤖 삼성전자 감사보고서 챗봇 시작!")
    print("종료하려면 'exit' 입력\n")

    try:
        while True:
            query = input("👉 질문: ").strip()

            if query.lower() == "exit":
                print("👋 종료합니다.")
                break

            if not query:
                print("질문을 입력해 주세요.\n")
                continue

            response = answer_query(query, conn, embed_model, index, metadata)
            print_response(response)

    finally:
        conn.close()


if __name__ == "__main__":
    run_chatbot()