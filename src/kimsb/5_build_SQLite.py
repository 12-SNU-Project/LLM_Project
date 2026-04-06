import os
import re
import sqlite3
import pandas as pd

# ============================================
# 0. 경로 설정
# ============================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TABLE_PATH = os.path.join(BASE_DIR, "parsed_table.csv")
DB_PATH = os.path.join(BASE_DIR, "finance.db")


# ============================================
# 1. 공통 유틸
# ============================================
def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).replace("\xa0", " ")
    text = " ".join(text.split())
    return text.strip()


def safe_int(x):
    try:
        if pd.isna(x):
            return None
        return int(x)
    except:
        return None


def split_row_content(content):
    """
    Step1에서 content를 ' | ' 기준으로 저장했으므로 다시 분리
    """
    content = clean_text(content)
    if not content:
        return []
    return [clean_text(x) for x in content.split("|") if clean_text(x)]


def normalize_value(text):
    """
    숫자 문자열 정규화
    예:
    '1,234' -> 1234
    '(1,234)' -> -1234
    '1,234.56' -> 1234.56
    """
    text = clean_text(text)
    if not text:
        return None

    if not re.search(r"\d", text):
        return None

    negative = False

    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()

    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)

    if text in {"", "-", ".", "-."}:
        return None

    try:
        if "." in text:
            value = float(text)
        else:
            value = int(text)

        if negative:
            value = -value

        return value
    except:
        return None


def extract_year_from_token(token):
    token = clean_text(token)
    if not token:
        return None

    m = re.search(r"(20\d{2}|19\d{2})", token)
    if m:
        return int(m.group(1))
    return None


def is_date_like_token(token):
    token = clean_text(token)
    if not token:
        return False

    patterns = [
        r"\d{4}-\d{2}-\d{2}",
        r"\d{4}\.\d{1,2}\.\d{1,2}",
        r"\d{4}/\d{1,2}/\d{1,2}",
        r"\d{4}년\s*\d{1,2}월\s*\d{1,2}일"
    ]
    return any(re.search(p, token) for p in patterns)


def is_mostly_english(token):
    token = clean_text(token)
    if not token:
        return False

    english_chars = re.findall(r"[A-Za-z]", token)
    if not english_chars:
        return False

    return len(english_chars) >= max(4, len(token) * 0.4)


# ============================================
# 2. 재무제표/계정명 관련 규칙
# ============================================
VALID_ACCOUNT_KEYWORDS = [
    "자산", "부채", "자본", "매출", "수익", "이익", "손실",
    "현금", "채권", "재고", "비용", "법인세", "차입금",
    "영업권", "매입채무", "매출채권", "유동", "비유동",
    "감가상각", "충당부채", "자본금", "이익잉여금", "총계"
]

INVALID_ACCOUNT_HINTS = [
    "Samsung", "Electronics", "Citibank", "BNP", "SETK", "SAMCOL",
    "참조", "주석", "합계", "단위", "백만원", "원", "날짜"
]


def detect_statement_type(text):
    """
    재무제표 종류 추정
    """
    text = clean_text(text)

    if "연결재무상태표" in text or "재무상태표" in text:
        return "재무상태표"

    if "연결포괄손익계산서" in text or "포괄손익계산서" in text:
        return "손익계산서"

    if "연결손익계산서" in text or "손익계산서" in text:
        return "손익계산서"

    if "연결현금흐름표" in text or "현금흐름표" in text:
        return "현금흐름표"

    if "연결자본변동표" in text or "자본변동표" in text:
        return "자본변동표"

    # 계정 힌트 기반 보조 추정
    balance_sheet_keywords = ["유동자산", "비유동자산", "유동부채", "비유동부채", "자본총계", "자산총계", "부채총계"]
    income_keywords = ["매출", "매출액", "영업이익", "당기순이익", "매출총이익", "법인세비용차감전순이익"]
    cashflow_keywords = ["영업활동", "투자활동", "재무활동", "현금및현금성자산"]

    if any(k in text for k in balance_sheet_keywords):
        return "재무상태표"
    if any(k in text for k in income_keywords):
        return "손익계산서"
    if any(k in text for k in cashflow_keywords):
        return "현금흐름표"

    return None


def is_account_name(token):
    """
    계정명으로 인정할 수 있는지 더 엄격하게 판단
    """
    token = clean_text(token)
    if not token:
        return False

    # 숫자만 있으면 제외
    if re.fullmatch(r"[\d,().\- ]+", token):
        return False

    # 너무 짧으면 제외
    if len(token) <= 1:
        return False

    # 너무 길면 설명문일 가능성 높음
    if len(token) > 30:
        return False

    # 영어 위주면 법인명/은행명 가능성 높음
    if is_mostly_english(token):
        return False

    # 날짜형 제외
    if is_date_like_token(token):
        return False

    # 무효 힌트 포함 시 제외
    if any(hint in token for hint in INVALID_ACCOUNT_HINTS):
        return False

    # 회계 계정 키워드가 하나라도 있어야 인정
    if any(k in token for k in VALID_ACCOUNT_KEYWORDS):
        return True

    return False


def is_valid_value_cell(token):
    """
    값 셀로 쓸 수 있는지 판단
    """
    token = clean_text(token)
    if not token:
        return False

    value = normalize_value(token)
    return value is not None


# ============================================
# 3. 연도 header 탐지
# ============================================
def detect_year_header(rows):
    """
    표 앞부분에서 연도 header 후보 탐지
    반환:
    {
        "header_row_id": ...,
        "years": [...]
    }
    또는 None
    """
    for row in rows[:8]:
        cells = row["cells"]
        years = []

        for cell in cells:
            y = extract_year_from_token(cell)
            if y is not None:
                years.append(y)

        # 최소 2개 이상 연도 필요
        if len(years) >= 2:
            return {
                "header_row_id": row["row_id"],
                "years": years
            }

    return None


# ============================================
# 4. 원본 row 로드
# ============================================
def load_table_rows():
    if not os.path.exists(TABLE_PATH):
        raise FileNotFoundError(f"❌ 파일이 없습니다: {TABLE_PATH}")

    df = pd.read_csv(TABLE_PATH)

    required_cols = ["file_name", "year", "table_id", "row_id", "content", "num_cols"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"❌ parsed_table.csv에 '{col}' 컬럼이 없습니다.")

    df["file_name"] = df["file_name"].apply(clean_text)
    df["year"] = df["year"].apply(safe_int)
    df["table_id"] = df["table_id"].apply(safe_int)
    df["row_id"] = df["row_id"].apply(safe_int)
    df["content"] = df["content"].apply(clean_text)
    df["num_cols"] = df["num_cols"].apply(safe_int)

    df = df[df["content"] != ""].copy()
    df = df.sort_values(["file_name", "table_id", "row_id"]).reset_index(drop=True)

    return df


# ============================================
# 5. financial_facts 생성
# ============================================
def build_financial_facts(df):
    facts = []

    grouped = df.groupby(["file_name", "year", "table_id"], dropna=False)

    for (file_name, doc_year, table_id), group in grouped:
        group = group.sort_values("row_id").reset_index(drop=True)

        rows = []
        for _, r in group.iterrows():
            cells = split_row_content(r["content"])
            if not cells:
                continue

            rows.append({
                "row_id": safe_int(r["row_id"]),
                "content": r["content"],
                "cells": cells
            })

        if not rows:
            continue

        # 표 상단 요약으로 재무제표 유형 추정
        combined_preview = " ".join([r["content"] for r in rows[:6]])
        statement_type = detect_statement_type(combined_preview)

        # 연도 header 탐지
        year_header = detect_year_header(rows)
        header_years = year_header["years"] if year_header else None
        header_row_id = year_header["header_row_id"] if year_header else None

        for row in rows:
            row_id = row["row_id"]
            cells = row["cells"]
            raw_row = row["content"]

            # header row 자체는 skip
            if header_row_id is not None and row_id == header_row_id:
                continue

            if len(cells) < 2:
                continue

            account_name = clean_text(cells[0])
            value_cells = cells[1:]

            # 계정명 필터 강화
            if not is_account_name(account_name):
                continue

            # row 단위 statement_type 보정
            row_statement_type = detect_statement_type(raw_row)
            final_statement_type = row_statement_type or statement_type

            # statement_type이 전혀 없는 경우는 너무 위험하므로 보수적으로 skip
            if final_statement_type is None:
                continue

            # 값 후보 개수 확인
            valid_value_count = sum(1 for v in value_cells if is_valid_value_cell(v))

            # 값이 하나도 없으면 skip
            if valid_value_count == 0:
                continue

            # ----------------------------------------
            # Case A: 연도 header와 value 셀을 매칭
            # ----------------------------------------
            if header_years and len(value_cells) >= 1:
                usable_count = min(len(header_years), len(value_cells))
                appended = 0

                for i in range(usable_count):
                    target_year = header_years[i]
                    value = normalize_value(value_cells[i])

                    if value is None:
                        continue

                    facts.append({
                        "file_name": file_name,
                        "doc_year": doc_year,
                        "table_id": table_id,
                        "row_id": row_id,
                        "statement_type": final_statement_type,
                        "account_name": account_name,
                        "target_year": target_year,
                        "value": value,
                        "unit": None,
                        "raw_row": raw_row
                    })
                    appended += 1

                if appended > 0:
                    continue

            # ----------------------------------------
            # Case B: row 안에 직접 연도 + 값이 있는 경우
            # 예: 유동자산 | 2014 | 123,456
            # ----------------------------------------
            if len(cells) >= 3:
                appended = 0
                for i in range(1, len(cells) - 1):
                    y = extract_year_from_token(cells[i])
                    v = normalize_value(cells[i + 1])

                    if y is not None and v is not None:
                        facts.append({
                            "file_name": file_name,
                            "doc_year": doc_year,
                            "table_id": table_id,
                            "row_id": row_id,
                            "statement_type": final_statement_type,
                            "account_name": account_name,
                            "target_year": y,
                            "value": v,
                            "unit": None,
                            "raw_row": raw_row
                        })
                        appended += 1

                if appended > 0:
                    continue

            # ----------------------------------------
            # Case C: 단일 값 row
            # 예: 유동자산 | 123,456
            # -> 문서연도를 target_year로 사용
            # 단, 지나치게 애매한 row는 제외
            # ----------------------------------------
            if len(value_cells) == 1:
                v = normalize_value(value_cells[0])
                if v is not None:
                    facts.append({
                        "file_name": file_name,
                        "doc_year": doc_year,
                        "table_id": table_id,
                        "row_id": row_id,
                        "statement_type": final_statement_type,
                        "account_name": account_name,
                        "target_year": doc_year,
                        "value": v,
                        "unit": None,
                        "raw_row": raw_row
                    })

    facts_df = pd.DataFrame(facts)

    if facts_df.empty:
        return facts_df

    facts_df = facts_df.drop_duplicates(
        subset=[
            "file_name",
            "table_id",
            "row_id",
            "statement_type",
            "account_name",
            "target_year",
            "value"
        ]
    ).reset_index(drop=True)

    return facts_df


# ============================================
# 6. SQLite 저장
# ============================================
def save_to_sqlite(df_rows, df_facts):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS table_rows")
    cursor.execute("DROP TABLE IF EXISTS financial_facts")

    cursor.execute("""
    CREATE TABLE table_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        year INTEGER,
        table_id INTEGER,
        row_id INTEGER,
        content TEXT,
        num_cols INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE financial_facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        doc_year INTEGER,
        table_id INTEGER,
        row_id INTEGER,
        statement_type TEXT,
        account_name TEXT,
        target_year INTEGER,
        value REAL,
        unit TEXT,
        raw_row TEXT
    )
    """)

    if not df_rows.empty:
        df_rows[["file_name", "year", "table_id", "row_id", "content", "num_cols"]].to_sql(
            "table_rows",
            conn,
            if_exists="append",
            index=False
        )

    if not df_facts.empty:
        df_facts[[
            "file_name",
            "doc_year",
            "table_id",
            "row_id",
            "statement_type",
            "account_name",
            "target_year",
            "value",
            "unit",
            "raw_row"
        ]].to_sql(
            "financial_facts",
            conn,
            if_exists="append",
            index=False
        )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_rows_file_year ON table_rows(file_name, year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_rows_table ON table_rows(table_id, row_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_account ON financial_facts(account_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_target_year ON financial_facts(target_year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_statement ON financial_facts(statement_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_file ON financial_facts(file_name, doc_year)")

    conn.commit()
    conn.close()


# ============================================
# 7. 실행
# ============================================
def run():
    print("🔹 parsed_table.csv 로드 중...")
    df_rows = load_table_rows()
    print(f"✅ 원본 table row 수: {len(df_rows)}")

    print("🔹 financial_facts 생성 중...")
    df_facts = build_financial_facts(df_rows)
    print(f"✅ 추출된 financial fact 수: {len(df_facts)}")

    print("🔹 SQLite 저장 중...")
    save_to_sqlite(df_rows, df_facts)

    print(f"\n✅ Step5 완료: {DB_PATH}")
    print("생성 테이블:")
    print("- table_rows")
    print("- financial_facts")


if __name__ == "__main__":
    run()