import os
import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# ============================================
# 경로 설정
# ============================================
DATA_DIR = "data"
OUTPUT_TEXT = "parsed_text.csv"
OUTPUT_TABLE = "parsed_table.csv"


# ============================================
# HTML 읽기
# ============================================
def read_html(file_path):
    try:
        with open(file_path, "r", encoding="euc-kr") as f:
            return f.read()
    except Exception:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()


# ============================================
# 파일명에서 연도 추출
# ============================================
def extract_year_from_filename(file_name):
    match = re.search(r"(20\d{2}|19\d{2})", file_name)
    if match:
        return int(match.group(1))
    return None


# ============================================
# 텍스트 정리
# ============================================
def clean_text(text):
    if text is None:
        return ""

    text = str(text).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ============================================
# 불필요한 짧은 잡문장 필터
# ============================================
def is_noise_line(line):
    line = clean_text(line)

    if not line:
        return True

    # 기호/숫자 위주 줄
    if re.fullmatch(r"[\d\W_]+", line):
        return True

    bad_exact = {
        "참조", "참조)", "(참조)", "주)", "(주)", "단위", "원", "백만원", "천원"
    }
    if line in bad_exact:
        return True

    # 너무 짧고 의미 없는 경우
    if len(line) <= 1:
        return True

    return False


# ============================================
# 제목(섹션) 판별
# ============================================
def is_section_title(line):
    """
    제목 후보를 훨씬 엄격하게 판별
    """
    line = clean_text(line)

    if not line:
        return False

    # 너무 긴 줄은 제목 가능성 낮음
    if len(line) > 40:
        return False

    # 노이즈 제거
    if is_noise_line(line):
        return False

    # 숫자/기호 위주 제거
    if re.fullmatch(r"[\W\d\(\)\[\]·,.\-]+", line):
        return False

    # 의미 없는 짧은 제목 제거
    bad_titles = {
        "참조", "참조)", "(참조)", "주)", "(주)", "단위", "원", "백만원", "천원",
        "해외", "국내", "연결", "별도"
    }
    if line in bad_titles:
        return False

    # 제목에 자주 등장하는 명확한 핵심 키워드
    strong_title_keywords = [
        "감사의견",
        "핵심감사사항",
        "재무제표에 대한 경영진의 책임",
        "재무제표에 대한 회사 경영진의 책임",
        "감사인의 책임",
        "재무상태표",
        "연결재무상태표",
        "손익계산서",
        "연결손익계산서",
        "포괄손익계산서",
        "연결포괄손익계산서",
        "현금흐름표",
        "연결현금흐름표",
        "자본변동표",
        "연결자본변동표",
        "주석"
    ]
    if any(k in line for k in strong_title_keywords):
        return True

    # 제목 말투 패턴
    title_like_patterns = [
        r"^\d+\.\s*.+",      # 1. 제목
        r"^[가-힣A-Za-z]+\.\s*.+",  # 가. 제목
        r"^제\s*\d+\s*기",   # 제 50 기
    ]
    if any(re.match(p, line) for p in title_like_patterns):
        # 하지만 너무 긴 건 제외
        return len(line) <= 35

    # 문장형/설명형은 제목으로 보지 않음
    if line.endswith(("니다", "습니다", ".", "다.")):
        return False

    # 너무 일반적인 계정명/표 항목은 제목으로 보지 않음
    non_section_keywords = [
        "사용제한금융상품", "단기매도가능금융자산", "기타유동자산",
        "기타비유동자산", "매출채권", "재고자산", "영업권"
    ]
    if any(k == line for k in non_section_keywords):
        return False

    return False


# ============================================
# Table 추출
# ============================================
def extract_tables(soup, file_name, year):
    tables_data = []
    tables = soup.find_all("table")

    for t_idx, table in enumerate(tables):
        rows = table.find_all("tr")

        if len(rows) < 2:
            continue

        for r_idx, row in enumerate(rows):
            cols = row.find_all(["td", "th"])
            row_cells = [clean_text(col.get_text(" ", strip=True)) for col in cols]

            if not any(cell.strip() for cell in row_cells):
                continue

            tables_data.append({
                "file_name": file_name,
                "year": year,
                "type": "table",
                "table_id": t_idx,
                "row_id": r_idx,
                "content": " | ".join(row_cells),
                "num_cols": len(row_cells)
            })

    return tables_data


# ============================================
# 문단 단위 text 추출
# ============================================
def extract_paragraphs(soup, file_name, year):
    paragraphs = []

    # table 제거
    for table in soup.find_all("table"):
        table.extract()

    full_text = soup.get_text("\n")
    raw_lines = full_text.split("\n")

    cleaned_lines = []
    for line in raw_lines:
        line = clean_text(line)
        if not line:
            continue
        if is_noise_line(line):
            continue
        cleaned_lines.append(line)

    current_section = None
    buffer = []
    paragraph_id = 0

    for line in cleaned_lines:
        # 섹션 제목이면 기존 buffer 저장 후 section 갱신
        if is_section_title(line):
            if buffer:
                paragraph_text = " ".join(buffer).strip()

                # 너무 짧은 문단은 제외
                if len(paragraph_text) >= 30:
                    paragraphs.append({
                        "file_name": file_name,
                        "year": year,
                        "type": "text",
                        "section_hint": current_section,
                        "paragraph_id": paragraph_id,
                        "content": paragraph_text,
                        "length": len(paragraph_text)
                    })
                    paragraph_id += 1

                buffer = []

            current_section = line
            continue

        # 일반 본문 누적
        buffer.append(line)

        # 너무 길어지면 문단 하나로 저장
        joined = " ".join(buffer).strip()
        if len(joined) >= 900:
            if len(joined) >= 30:
                paragraphs.append({
                    "file_name": file_name,
                    "year": year,
                    "type": "text",
                    "section_hint": current_section,
                    "paragraph_id": paragraph_id,
                    "content": joined,
                    "length": len(joined)
                })
                paragraph_id += 1
            buffer = []

    # 마지막 buffer 저장
    if buffer:
        paragraph_text = " ".join(buffer).strip()
        if len(paragraph_text) >= 30:
            paragraphs.append({
                "file_name": file_name,
                "year": year,
                "type": "text",
                "section_hint": current_section,
                "paragraph_id": paragraph_id,
                "content": paragraph_text,
                "length": len(paragraph_text)
            })

    return paragraphs


# ============================================
# 중복 제거
# ============================================
def remove_duplicates_text(data):
    seen = set()
    unique = []

    for item in data:
        key = (
            item.get("file_name"),
            item.get("type"),
            item.get("section_hint"),
            item.get("content")
        )
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique


def remove_duplicates_table(data):
    seen = set()
    unique = []

    for item in data:
        key = (
            item.get("file_name"),
            item.get("type"),
            item.get("table_id"),
            item.get("row_id"),
            item.get("content")
        )
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique


# ============================================
# 전체 파싱
# ============================================
def parse_all_files(data_dir):
    all_texts = []
    all_tables = []

    files = [f for f in os.listdir(data_dir) if f.endswith(".htm")]
    files.sort()

    print(f"총 파일 수: {len(files)}")

    for file in tqdm(files):
        file_path = os.path.join(data_dir, file)
        year = extract_year_from_filename(file)

        html = read_html(file_path)
        soup = BeautifulSoup(html, "lxml")

        tables = extract_tables(soup, file, year)
        texts = extract_paragraphs(soup, file, year)

        all_tables.extend(tables)
        all_texts.extend(texts)

    all_texts = remove_duplicates_text(all_texts)
    all_tables = remove_duplicates_table(all_tables)

    return all_texts, all_tables


# ============================================
# CSV 저장
# ============================================
def save_csv(texts, tables):
    df_text = pd.DataFrame(texts)
    df_table = pd.DataFrame(tables)

    if not df_text.empty:
        df_text["text_length_check"] = df_text["content"].apply(len)

    if not df_table.empty:
        df_table["row_length_check"] = df_table["content"].apply(len)

    df_text.to_csv(OUTPUT_TEXT, index=False, encoding="utf-8-sig")
    df_table.to_csv(OUTPUT_TABLE, index=False, encoding="utf-8-sig")

    print(f"\n✅ 텍스트 CSV 저장 완료: {OUTPUT_TEXT}")
    print(f"✅ 테이블 CSV 저장 완료: {OUTPUT_TABLE}")


# ============================================
# 실행
# ============================================
if __name__ == "__main__":
    texts, tables = parse_all_files(DATA_DIR)

    print(f"\n📄 문단 텍스트 개수: {len(texts)}")
    print(f"📊 테이블 행 개수: {len(tables)}")

    save_csv(texts, tables)