import os
import pandas as pd

# ============================================
# 0. 경로 설정
# ============================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEXT_PATH = os.path.join(BASE_DIR, "parsed_text.csv")
TABLE_PATH = os.path.join(BASE_DIR, "parsed_table.csv")

OUTPUT_TEXT_CHUNK = os.path.join(BASE_DIR, "chunked_text.csv")
OUTPUT_TABLE_ROW_CHUNK = os.path.join(BASE_DIR, "chunked_table_rows.csv")
OUTPUT_TABLE_GROUP_CHUNK = os.path.join(BASE_DIR, "chunked_table_groups.csv")


# ============================================
# 1. 공통 유틸
# ============================================
IMPORTANT_SECTIONS = [
    "감사의견",
    "재무제표에 대한 경영진의 책임",
    "재무제표에 대한 회사 경영진의 책임",
    "감사인의 책임",
    "핵심감사사항",
    "주석"
]


def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).replace("\xa0", " ")
    text = " ".join(text.split())
    return text.strip()


def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None


def is_important_section(section_hint):
    section_hint = clean_text(section_hint)
    return any(k in section_hint for k in IMPORTANT_SECTIONS)


def split_long_text(text, chunk_size=850, overlap=150):
    text = clean_text(text)

    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    n = len(text)

    while start < n:
        end = start + chunk_size
        chunk = text[start:end].strip()

        if chunk:
            chunks.append(chunk)

        if end >= n:
            break

        start += (chunk_size - overlap)

    return chunks


# ============================================
# 2. TEXT CHUNKING
# ============================================
def process_text(min_len=40, max_chunk_size=850, overlap=150):
    df = pd.read_csv(TEXT_PATH)

    df["content"] = df["content"].apply(clean_text)
    df["section_hint"] = df["section_hint"].fillna("").apply(clean_text)
    df["year"] = df["year"].apply(safe_int)

    df = df[df["content"] != ""].copy()
    df = df[df["content"].str.len() >= min_len].copy()
    df = df.sort_values(["file_name", "section_hint", "paragraph_id"]).reset_index(drop=True)

    section_chunks = []
    chunk_seq = 0

    # 핵심: file_name + section_hint 단위로 먼저 묶기
    grouped = df.groupby(["file_name", "year", "section_hint"], dropna=False)

    for (file_name, year, section_hint), group in grouped:
        group = group.sort_values("paragraph_id").reset_index(drop=True)

        texts = group["content"].tolist()
        para_ids = group["paragraph_id"].tolist()

        merged_text = " ".join([t for t in texts if t]).strip()
        if not merged_text:
            continue

        # 중요한 섹션이면 검색 강화를 위해 prefix를 더 분명히 부여
        prefix_parts = []
        if year is not None:
            prefix_parts.append(f"[연도: {year}]")
        if section_hint:
            prefix_parts.append(f"[섹션: {section_hint}]")

        # 중요한 섹션은 섹션명을 한 번 더 강조
        if is_important_section(section_hint):
            prefix_parts.append(f"[중요섹션: {section_hint}]")

        prefix = " ".join(prefix_parts).strip()

        split_chunks = split_long_text(merged_text, chunk_size=max_chunk_size, overlap=overlap)

        for ch in split_chunks:
            final_content = f"{prefix} {ch}".strip() if prefix else ch

            section_chunks.append({
                "chunk_id": f"text_{chunk_seq}",
                "file_name": file_name,
                "year": year,
                "source_type": "text",
                "section_hint": section_hint if section_hint else None,
                "table_id": None,
                "row_id_start": None,
                "row_id_end": None,
                "paragraph_id_start": min(para_ids) if para_ids else None,
                "paragraph_id_end": max(para_ids) if para_ids else None,
                "content": final_content,
                "content_length": len(final_content)
            })
            chunk_seq += 1

    df_out = pd.DataFrame(section_chunks)

    if not df_out.empty:
        df_out = df_out.drop_duplicates(
            subset=["file_name", "year", "section_hint", "content"]
        ).reset_index(drop=True)

    return df_out


# ============================================
# 3. TABLE ROW CHUNKING
# ============================================
def process_table_rows():
    df = pd.read_csv(TABLE_PATH)

    df["content"] = df["content"].apply(clean_text)
    df["year"] = df["year"].apply(safe_int)

    final_rows = []
    chunk_seq = 0

    for _, row in df.iterrows():
        content = row["content"]
        if not content:
            continue

        file_name = clean_text(row["file_name"])
        year = safe_int(row["year"])
        table_id = safe_int(row["table_id"])
        row_id = safe_int(row["row_id"])
        num_cols = safe_int(row["num_cols"])

        prefix_parts = ["[표행]"]
        if year is not None:
            prefix_parts.append(f"[연도: {year}]")
        if table_id is not None:
            prefix_parts.append(f"[table_id: {table_id}]")
        if row_id is not None:
            prefix_parts.append(f"[row_id: {row_id}]")

        prefix = " ".join(prefix_parts)
        final_content = f"{prefix} {content}".strip()

        final_rows.append({
            "chunk_id": f"table_row_{chunk_seq}",
            "file_name": file_name,
            "year": year,
            "source_type": "table_row",
            "section_hint": None,
            "table_id": table_id,
            "row_id_start": row_id,
            "row_id_end": row_id,
            "paragraph_id_start": None,
            "paragraph_id_end": None,
            "content": final_content,
            "content_length": len(final_content),
            "num_cols": num_cols
        })
        chunk_seq += 1

    df_out = pd.DataFrame(final_rows)

    if not df_out.empty:
        df_out = df_out.drop_duplicates(
            subset=["file_name", "table_id", "row_id_start", "row_id_end", "content"]
        ).reset_index(drop=True)

    return df_out


# ============================================
# 4. TABLE GROUP CHUNKING
# ============================================
def process_table_groups(group_size=4, overlap=1):
    """
    lookup noise를 줄이기 위해 이전보다 조금 더 작은 묶음으로 구성
    """
    df = pd.read_csv(TABLE_PATH)

    df["content"] = df["content"].apply(clean_text)
    df["year"] = df["year"].apply(safe_int)

    grouped_chunks = []
    chunk_seq = 0

    grouped = df.groupby(["file_name", "year", "table_id"], dropna=False)

    for (file_name, year, table_id), group in grouped:
        group = group.sort_values("row_id").reset_index(drop=True)

        rows = []
        for _, row in group.iterrows():
            rows.append({
                "row_id": safe_int(row["row_id"]),
                "content": clean_text(row["content"])
            })

        if not rows:
            continue

        start = 0
        total = len(rows)

        while start < total:
            end = min(start + group_size, total)
            subset = rows[start:end]

            row_texts = [r["content"] for r in subset if r["content"]]
            if row_texts:
                row_id_start = subset[0]["row_id"]
                row_id_end = subset[-1]["row_id"]

                prefix_parts = ["[표묶음]"]
                if year is not None:
                    prefix_parts.append(f"[연도: {year}]")
                if table_id is not None:
                    prefix_parts.append(f"[table_id: {table_id}]")
                if row_id_start is not None and row_id_end is not None:
                    prefix_parts.append(f"[row_range: {row_id_start}-{row_id_end}]")

                prefix = " ".join(prefix_parts)
                final_content = prefix + "\n" + "\n".join(row_texts)

                grouped_chunks.append({
                    "chunk_id": f"table_group_{chunk_seq}",
                    "file_name": file_name,
                    "year": year,
                    "source_type": "table_group",
                    "section_hint": None,
                    "table_id": table_id,
                    "row_id_start": row_id_start,
                    "row_id_end": row_id_end,
                    "paragraph_id_start": None,
                    "paragraph_id_end": None,
                    "content": final_content,
                    "content_length": len(final_content)
                })
                chunk_seq += 1

            if end >= total:
                break

            start += (group_size - overlap)

    df_out = pd.DataFrame(grouped_chunks)

    if not df_out.empty:
        df_out = df_out.drop_duplicates(
            subset=["file_name", "table_id", "row_id_start", "row_id_end", "content"]
        ).reset_index(drop=True)

    return df_out


# ============================================
# 5. 실행
# ============================================
def run():
    print("🔹 TEXT CHUNKING 시작")
    text_chunks = process_text()

    print("🔹 TABLE ROW CHUNKING 시작")
    table_row_chunks = process_table_rows()

    print("🔹 TABLE GROUP CHUNKING 시작")
    table_group_chunks = process_table_groups()

    text_chunks.to_csv(OUTPUT_TEXT_CHUNK, index=False, encoding="utf-8-sig")
    table_row_chunks.to_csv(OUTPUT_TABLE_ROW_CHUNK, index=False, encoding="utf-8-sig")
    table_group_chunks.to_csv(OUTPUT_TABLE_GROUP_CHUNK, index=False, encoding="utf-8-sig")

    print("\n✅ Step2 완료")
    print(f"TEXT chunks: {len(text_chunks)}")
    print(f"TABLE ROW chunks: {len(table_row_chunks)}")
    print(f"TABLE GROUP chunks: {len(table_group_chunks)}")


if __name__ == "__main__":
    run()