import os
import pickle
import numpy as np
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer

# ============================================
# 0. 경로 설정
# ============================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEXT_CHUNK_PATH = os.path.join(BASE_DIR, "chunked_text.csv")
TABLE_ROW_CHUNK_PATH = os.path.join(BASE_DIR, "chunked_table_rows.csv")
TABLE_GROUP_CHUNK_PATH = os.path.join(BASE_DIR, "chunked_table_groups.csv")

INDEX_PATH = os.path.join(BASE_DIR, "vector.index")
META_PATH = os.path.join(BASE_DIR, "metadata.pkl")


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


# ============================================
# 2. CSV 로드
# ============================================
def load_single_csv(path, expected_source_type=None):
    if not os.path.exists(path):
        print(f"⚠️ 파일 없음: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)

    if df.empty:
        print(f"⚠️ 빈 파일: {path}")
        return df

    # content 정리
    if "content" in df.columns:
        df["content"] = df["content"].apply(clean_text)
        df = df[df["content"] != ""].copy()

    # 메타데이터 컬럼 보정
    required_cols = [
        "chunk_id",
        "file_name",
        "year",
        "source_type",
        "section_hint",
        "table_id",
        "row_id_start",
        "row_id_end",
        "paragraph_id_start",
        "paragraph_id_end",
        "content",
        "content_length"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # source_type 강제 지정
    if expected_source_type is not None:
        df["source_type"] = expected_source_type

    # 형 변환
    int_cols = ["year", "table_id", "row_id_start", "row_id_end",
                "paragraph_id_start", "paragraph_id_end", "content_length"]

    for col in int_cols:
        df[col] = df[col].apply(safe_int)

    df["section_hint"] = df["section_hint"].fillna("").apply(clean_text)
    df["file_name"] = df["file_name"].fillna("").apply(clean_text)
    df["chunk_id"] = df["chunk_id"].fillna("").apply(clean_text)
    df["source_type"] = df["source_type"].fillna("").apply(clean_text)

    return df[required_cols].copy()


# ============================================
# 3. 전체 chunk 데이터 로드
# ============================================
def load_all_chunks():
    print("🔹 chunked_text.csv 로드 중...")
    df_text = load_single_csv(TEXT_CHUNK_PATH, expected_source_type="text")

    print("🔹 chunked_table_rows.csv 로드 중...")
    df_table_rows = load_single_csv(TABLE_ROW_CHUNK_PATH, expected_source_type="table_row")

    print("🔹 chunked_table_groups.csv 로드 중...")
    df_table_groups = load_single_csv(TABLE_GROUP_CHUNK_PATH, expected_source_type="table_group")

    df_all = pd.concat([df_text, df_table_rows, df_table_groups], ignore_index=True)

    if df_all.empty:
        raise ValueError("❌ 벡터 DB 생성 대상 데이터가 없습니다. Step2 결과 파일을 확인하세요.")

    # 중복 제거
    df_all = df_all.drop_duplicates(subset=["file_name", "source_type", "content"]).reset_index(drop=True)

    print(f"✅ 전체 chunk 수: {len(df_all)}")
    print("source_type 분포:")
    print(df_all["source_type"].value_counts(dropna=False))

    return df_all


# ============================================
# 4. 임베딩 생성
# ============================================
def create_embeddings(texts, model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"):
    print(f"🔹 임베딩 모델 로딩 중: {model_name}")
    model = SentenceTransformer(model_name)

    print("🔹 임베딩 생성 중...")
    embeddings = model.encode(
        texts,
        show_progress_bar=True,
        convert_to_numpy=True
    )

    embeddings = np.array(embeddings).astype("float32")
    return embeddings


# ============================================
# 5. FAISS 인덱스 생성
# ============================================
def build_faiss_index(embeddings):
    if len(embeddings.shape) != 2:
        raise ValueError("❌ embeddings shape가 올바르지 않습니다.")

    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)

    return index


# ============================================
# 6. metadata 생성
# ============================================
def build_metadata(df_all):
    metadata = []

    for _, row in df_all.iterrows():
        metadata.append({
            "chunk_id": row["chunk_id"],
            "file_name": row["file_name"],
            "year": safe_int(row["year"]),
            "source_type": row["source_type"],
            "section_hint": row["section_hint"] if row["section_hint"] else None,
            "table_id": safe_int(row["table_id"]),
            "row_id_start": safe_int(row["row_id_start"]),
            "row_id_end": safe_int(row["row_id_end"]),
            "paragraph_id_start": safe_int(row["paragraph_id_start"]),
            "paragraph_id_end": safe_int(row["paragraph_id_end"]),
            "content": row["content"],
            "content_length": safe_int(row["content_length"])
        })

    return metadata


# ============================================
# 7. 저장
# ============================================
def save_outputs(index, metadata):
    faiss.write_index(index, INDEX_PATH)

    with open(META_PATH, "wb") as f:
        pickle.dump(metadata, f)

    print(f"✅ FAISS 인덱스 저장 완료: {INDEX_PATH}")
    print(f"✅ metadata 저장 완료: {META_PATH}")


# ============================================
# 8. 실행
# ============================================
def run():
    df_all = load_all_chunks()

    texts = df_all["content"].tolist()
    metadata = build_metadata(df_all)

    print(f"🔹 임베딩 대상 텍스트 수: {len(texts)}")

    embeddings = create_embeddings(texts)
    index = build_faiss_index(embeddings)

    save_outputs(index, metadata)

    print("\n✅ Step3 통합 Vector DB 구축 완료")


if __name__ == "__main__":
    run()