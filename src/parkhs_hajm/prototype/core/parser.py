"""감사보고서 파싱 시스템

회사표(종속기업/관계기업) 전문 파서.
- 2단 헤더: 상위_하위 형태로 컬럼명 생성 (예: 당기_자산, 당기_부채)
- 국가명 자동 추출 (회사명 + 지역 필드)
- 재무상태표 파싱 제외
"""

from bs4 import BeautifulSoup
from chromadb import PersistentClient
from chromadb.api.types import EmbeddingFunction
from typing import List, Dict, Optional
import sqlite3, re, os, json


COUNTRY_MAP = {
    "Vietnam": "베트남", "Viet": "베트남", "THAINGUYEN": "베트남",
    "China": "중국", "Suzhou": "중국", "Tianjin": "중국", "Shanghai": "중국",
    "Shenzhen": "중국", "Beijing": "중국", "Dongguan": "중국", "Huizhou": "중국",
    "America": "미국", "USA": "미국",
    "India": "인도", "Europe": "유럽", "Japan": "일본", "Brazil": "브라질",
    "Mexico": "멕시코", "Hungary": "헝가리", "UK": "영국", "Poland": "폴란드",
    "Russia": "러시아", "Australia": "호주", "Canada": "캐나다",
    "Singapore": "싱가포르", "Thailand": "태국", "Indonesia": "인도네시아",
    "Malaysia": "말레이시아", "Philippines": "필리핀", "Czech": "체코",
    "Slovakia": "슬로바키아", "Ukraine": "우크라이나", "Turkey": "튀르키예",
    "Egypt": "이집트", "Kazakhstan": "카자흐스탄",
}


def _extract_countries(name: str) -> str:
    found = []
    for eng, kor in COUNTRY_MAP.items():
        if eng in name and kor not in found:
            found.append(kor)
    return ", ".join(found)


class AuditReportParser:

    def __init__(self, vector_db_path, sqlite_db_path, embedding_fn, reset_db=False):
        if reset_db and os.path.exists(vector_db_path):
            import shutil
            shutil.rmtree(vector_db_path)
            print("  ✓ 벡터DB 초기화")

        self._chroma = PersistentClient(path=vector_db_path)
        kw = {"embedding_function": embedding_fn}
        self.col_company = self._chroma.get_or_create_collection("audit_company", **kw)

        self.db = sqlite3.connect(sqlite_db_path)
        self._init_db(reset_db)

        print(f"✓ 벡터DB: {vector_db_path} | company={self.col_company.count()}")
        print(f"✓ SQLite:  {sqlite_db_path}")

    def _init_db(self, reset):
        cur = self.db.cursor()
        if reset:
            cur.execute("DROP TABLE IF EXISTS subsidiaries")
        cur.execute("""CREATE TABLE IF NOT EXISTS subsidiaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER, company_name TEXT, data_json TEXT)""")
        self.db.commit()

    def _read_html(self, path):
        for enc in ["euc-kr", "cp949", "latin1"]:
            try:
                with open(path, "r", encoding=enc) as f:
                    return f.read()
            except Exception:
                continue
        raise ValueError(f"읽기 실패: {path}")

    def _norm(self, text):
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _add(self, col, docs, metas, ids):
        try:
            col.add(documents=docs, metadatas=metas, ids=ids)
        except Exception:
            for d, m, i in zip(docs, metas, ids):
                try:
                    col.add(documents=[d], metadatas=[m], ids=[i])
                except Exception:
                    pass

    # ------------------------------------------------------------------ 병합 셀

    def _parse_merged(self, table) -> List[List[str]]:
        rows = table.find_all("tr")
        max_cols = max(
            (sum(int(c.get("colspan",1)) for c in r.find_all(["td","th"])) for r in rows),
            default=0
        )
        data: List[List[str]] = []
        for ri, row in enumerate(rows):
            if ri >= len(data): data.append([""] * max_cols)
            ci = 0
            for cell in row.find_all(["td","th"]):
                while ci < max_cols and data[ri][ci] != "": ci += 1
                if ci >= max_cols: break
                v  = cell.get_text(strip=True)
                cs = int(cell.get("colspan",1))
                rs = int(cell.get("rowspan",1))
                for r in range(rs):
                    if ri+r >= len(data): data.append([""] * max_cols)
                    for c in range(cs):
                        if ci+c < max_cols: data[ri+r][ci+c] = v
                ci += cs
        return data

    # ------------------------------------------------------------------ 2단 헤더

    def _build_headers(self, data: List[List[str]]) -> tuple:
        """2단 헤더를 상위_하위 형태로 합쳐서 컬럼명 생성.
        
        단일 헤더면 row0 그대로, 2단이면 row0_row1 조합.
        반환: (headers, data_start_row)
        """
        if not data:
            return [], 0

        row0 = data[0]
        if len(data) < 2:
            return row0, 1

        row1 = data[1]

        # row1이 대부분 숫자/비어있으면 단일 헤더
        non_text = [v for v in row1 if v and not re.fullmatch(r"[\d,.()\-△%\s]+", v)]
        if len(non_text) < len(row1) * 0.3:
            return row0, 1

        # row0과 row1이 동일하면 단일 헤더
        if row0 == row1:
            return row0, 1

        # 2단 헤더 조합
        headers = []
        for h0, h1 in zip(row0, row1):
            if not h0 and not h1:
                headers.append("")
            elif not h1 or h0 == h1:
                # rowspan으로 내려온 단일 헤더
                headers.append(h0)
            elif not h0:
                headers.append(h1)
            else:
                # 상위_하위 조합 (예: 당기_자산)
                headers.append(f"{h0}_{h1}")

        return headers, 2

    # ------------------------------------------------------------------ 문서 순회

    def _traverse(self, html_path, year) -> List[Dict]:
        soup = BeautifulSoup(self._read_html(html_path), "lxml")
        body = soup.body or soup

        section, bold = "", ""
        candidates = []
        ti = 0

        for tag in body.find_all(["p","table"], recursive=True):
            if tag.name == "p":
                if "SECTION-1" in (tag.get("class") or []):
                    continue
                text  = self._norm(tag.get_text(" "))
                style = (tag.get("style") or "").replace(" ","").lower()
                is_bold = "font-weight:bold" in style or "font-weight:700" in style
                is_sec  = any(k in text for k in ["재무상태표","종속기업","관계기업","핵심감사사항","감사의견"]) or \
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
                    "year": year, "table_index": ti,
                    "table": tag, "section_title": section, "bold_title": bold,
                })
                ti += 1

        print(f"  ✓ 표 후보 {len(candidates)}개")
        return candidates

    # ------------------------------------------------------------------ 회사표 파싱

    def _is_company_table(self, data, headers) -> bool:
        hdr_text = " ".join(headers)
        if any(k in hdr_text for k in ["기업명", "회사명", "법인명"]):
            return True
        flat = " ".join(" ".join(r) for r in data[:15]).replace(" ", "")
        return "종속기업" in flat or "관계기업" in flat

    def _get_relation_type(self, section, bold, flat) -> str:
        merged = f"{section} {bold} {flat}"
        if "종속기업" in merged: return "subsidiary"
        if "관계기업" in merged: return "affiliate"
        if "공동기업" in merged: return "joint_venture"
        return "unknown_company_table"

    def _parse_companies(self, candidates: List[Dict], year: int):
        print(f"\n{'='*60}\n{year}년 회사표 파싱\n{'='*60}")
        cur = self.db.cursor()
        total = 0

        for c in candidates:
            data = self._parse_merged(c["table"])
            if len(data) < 2:
                continue

            headers, data_start = self._build_headers(data)

            if not self._is_company_table(data, headers):
                continue

            ccol = next(
                (i for i, h in enumerate(headers) if any(k in h for k in ["기업명","회사명","법인명"])),
                None
            )
            if ccol is None:
                continue

            sec  = c["section_title"]
            tid  = f"company_{year}_{c['table_index']}"
            flat = " ".join(" ".join(r) for r in data[:15]).replace(" ","")
            rel  = self._get_relation_type(sec, c["bold_title"], flat)

            # 요약 문서
            self._add(self.col_company,
                docs=[
                    f"삼성전자 {year} 감사보고서\n"
                    f"표유형: company_table\n"
                    f"관계유형: {rel}\n"
                    f"섹션: {sec}\n"
                    f"헤더: {', '.join(h for h in headers if h)}\n"
                    f"table_id: {tid}"
                ],
                metas=[{"year": year, "doc_type": "summary", "relation_type": rel,
                        "table_id": tid, "section_title": sec}],
                ids=[f"{tid}_summary"],
            )

            for ri, row in enumerate(data[data_start:]):
                if len(row) <= ccol:
                    continue
                name = row[ccol].strip()
                if not name or len(name) < 2:
                    continue

                # 행 데이터 - 2단 헤더 컬럼명 포함
                row_dict = {"relation_type": rel}
                for i, v in enumerate(row):
                    if i >= len(headers) or not v.strip() or v.strip() == "-":
                        continue
                    key   = headers[i]
                    clean = v.replace(",","").replace("(", "-").replace(")","").replace("△","-").strip()
                    try:
                        row_dict[key] = int(clean)   if re.match(r"^-?\d{5,}$", clean) else \
                                        float(clean) if re.match(r"^-?\d+\.\d+$", clean) else v.strip()
                    except Exception:
                        row_dict[key] = v.strip()

                # SQLite 저장
                cur.execute(
                    "INSERT INTO subsidiaries (year, company_name, data_json) VALUES (?,?,?)",
                    (year, name, json.dumps(row_dict, ensure_ascii=False))
                )

                # 국가명 추출 (회사명 + 지역 필드)
                countries = _extract_countries(name)
                for k, v in row_dict.items():
                    if any(x in k for x in ["지역","소재지","주사업장"]):
                        for eng, kor in COUNTRY_MAP.items():
                            if eng in str(v) and kor not in countries:
                                countries = f"{countries}, {kor}" if countries else kor

                # 행 문서 - 국가명 + 2단 헤더 컬럼명 포함
                row_lines = "\n".join(
                    f"{k}: {v}" for k, v in row_dict.items() if k != "relation_type" and v
                )
                doc_text = (
                    f"삼성전자 {year} 감사보고서\n"
                    f"표유형: company_table\n"
                    f"관계유형: {rel}\n"
                    f"기업명: {name}\n"
                )
                if countries:
                    doc_text += f"국가: {countries}\n"
                doc_text += row_lines

                self._add(self.col_company,
                    docs=[doc_text],
                    metas=[{"year": year, "doc_type": "row", "relation_type": rel,
                            "table_id": tid, "company_name": name}],
                    ids=[f"{tid}_r{ri}"],
                )
                total += 1

            print(f"  ✓ {rel} | {sec[:50]}")

        self.db.commit()
        print(f"  → 총 {total}개")

    # ------------------------------------------------------------------ 전체 처리

    def process_year(self, html_path, year):
        print(f"\n{'='*60}\n{year}년 처리\n{'='*60}")
        try:
            candidates = self._traverse(html_path, year)
            self._parse_companies(candidates, year)
        except Exception as e:
            import traceback
            print(f"✗ {year}년 실패: {e}")
            traceback.print_exc()

    def process_all(self, base_path, years):
        for year in years:
            path = f"{base_path}/감사보고서_{year}.htm"
            if os.path.exists(path):
                self.process_year(path, year)
            else:
                print(f"✗ {year}년 파일 없음")

    def close(self):
        self.db.close()


# ============================================================
# 임베딩 함수
# ============================================================

class Qwen3EmbeddingFunction(EmbeddingFunction):

    def __init__(self, model_name="Qwen/Qwen3-Embedding-4B", device="mps"):
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

    parser = AuditReportParser(
        vector_db_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/chroma_sections",
        sqlite_db_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/audit_data.db",
        embedding_fn=ef,
        reset_db=True,
    )

    parser.process_all(
        base_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/삼성전자_감사보고서_2014_2024",
        years=list(range(2014, 2025)),
    )

    cur = parser.db.cursor()
    cur.execute("SELECT year, COUNT(*) FROM subsidiaries GROUP BY year ORDER BY year")
    print("\n[종속기업/관계기업]")
    for r in cur.fetchall():
        print(f"  {r[0]}년: {r[1]}개")

    print(f"\n[벡터DB] company={parser.col_company.count()}")
    parser.close()