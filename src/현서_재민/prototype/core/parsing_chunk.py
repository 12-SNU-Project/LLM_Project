"""감사보고서 파싱 시스템

- 표 → SQLite (기존 작동하던 로직 사용)
- 텍스트 → 청크 → 벡터DB
"""

from bs4 import BeautifulSoup
import chromadb
from typing import List, Dict
import pandas as pd
import sqlite3
import re
import os
import json


class AuditReportParser:
    
    def __init__(self, 
                 sqlite_db_path: str,
                 vector_db_path: str,
                 chunk_size: int = 1000,
                 reset_db: bool = False):
        
        self.chunk_size = chunk_size
        
        # SQLite 초기화
        self.sqlite_conn = sqlite3.connect(sqlite_db_path)
        if reset_db:
            cursor = self.sqlite_conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS subsidiaries")
            cursor.execute("DROP TABLE IF EXISTS balance_sheet")
            self.sqlite_conn.commit()
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subsidiaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                company_name TEXT,
                data_json TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                account_name TEXT,
                data_json TEXT
            )
        """)
        self.sqlite_conn.commit()
        
        # 벡터DB 초기화
        if reset_db and os.path.exists(vector_db_path):
            import shutil
            shutil.rmtree(vector_db_path)
        
        self.chroma_client = chromadb.PersistentClient(path=vector_db_path)
        self.text_collection = self.chroma_client.get_or_create_collection(
            name="audit_text_chunks"
        )
        
        print(f"✓ SQLite: {sqlite_db_path}")
        print(f"✓ 벡터DB: {vector_db_path}, 청크: {chunk_size}자")
    
    # ========== 표 파싱 (기존 로직) ==========
    
    def parse_company_tables(self, html_path: str, year: int):
        """기업명 표 파싱"""
        
        # HTML 읽기
        for encoding in ['euc-kr', 'cp949', 'latin1']:
            try:
                with open(html_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except:
                continue
        
        soup = BeautifulSoup(html_content, 'lxml')
        all_tables = soup.find_all('table')
        
        # "기업명" 헤더 있는 표 찾기
        total_rows = 0
        
        for table in all_tables:
            first_row = table.find('tr')
            if not first_row:
                continue
            
            header_text = first_row.get_text()
            
            if '기업명' in header_text:
                df = self._parse_company_table(table, year)
                
                if not df.empty:
                    for _, row in df.iterrows():
                        self._save_subsidiary(row)
                        total_rows += 1
        
        self.sqlite_conn.commit()
        return total_rows
    
    def _parse_company_table(self, table, year: int) -> pd.DataFrame:
        """단일 기업명 표 파싱"""
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return pd.DataFrame()
        
        # 헤더
        headers = [c.get_text(strip=True) for c in rows[0].find_all(['th', 'td'])]
        
        # 데이터
        all_data = []
        
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) < 2:
                continue
            
            cell_texts = [c.get_text(strip=True) for c in cells]
            
            # 회사명 찾기
            company_name = None
            
            for text in cell_texts:
                if 'Samsung' in text or 'SAMSUNG' in text:
                    company_name = text
                    break
                if re.match(r'^[A-Z][a-zA-Z\s&\-().]+', text) and len(text) > 3:
                    if text not in ['Korea', 'China', 'USA', 'Japan', 'Vietnam', 'India', 'Europe']:
                        company_name = text
                        break
                if re.match(r'^[가-힣()㈜]+', text) and len(text) > 2:
                    company_name = text
                    break
            
            if not company_name:
                continue
            
            # 데이터 JSON
            row_dict = {}
            for i, text in enumerate(cell_texts):
                if i < len(headers) and headers[i]:
                    clean_text = text.replace(',', '').replace('(', '-').replace(')', '')
                    
                    if re.match(r'^-?\d{5,}$', clean_text):
                        try:
                            row_dict[headers[i]] = int(clean_text)
                            continue
                        except:
                            pass
                    
                    if re.match(r'^-?\d+\.\d+$', clean_text):
                        try:
                            row_dict[headers[i]] = float(clean_text)
                            continue
                        except:
                            pass
                    
                    if text and text != '-':
                        row_dict[headers[i]] = text
            
            all_data.append({
                'year': year,
                'company_name': company_name,
                'data_json': json.dumps(row_dict, ensure_ascii=False)
            })
        
        return pd.DataFrame(all_data)
    
    def parse_balance_sheet(self, html_path: str, year: int):
        """재무상태표 파싱"""
        
        # HTML 읽기
        for encoding in ['euc-kr', 'cp949', 'latin1']:
            try:
                with open(html_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except:
                continue
        
        soup = BeautifulSoup(html_content, 'lxml')
        all_tables = soup.find_all('table')
        
        if len(all_tables) < 5:
            return 0
        
        # 첫 15개 중 가장 큰 표
        front_tables = all_tables[:15]
        largest_table = max(front_tables, key=lambda t: len(t.find_all('tr')))
        
        # 2차원 배열로 변환
        table_data = self._parse_merged_cells(largest_table)
        
        if len(table_data) < 2:
            return 0
        
        # 헤더에서 연도 찾기
        header_row = table_data[0]
        year_columns = []
        
        for i, header in enumerate(header_row[1:], start=1):
            year_match = re.search(r'(\d{4})', header)
            if year_match:
                col_year = int(year_match.group(1))
                year_columns.append((i, col_year))
        
        # 당기 컬럼
        if year_columns:
            year_columns.sort(key=lambda x: x[1], reverse=True)
            current_col_idx = year_columns[0][0]
            current_year = year_columns[0][1]
        else:
            current_col_idx = 1
            current_year = year
        
        # 데이터 저장
        count = 0
        cursor = self.sqlite_conn.cursor()
        
        for row_data in table_data[1:]:
            if len(row_data) < 2:
                continue
            
            account_name_raw = row_data[0].strip()
            account_name = account_name_raw.replace(' ', '')
            
            if not account_name or len(account_name) < 2:
                continue
            
            if current_col_idx < len(row_data):
                amount_text = row_data[current_col_idx].replace(',', '').replace(' ', '').strip()
                
                is_summary_row = any(prefix in account_name for prefix in ['Ⅰ.', 'Ⅱ.', 'Ⅲ.', 'Ⅳ.', '총계', '합계'])
                
                if re.match(r'^-?\d+$', amount_text):
                    pass
                elif is_summary_row:
                    found_amount = False
                    for col_idx in range(1, len(row_data)):
                        test_amount = row_data[col_idx].replace(',', '').replace(' ', '').strip()
                        if re.match(r'^-?\d+$', test_amount):
                            amount_text = test_amount
                            found_amount = True
                            break
                    if not found_amount:
                        continue
                else:
                    continue
                
                data_dict = {
                    'account_name': account_name,
                    'account_name_display': account_name_raw,
                    'amount': amount_text,
                    'fiscal_year': current_year
                }
                
                cursor.execute("""
                    INSERT INTO balance_sheet (year, account_name, data_json)
                    VALUES (?, ?, ?)
                """, (current_year, account_name, json.dumps(data_dict, ensure_ascii=False)))
                count += 1
        
        self.sqlite_conn.commit()
        return count
    
    def _parse_merged_cells(self, table) -> List[List[str]]:
        """병합 셀 처리"""
        rows = table.find_all('tr')
        
        max_cols = 0
        for row in rows:
            cells = row.find_all(['td', 'th'])
            col_count = sum(int(cell.get('colspan', 1)) for cell in cells)
            max_cols = max(max_cols, col_count)
        
        table_data = []
        row_idx = 0
        
        for row in rows:
            if row_idx >= len(table_data):
                table_data.append([''] * max_cols)
            
            cells = row.find_all(['td', 'th'])
            col_idx = 0
            
            for cell in cells:
                while col_idx < max_cols and table_data[row_idx][col_idx] != '':
                    col_idx += 1
                
                if col_idx >= max_cols:
                    break
                
                cell_value = cell.get_text(strip=True)
                colspan = int(cell.get('colspan', 1))
                rowspan = int(cell.get('rowspan', 1))
                
                for r in range(rowspan):
                    if row_idx + r >= len(table_data):
                        table_data.append([''] * max_cols)
                    
                    for c in range(colspan):
                        if col_idx + c < max_cols:
                            table_data[row_idx + r][col_idx + c] = cell_value
                
                col_idx += colspan
            
            row_idx += 1
        
        return table_data
    
    def _save_subsidiary(self, row):
        cursor = self.sqlite_conn.cursor()
        cursor.execute(
            "INSERT INTO subsidiaries (year, company_name, data_json) VALUES (?, ?, ?)",
            (row['year'], row['company_name'], row['data_json'])
        )
    
    # ========== 텍스트 → 벡터DB ==========
    
    def parse_text_sections(self, html_path: str, year: int):
        """텍스트 섹션 추출 → 청크 → 벡터DB"""
        
        # HTML 읽기
        for encoding in ['euc-kr', 'cp949', 'latin1']:
            try:
                with open(html_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except:
                continue
        
        soup = BeautifulSoup(html_content, 'lxml')
        
        # 표 제거
        for table in soup.find_all('table'):
            table.decompose()
        
        full_text = soup.get_text()
        
        # 주요 제목
        titles = [
            "핵심감사사항", "감사의견",
            "재무제표에 대한 경영진과 지배기구의 책임",
            "재무제표감사에 대한 감사인의 책임",
            "내부회계관리제도"
        ]
        
        positions = []
        for title in titles:
            pos = full_text.find(title)
            if pos != -1:
                positions.append((pos, title))
        
        # 주석 패턴
        for m in re.finditer(r'(?:^|\n)\s*([가-힣]{1,2}\.|[0-9]{1,2}\.)\s+([가-힣\s,()]+)', full_text):
            positions.append((m.start(), f"{m.group(1)} {m.group(2).strip()}"))
        
        positions.sort()
        
        # 섹션 추출 → 청크
        chunk_count = 0
        
        for i, (pos, title) in enumerate(positions):
            end = positions[i+1][0] if i+1 < len(positions) else len(full_text)
            text = full_text[pos:end].strip()
            
            if len(text) < 100:
                continue
            
            # 청크 분할
            for j in range(0, len(text), self.chunk_size):
                chunk = text[j:j + self.chunk_size]
                
                if len(chunk) < 100:
                    continue
                
                doc_id = f"{year}_{title[:20].replace(' ', '_')}_{j}"
                
                self.text_collection.add(
                    documents=[chunk],
                    metadatas=[{
                        'year': year,
                        'section_title': title,
                        'chunk_index': j // self.chunk_size
                    }],
                    ids=[doc_id]
                )
                chunk_count += 1
        
        return chunk_count
    
    # ========== 실행 ==========
    
    def process_year(self, html_path: str, year: int):
        """연도 처리"""
        print(f"\n{'='*70}")
        print(f"{year}년 처리")
        print(f"{'='*70}")
        
        sub_count = self.parse_company_tables(html_path, year)
        bal_count = self.parse_balance_sheet(html_path, year)
        chunk_count = self.parse_text_sections(html_path, year)
        
        print(f"  → 종속기업: {sub_count}개")
        print(f"  → 재무상태표: {bal_count}개")
        print(f"  → 텍스트 청크: {chunk_count}개")
    
    def process_all(self, base_path: str, years: List[int]):
        """전체 처리"""
        for year in years:
            html_path = f"{base_path}/감사보고서_{year}.htm"
            if os.path.exists(html_path):
                self.process_year(html_path, year)
    
    def close(self):
        self.sqlite_conn.close()


# ========== 실행 ==========

if __name__ == "__main__":
    
    parser = AuditReportParser(
        sqlite_db_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/audit_data.db",
        vector_db_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/chroma_sections",
        chunk_size=1000,
        reset_db=True
    )
    
    base_path = "/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/삼성전자_감사보고서_2014_2024"
    years = list(range(2014, 2025))
    
    print("\n감사보고서 파싱")
    print("="*70)
    
    parser.process_all(base_path, years)
    
    print("\n" + "="*70)
    print("완료")
    print("="*70)
    
    cursor = parser.sqlite_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM subsidiaries")
    print(f"종속기업: {cursor.fetchone()[0]}개")
    
    cursor.execute("SELECT COUNT(*) FROM balance_sheet")
    print(f"재무상태표: {cursor.fetchone()[0]}개")
    
    print(f"텍스트 청크: {parser.text_collection.count()}개")
    
    parser.close()