"""감사보고서 섹션 기반 처리 시스템
- 섹션 단위로 벡터DB 저장
- "기업명" 헤더 있는 표 전부 파싱
- 재무상태표 파싱 (병합 셀 처리)
"""

from bs4 import BeautifulSoup
import chromadb
from typing import List, Dict
import pandas as pd
import sqlite3
import re
import os
import json


class AuditReportSectionProcessor:
    """감사보고서 섹션 기반 처리"""
    
    def __init__(self, 
                 vector_db_path: str = "./chroma_sections",
                 sqlite_db_path: str = "./audit_data.db",
                 reset_db: bool = False):
        
        # 벡터DB 초기화
        if reset_db:
            print(f"⚠ 벡터DB 완전 초기화...")
            if os.path.exists(vector_db_path):
                import shutil
                shutil.rmtree(vector_db_path)
                print(f"  ✓ 폴더 삭제: {vector_db_path}")
        
        self.chroma_client = chromadb.PersistentClient(path=vector_db_path)
        self.section_collection = self.chroma_client.get_or_create_collection(
            name="audit_sections",
            metadata={"description": "감사보고서 섹션 인덱스"}
        )
        
        # SQLite 초기화
        self.sqlite_conn = sqlite3.connect(sqlite_db_path)
        self._create_sqlite_tables(reset_db)
        
        print(f"✓ 벡터DB: {vector_db_path}, 섹션 {self.section_collection.count()}개")
        print(f"✓ SQLite: {sqlite_db_path}")
    
    def _create_sqlite_tables(self, reset: bool = False):
        """SQLite 테이블 생성"""
        cursor = self.sqlite_conn.cursor()
        
        if reset:
            cursor.execute("DROP TABLE IF EXISTS subsidiaries")
            cursor.execute("DROP TABLE IF EXISTS balance_sheet")
            print(f"  ✓ SQLite 테이블 초기화")
        
        # 종속기업/관계기업 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subsidiaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                company_name TEXT,
                data_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 재무상태표 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                account_name TEXT,
                data_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.sqlite_conn.commit()
    
    # ========== 1. 섹션 분리 ==========
    
    def split_into_sections(self, html_path: str, year: int) -> List[Dict]:
        """HTML을 섹션 단위로 분리"""
        
        print(f"\n{'='*70}")
        print(f"{year}년 섹션 분리")
        print(f"{'='*70}")
        
        # HTML 읽기
        encodings = ['euc-kr', 'cp949', 'latin1']
        html_content = None
        
        for encoding in encodings:
            try:
                with open(html_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                print(f"  ✓ 인코딩: {encoding}")
                break
            except:
                continue
        
        if not html_content:
            raise ValueError(f"파일을 읽을 수 없습니다: {html_path}")
        
        # 주석 번호 패턴으로 섹션 찾기
        pattern = r'(?:^|\n)\s*([가-힣]{1,2}\.|[0-9]{1,2}\.)\s+([가-힣\s,()]+)'
        
        sections = []
        soup = BeautifulSoup(html_content, 'lxml')
        full_text = soup.get_text()
        
        # 섹션 제목 위치 찾기
        matches = list(re.finditer(pattern, full_text))
        
        print(f"  ✓ {len(matches)}개 섹션 후보 발견")
        
        # 각 섹션 추출
        for i, match in enumerate(matches):
            section_num = match.group(1)
            section_title = match.group(2).strip()
            
            # 섹션 시작/끝 위치
            start_pos = match.start()
            end_pos = matches[i+1].start() if i+1 < len(matches) else len(full_text)
            
            # 섹션 내용
            section_text = full_text[start_pos:end_pos]
            
            # 너무 짧은 섹션 스킵
            if len(section_text) < 100:
                continue
            
            sections.append({
                'year': year,
                'section_num': section_num,
                'section_title': section_title,
                'start_pos': start_pos,
                'end_pos': end_pos,
                'text': section_text[:2000],
                'filepath': html_path
            })
        
        print(f"  ✓ {len(sections)}개 유효 섹션 추출")
        return sections
    
    # ========== 2. 벡터DB 저장 ==========
    
    def store_sections(self, sections: List[Dict], year: int):
        """섹션을 벡터DB에 저장"""
        
        print(f"\n{'='*70}")
        print(f"{year}년 섹션 벡터화")
        print(f"{'='*70}")
        
        # 기존 연도 데이터 삭제
        try:
            results = self.section_collection.get(where={"year": year})
            if results['ids']:
                self.section_collection.delete(ids=results['ids'])
                print(f"  ✓ 기존 {len(results['ids'])}개 섹션 삭제")
        except:
            pass
        
        # 저장
        documents = []
        metadatas = []
        ids = []
        
        for i, section in enumerate(sections):
            # 검색용 텍스트
            search_text = f"{section['section_num']} {section['section_title']}\n{section['text']}"
            documents.append(search_text)
            
            # 메타데이터
            metadatas.append({
                'year': section['year'],
                'section_num': section['section_num'],
                'section_title': section['section_title'],
                'start_pos': section['start_pos'],
                'end_pos': section['end_pos'],
                'filepath': section['filepath']
            })
            
            ids.append(f"{year}_section_{i}")
        
        self.section_collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )
        
        print(f"  ✓ {len(sections)}개 섹션 저장 완료\n")
    
    # ========== 3. 기업명 표 파싱 ==========
    
    def parse_all_company_tables(self, html_path: str, year: int):
        """기업명 헤더 있는 모든 표 파싱"""
        
        print(f"\n{'='*70}")
        print(f"{year}년 '기업명' 표 자동 추출")
        print(f"{'='*70}")
        
        # HTML 읽기
        encodings = ['euc-kr', 'cp949', 'latin1']
        html_content = None
        
        for encoding in encodings:
            try:
                with open(html_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except:
                continue
        
        if not html_content:
            print(f"  ✗ 파일 읽기 실패")
            return
        
        soup = BeautifulSoup(html_content, 'lxml')
        all_tables = soup.find_all('table')
        
        print(f"  ✓ 총 {len(all_tables)}개 표 발견")
        
        # "기업명" 헤더 있는 표 찾기
        company_tables = []
        
        for i, table in enumerate(all_tables):
            first_row = table.find('tr')
            if not first_row:
                continue
            
            header_text = first_row.get_text()
            
            if '기업명' in header_text:
                company_tables.append({
                    'index': i + 1,
                    'table': table
                })
        
        print(f"  ✓ '기업명' 헤더 있는 표 {len(company_tables)}개 발견")
        
        # 각 표 파싱
        total_rows = 0
        
        for info in company_tables:
            df = self._parse_company_table(info['table'], year)
            
            if not df.empty:
                print(f"    표 {info['index']}: {len(df)}개 기업")
                
                # SQLite에 저장
                for _, row in df.iterrows():
                    self._save_subsidiary_row(row)
                    total_rows += 1
        
        self.sqlite_conn.commit()
        print(f"\n  ✓ 총 {total_rows}개 데이터 저장 완료")
    
    def _parse_company_table(self, table, year: int) -> pd.DataFrame:
        """단일 기업명 표 파싱"""
        
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return pd.DataFrame()
        
        # 헤더
        header_row = rows[0]
        headers = [c.get_text(strip=True) for c in header_row.find_all(['th', 'td'])]
        
        # 데이터 행
        all_data = []
        
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) < 2:
                continue
            
            cell_texts = [c.get_text(strip=True) for c in cells]
            
            # 회사명 찾기
            company_name = None
            
            for text in cell_texts:
                # Samsung 포함
                if 'Samsung' in text or 'SAMSUNG' in text:
                    company_name = text
                    break
                # 영문 회사명
                if re.match(r'^[A-Z][a-zA-Z\s&\-().]+', text) and len(text) > 3:
                    if text not in ['Korea', 'China', 'USA', 'Japan', 'Vietnam', 'India', 'Europe']:
                        company_name = text
                        break
                # 한글 회사명
                if re.match(r'^[가-힣()㈜]+', text) and len(text) > 2:
                    company_name = text
                    break
            
            if not company_name:
                continue
            
            # 전체 행 데이터를 JSON으로
            row_dict = {}
            for i, text in enumerate(cell_texts):
                if i < len(headers) and headers[i]:
                    # 숫자 변환 시도
                    clean_text = text.replace(',', '').replace('(', '-').replace(')', '')
                    
                    # 큰 정수
                    if re.match(r'^-?\d{5,}$', clean_text):
                        try:
                            row_dict[headers[i]] = int(clean_text)
                            continue
                        except:
                            pass
                    
                    # 소수점
                    if re.match(r'^-?\d+\.\d+$', clean_text):
                        try:
                            row_dict[headers[i]] = float(clean_text)
                            continue
                        except:
                            pass
                    
                    # 텍스트
                    if text and text != '-':
                        row_dict[headers[i]] = text
            
            all_data.append({
                'year': year,
                'company_name': company_name,
                'data_json': json.dumps(row_dict, ensure_ascii=False)
            })
        
        return pd.DataFrame(all_data)
    
    def _save_subsidiary_row(self, row):
        """종속기업 데이터 저장"""
        cursor = self.sqlite_conn.cursor()
        cursor.execute("""
            INSERT INTO subsidiaries (year, company_name, data_json)
            VALUES (?, ?, ?)
        """, (row['year'], row['company_name'], row['data_json']))
    
    # ========== 4. 재무상태표 파싱 ==========
    
    def auto_extract_balance_sheet(self, html_path: str, year: int):
        """재무상태표 자동 추출 - 병합 셀 처리 + 정규화 + 당기/전기 분리"""
        
        print(f"\n{'='*70}")
        print(f"{year}년 재무상태표 자동 추출")
        print(f"{'='*70}")
        
        # HTML 읽기
        encodings = ['euc-kr', 'cp949', 'latin1']
        html_content = None
        
        for encoding in encodings:
            try:
                with open(html_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except:
                continue
        
        if not html_content:
            print(f"  ✗ 파일 읽기 실패")
            return
        
        soup = BeautifulSoup(html_content, 'lxml')
        all_tables = soup.find_all('table')
        
        if len(all_tables) < 5:
            print(f"  ✗ 표가 충분하지 않음")
            return
        
        # 표 1~15번 중 가장 큰 표
        front_tables = all_tables[:15]
        largest_table = max(front_tables, key=lambda t: len(t.find_all('tr')))
        
        rows = largest_table.find_all('tr')
        print(f"  ✓ 첫 번째 큰 표 발견 ({len(rows)}개 행)")
        
        # 파싱
        cursor = self.sqlite_conn.cursor()
        count = 0
        
        # 2차원 배열로 변환
        table_data = self._parse_table_with_merged_cells(largest_table)
        
        if len(table_data) < 2:
            print(f"  ✗ 데이터 없음")
            return
        
        # 헤더에서 연도 추출 (당기/전기 구분)
        header_row = table_data[0]
        year_columns = []
        
        for i, header in enumerate(header_row[1:], start=1):
            year_match = re.search(r'(\d{4})', header)
            if year_match:
                col_year = int(year_match.group(1))
                year_columns.append((i, col_year))
        
        # 당기 컬럼 찾기 (가장 최근 연도)
        if year_columns:
            year_columns.sort(key=lambda x: x[1], reverse=True)
            current_col_idx = year_columns[0][0]
            current_year = year_columns[0][1]
            print(f"  ✓ 당기 컬럼: {current_col_idx}번째 ({current_year}년)")
        else:
            current_col_idx = 1
            current_year = year
            print(f"  ✓ 당기 컬럼: 2번째 (추정)")
        
        # 데이터 저장
        for row_data in table_data[1:]:
            if len(row_data) < 2:
                continue
            
            # 계정과목 (정규화: 공백 제거)
            account_name_raw = row_data[0].strip()
            account_name = account_name_raw.replace(' ', '')
            
            if not account_name or len(account_name) < 2:
                continue
            
            # 당기 금액 추출
            if current_col_idx < len(row_data):
                amount_text = row_data[current_col_idx].replace(',', '').replace(' ', '').strip()
                
                # 합계 행 여부
                is_summary_row = any(prefix in account_name for prefix in ['Ⅰ.', 'Ⅱ.', 'Ⅲ.', 'Ⅳ.', '총계', '합계'])
                
                # 숫자 확인
                if re.match(r'^-?\d+$', amount_text):
                    pass
                elif is_summary_row:
                    # 합계 행인데 금액 비어있으면 다른 열에서 찾기
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
                
                # 저장
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
        print(f"  ✓ {count}개 계정과목 저장 (당기: {current_year}년)")
    
    def _parse_table_with_merged_cells(self, table) -> List[List[str]]:
        """병합 셀을 처리하여 2차원 배열로 변환"""
        
        rows = table.find_all('tr')
        
        # 최대 열 개수 계산
        max_cols = 0
        for row in rows:
            cells = row.find_all(['td', 'th'])
            col_count = sum(int(cell.get('colspan', 1)) for cell in cells)
            max_cols = max(max_cols, col_count)
        
        # 2차원 배열 초기화
        table_data = []
        row_idx = 0
        
        for row in rows:
            if row_idx >= len(table_data):
                table_data.append([''] * max_cols)
            
            cells = row.find_all(['td', 'th'])
            col_idx = 0
            
            for cell in cells:
                # 이미 채워진 셀 건너뛰기 (rowspan 처리)
                while col_idx < max_cols and table_data[row_idx][col_idx] != '':
                    col_idx += 1
                
                if col_idx >= max_cols:
                    break
                
                cell_value = cell.get_text(strip=True)
                colspan = int(cell.get('colspan', 1))
                rowspan = int(cell.get('rowspan', 1))
                
                # 병합된 모든 셀에 값 채우기
                for r in range(rowspan):
                    if row_idx + r >= len(table_data):
                        table_data.append([''] * max_cols)
                    
                    for c in range(colspan):
                        if col_idx + c < max_cols:
                            table_data[row_idx + r][col_idx + c] = cell_value
                
                col_idx += colspan
            
            row_idx += 1
        
        return table_data
    
    # ========== 5. 전체 처리 ==========
    
    def process_year(self, html_path: str, year: int):
        """특정 연도 전체 처리"""
        
        try:
            # 섹션 분리 + 벡터화
            sections = self.split_into_sections(html_path, year)
            self.store_sections(sections, year)
            
            # 데이터 추출
            self.parse_all_company_tables(html_path, year)
            self.auto_extract_balance_sheet(html_path, year)
            
        except Exception as e:
            print(f"\n✗ {year}년 처리 실패: {e}")
    
    def process_all_years(self, base_path: str, years: List[int]):
        """전체 연도 처리"""
        
        for year in years:
            html_path = f"{base_path}/감사보고서_{year}.htm"
            
            if not os.path.exists(html_path):
                print(f"\n✗ {year}년 파일 없음")
                continue
            
            self.process_year(html_path, year)
    
    def close(self):
        """연결 종료"""
        self.sqlite_conn.close()


# ========== 실행 ==========

if __name__ == "__main__":
    
    # 초기화 (절대경로, 매번 완전 초기화)
    processor = AuditReportSectionProcessor(
        vector_db_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/chroma_sections",
        sqlite_db_path="/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/audit_data.db",
        reset_db=True
    )
    
    # 경로
    base_path = "/Users/parkhyeonseo/Documents/SNU12_ABS_Code/프로젝트/자연어처리/삼성전자_감사보고서_2014_2024"
    years = list(range(2014, 2025))
    
    print("\n" + "="*70)
    print("감사보고서 섹션 기반 처리")
    print("="*70)
    print(f"연도: {years[0]}~{years[-1]}")
    print("="*70)
    
    # 전체 처리
    processor.process_all_years(base_path, years)
    
    # 결과 확인
    print("\n" + "="*70)
    print("처리 결과")
    print("="*70)
    
    cursor = processor.sqlite_conn.cursor()
    
    # 종속기업
    cursor.execute("SELECT year, COUNT(*) FROM subsidiaries GROUP BY year ORDER BY year")
    print("\n[종속기업/관계기업]")
    for year, count in cursor.fetchall():
        print(f"{year}년: {count}개")
    
    # 재무상태표
    cursor.execute("SELECT year, COUNT(*) FROM balance_sheet GROUP BY year ORDER BY year")
    print("\n[재무상태표]")
    for year, count in cursor.fetchall():
        print(f"{year}년: {count}개")
    
    processor.close()
    print("\n✓ 처리 완료")
