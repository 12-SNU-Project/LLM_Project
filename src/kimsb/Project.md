# Samsung Audit Report RAG Project

## 1. 프로젝트 개요


이 프로젝트는 **삼성전자 감사보고서 HTML 파일**을 바탕으로,  
사용자의 질문에 대해 관련 내용을 찾아주는 **RAG(Retrieval-Augmented Generation) 기반 질의응답 시스템**을 만드는 것을 목표로 합니다.

프로젝트의 전체 흐름은 다음과 같습니다.

1. HTML 감사보고서 파일에서 **텍스트와 표(table)** 를 추출한다.
2. 추출한 텍스트와 표를 검색하기 좋은 형태로 **chunk(조각)** 단위로 재구성한다.
3. 텍스트/표 chunk를 임베딩하여 **Vector DB(FAISS)** 를 만든다.
4. 표 데이터는 별도로 **SQLite DB** 로 저장하여 숫자 질의에 대응한다.
5. 사용자의 질문을 입력받아  
   - 설명형 질문은 Vector DB 검색
   - 수치형 질문은 SQLite 검색  
   방식으로 답변을 생성한다. :contentReference[oaicite:0]{index=0} :contentReference[oaicite:1]{index=1} :contentReference[oaicite:2]{index=2} :contentReference[oaicite:3]{index=3} :contentReference[oaicite:4]{index=4}

---

## 2. 프로젝트 목적

이 프로젝트는 단순히 문서를 저장하는 것이 아니라,  
**감사보고서 안의 텍스트와 재무 수치 정보를 함께 검색**할 수 있도록 구성한 것이 특징입니다.

예를 들어 다음과 같은 질문을 처리하는 것을 목표로 했습니다.

- "2019년 감사의견을 알려줘"
- "재무제표에 대한 경영진의 책임을 설명해줘"
- "2014년 유동자산이 얼마야?"
- "2019년 영업이익이 얼마야?"

즉,  
**설명형 질문**과 **수치형 질문**을 분리해서 처리하려고 설계한 프로젝트입니다. :contentReference[oaicite:5]{index=5} :contentReference[oaicite:6]{index=6}

---

## 3. 파일 구성

프로젝트에서 핵심이 되는 파이썬 파일은 아래 5개입니다.

- `1_parse_html.py`
- `2_chunking.py`
- `3_vector_db.py`
- `5_build_SQLite.py`
- `6_chatbot.py`

각 파일은 순서대로 실행되며, 앞 단계의 결과를 다음 단계에서 사용합니다. :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9} :contentReference[oaicite:10]{index=10} :contentReference[oaicite:11]{index=11}

---

## 4. 전체 동작 흐름

### Step 1. HTML 파싱
`1_parse_html.py`

이 단계에서는 `data/` 폴더 안의 HTML 파일들을 읽어서 다음 두 가지를 분리합니다.

- **텍스트(text)**
- **표(table)**

텍스트는 문단 단위로 정리해서 `parsed_text.csv`로 저장하고,  
표는 행(row) 단위로 정리해서 `parsed_table.csv`로 저장합니다. :contentReference[oaicite:12]{index=12} :contentReference[oaicite:13]{index=13} :contentReference[oaicite:14]{index=14}

이 단계에서 추가로 한 일:
- 파일명에서 연도를 추출
- 불필요한 짧은 줄/잡문장 제거
- 섹션 제목 후보 판별
- 표와 텍스트를 따로 관리

즉, 이후 검색 성능을 높이기 위한 **전처리 단계**라고 볼 수 있습니다. :contentReference[oaicite:15]{index=15} :contentReference[oaicite:16]{index=16} :contentReference[oaicite:17]{index=17}

---

### Step 2. Chunking
`2_chunking.py`

이 단계에서는 Step 1에서 만든 CSV를 검색하기 쉬운 형태로 다시 나눕니다.

#### 1) 텍스트 chunk
텍스트는 `file_name + year + section_hint` 기준으로 묶은 뒤,  
길이가 너무 길면 다시 잘라서 `chunked_text.csv`로 저장합니다.  
또한 중요한 섹션(예: 감사의견, 경영진의 책임, 감사인의 책임 등)은 prefix를 추가해 검색에 더 잘 잡히도록 설계했습니다. :contentReference[oaicite:18]{index=18} :contentReference[oaicite:19]{index=19}

#### 2) 표 row chunk
표는 각 row를 하나의 검색 단위로 만들어 `chunked_table_rows.csv`로 저장합니다.  
이는 수치 관련 row를 개별적으로 찾기 위한 용도입니다. :contentReference[oaicite:20]{index=20}

#### 3) 표 group chunk
표 전체를 너무 크게 넣으면 노이즈가 커질 수 있어서,  
몇 개의 row를 묶어서 `chunked_table_groups.csv`로 저장합니다.  
이 파일은 표의 맥락을 파악하는 데 도움을 줍니다. :contentReference[oaicite:21]{index=21}

---

### Step 3. Vector DB 구축
`3_vector_db.py`

이 단계에서는 Step 2에서 만든 세 가지 chunk 파일을 모두 읽어서 하나의 검색 대상 집합으로 합칩니다.

- `chunked_text.csv`
- `chunked_table_rows.csv`
- `chunked_table_groups.csv`

그 후 `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` 모델을 사용하여 임베딩을 만들고,  
FAISS 인덱스를 생성해 `vector.index`에 저장합니다.  
검색 결과의 메타데이터는 `metadata.pkl`로 저장합니다. :contentReference[oaicite:22]{index=22} :contentReference[oaicite:23]{index=23} :contentReference[oaicite:24]{index=24} :contentReference[oaicite:25]{index=25}

즉, 설명형 질문에 대해 관련 문장을 찾기 위한 **벡터 검색 DB 구축 단계**입니다.

---

### Step 5. SQLite 구축
`5_build_SQLite.py`

이 단계에서는 `parsed_table.csv`를 기반으로 두 종류의 SQLite 테이블을 만듭니다.

#### 1) `table_rows`
원본 표 row를 그대로 저장하는 테이블입니다.  
나중에 디버깅하거나 원본을 추적할 때 사용합니다. :contentReference[oaicite:26]{index=26}

#### 2) `financial_facts`
표 row에서 회계 계정명, 연도, 값 등을 추출해 구조화한 테이블입니다.  
예를 들면 다음과 같은 형태로 저장됩니다.

- 파일명
- 문서 연도
- 재무제표 종류
- 계정명
- 대상 연도
- 값

이 테이블은 `"2014년 유동자산 얼마야?"` 같은 **수치형 질문**에 답하기 위해 사용됩니다. :contentReference[oaicite:27]{index=27} :contentReference[oaicite:28]{index=28}

이 단계에서 한 일:
- 숫자 문자열 정규화
- 연도 header 탐지
- 계정명처럼 보이는 첫 번째 셀 판별
- 값 셀 파싱
- 재무상태표/손익계산서/현금흐름표 등 재무제표 종류 추정

즉, 표 데이터를 단순 문자열이 아니라 **질문 가능한 구조형 데이터**로 바꾸는 과정입니다. :contentReference[oaicite:29]{index=29} :contentReference[oaicite:30]{index=30} :contentReference[oaicite:31]{index=31}

---

### Step 6. 챗봇 실행
`6_chatbot.py`

이 단계는 최종 질의응답 단계입니다.

사용자가 질문을 입력하면 먼저 질문 유형을 분류합니다.

- **numeric**: 수치형 질문
- **lookup**: 설명형 질문

그 다음:

- 수치형 질문이면 `financial_facts` 테이블을 조회
- 설명형 질문이면 FAISS Vector DB에서 관련 chunk를 검색

하는 방식으로 답변을 생성합니다. :contentReference[oaicite:32]{index=32} :contentReference[oaicite:33]{index=33} :contentReference[oaicite:34]{index=34}

답변 출력 시에는 다음 정보를 함께 보여줍니다.

- 질문 유형
- 생성된 답변
- 근거
- 출처 파일

즉, 전체 프로젝트의 최종 인터페이스 역할을 합니다. :contentReference[oaicite:35]{index=35}

---

## 5. 실행 순서

프로젝트를 처음부터 실행하려면 아래 순서대로 실행하면 됩니다.

```bash
python src/1_parse_html.py
python src/2_chunking.py
python src/3_vector_db.py
python src/5_build_SQLite.py
python src/6_chatbot.py


 ## 6. 한계점
 1. 수치형 질문 정확도 한계
 - 유동자산 대신 기타유동자산을 반환하는 등의 오류가 발생함. 이는 표의 header 구조, 계정명 판별, 값 매칭 규칙에 문제가 있는 것으로 보임.
 2. 설명형 질문 정확도 한계
 - 의미적으로 비슷하지만 질문과 직접 연관 없는 chunk가 선택된것으로 보임.
 3. 표 구조 해석의 한계
 - 재무제표 HTML은 구조가 복잡하고 표마다 형식이 다르기 때문에, 현재 규칙 기반으로 모든 경우를 완벽하게 처리하지 못하고 있음.


 ## 7. 향후 개선 방향
 - 계정명 매칭을 exact match 중심으로 더 정교하게 개선
 - financial_facts 생성 시 표 구조 해석 로직 강화
 - lookup 질문에 대해 section 기반 reranking 개선
 - 감사의견, 경영진의 책임, 핵심감사사항 전용 검색 규칙 추가
 - LLM을 이용한 최종 답변 요약/정제 단계 추가 필요


