# Audit Report Structure-First Prototype

## 목적
이 프로젝트는 `2014~2024` 감사보고서 HTML을 `structure-first` 방식으로 파싱해,
회계 질의응답 시스템에 필요한 두 종류의 저장소를 만든다.

- SQLite RDB: 표에서 나온 정량 fact + 표 전체 맥락 저장
- Chroma VDB: 본문 설명 텍스트 chunk 저장

핵심 원칙은 아래와 같다.

1. HTML을 먼저 markdown으로 바꾸지 않는다.
2. DOM/블록 구조를 보존한 뒤, 표와 본문을 따로 정규화한다.
3. 숫자는 SQL로 찾고, 설명은 Chroma로 찾는다.
4. 1차 검색 후 `EvidenceExpansionPlanner`가 근거 부족 축을 판정하고, 필요 시 `tables_registry`의 표 markdown/footnotes를 추가 근거로 붙인다.
5. 최종 LLM은 검색된 근거를 설명하는 역할만 맡는다.

## 운영 방식

### 1. Offline Ingest
사전에 한 번 실행하는 배치 작업이다.

- 입력: `data/*.htm`
- 출력:
  - `.runtime/audit_qa/sqlite/audit_reports.sqlite3`
  - `.runtime/audit_qa/chroma/audit_chunks`
  - `.runtime/audit_qa/manifests/offline_ingest.json`

하는 일은 다음과 같다.

1. HTML 디코딩
2. block/section/table/text chunk 파싱
3. 표 fact를 SQLite에 적재
4. text chunk를 Chroma에 적재
5. 실행 결과를 manifest에 기록

주의:
- `--strict-runtime` 로 실행하면 `chromadb` import 실패 시 즉시 종료한다.
- `chromadb`가 설치되어 있어도 구버전 `chromadb`와 `pydantic v2` 조합이면 import 단계에서 실패할 수 있다.
- 이 경우 `pydantic-settings`만 추가로 설치해도 해결되지 않을 수 있으며, `chromadb`와 `pydantic` 버전을 함께 맞춰야 한다.
- 최신 `chromadb`는 custom embedding function에 `name`, `build_from_config`, `get_config` 같은 직렬화 메서드를 요구하므로, runtime adapter도 이 계약을 만족해야 한다.
- 최신 `chromadb` query 경로는 custom embedding function에 `embed_query`도 기대하므로, ingest만 성공해도 query adapter가 빠져 있으면 설명형 질의가 런타임에서 실패할 수 있다.
- CLI 진입점은 `requests` 인코딩 경고와 gRPC/absl 초기 노이즈 로그를 기본적으로 억제한다. 실제 실패 원인은 traceback과 manifest runtime report 기준으로 본다.
- `offline_ingest.py`의 Chroma 적재는 기본적으로 배치 단위로 upsert한다. 기본 배치 크기는 `32`이며, `--embedding-batch-size`로 조절할 수 있다.
- 임베딩 경로는 `OllamaEmbeddings` client를 재사용하고, 기본 timeout은 `120초`, 기본 keep-alive는 `300초`다. `--embedding-timeout`, `--embedding-keep-alive`로 조절할 수 있다.
- 현재 스택에서 직접 제어 가능한 가속 경로는 `Apple GPU/Metal`이다. `--ollama-num-gpu 1`이 기본값이며, `0`은 CPU 강제다. Apple Neural Engine(NPU)을 직접 지정하는 옵션은 현재 런타임에 없다.
- 특정 배치가 `ReadTimeout` 나면 해당 배치를 자동으로 더 작은 배치로 분할해 재시도한다. 즉 timeout이 나도 전체 ingest를 즉시 실패시키지 않고, 가능한 한 작은 단위까지 내려가 본다.
- 기본값으로 배치 진행 로그를 출력하며, 필요하면 `--quiet-ingest-progress`로 끌 수 있다.

### 2. Online Service
사전에 구축된 SQLite/Chroma를 읽어서 질문에 답하는 단계다.

1. 사용자 질문 입력
2. 질의 해석 LLM
3. SQL 조회
4. Chroma 조회
5. evidence sufficiency 판정
6. 근거 부족 시 `tables_registry`에서 표 전체 맥락 추가 조회
7. 최종 답변 생성

중요한 점은 온라인 서비스에서는 HTML을 다시 파싱하지 않는다는 것이다.

## 현재 런타임 산출물
현재 runtime DB는 추론에 필요한 최소 구조만 남긴다.

- `filings`
- `tables_registry`
- `metric_facts`
- `text_chunks`

### `filings`
문서 단위 메타데이터다.

- `filing_id`
- `company_name`
- `fiscal_year`
- `auditor_name`
- `auditor_report_date`

### `tables_registry`
표 전체 컨텍스트를 보존하는 레지스트리다.

- `table_id`
- `filing_id`
- `table_title`
- `semantic_table_type`
- `table_unit`
- `table_markdown`
- `footnotes`

### `metric_facts`
표의 숫자값을 평탄화한 fact 테이블이다.

- `table_id`
- `semantic_table_type`
- `table_title`
- `table_unit`
- `row_group_label`
- `raw_label`
- `parent_row_id`
- `is_section_header`
- `column_key`
- `value_raw`
- `value_numeric`
- `page_start`

### `text_chunks`
Chroma에 임베딩되는 본문 청크의 디버그/검수용 원본 기록이다.

- `section_type`
- `section_title`
- `topic_hint`
- `near_table_id`
- `text`
- `page_start`
- `page_end`

## 폴더 역할

### `prototype/core`
파싱과 정규화의 중심이다.

- `html_io.py`: HTML 디코딩
- `parser.py`: block/section 생성
- `table_processor.py`: 표 정규화, 행/값 복원, 의미 분류
- `pipeline.py`: 파싱 결과 조립
- `sqlite_loader.py`: SQLite 적재
- `rdb_schema_draft.sql`: runtime SQLite 스키마

### `prototype/query`
질문을 구조화된 질의로 바꾸는 계층이다.

- `interpreter.py`: 규칙 기반 fallback 해석
- `langchain_interpreter.py`: LangChain + 로컬 LLM 기반 질의 해석
- `policy.py`: `need_sql / need_vdb / clarification_needed` 확정
- `sql_templates.py`: 템플릿 기반 SQL 생성/실행 + 표 컨텍스트 조회

### `prototype/retrieval`
Chroma 문서화와 SQL/VDB 결합 담당 계층이다.

- `chroma_metadata.py`: text chunk -> Chroma 문서
- `chroma_store.py`: Chroma persistent store 어댑터
- `evidence_planner.py`: 근거 요구 축 / 부족 축 / 표 보강 계획 계산
- `fusion.py`: SQL + vector retrieval 결합
- `organizer.py`: 최종 evidence 정리
- `schema.py`: `sql_results / vector_hits / table_contexts / evidence_gaps` 묶음 정의

### `prototype/llm`
로컬 LLM과 임베딩 모델 어댑터다.

- 질의 해석 LLM: `qwen3:8b`
- 최종 답변 LLM: `qwen3:8b`
- 임베딩 모델: `qwen3-embedding:8b`

### `prototype/service`
전체 오케스트레이션 계층이다.

- `runtime_factory.py`: LangChain/Ollama/Chroma 조립
- `runtime_factory.py`: LangChain/Ollama/Chroma 조립 + batched vector ingest
- `hybrid_qa.py`: 질의 해석 -> SQL/VDB -> evidence planner -> 표 컨텍스트 보강 -> answer 흐름 연결

### `prototype/support/cli`
실행용 엔트리포인트다.

- `offline_ingest.py`: 배치 구축
- `service_cli.py`: 대화형 질의 실행

### `prototype/support/answering`
최종 답변 생성 계층이다.

- `composer.py`: evidence 기반 답변 조합
- `schema.py`: 최종 응답 스키마

### `prototype/support/tests`
회귀 테스트 계층이다.

- `regression_suite.py`: `2014~2024` 전 파일 + 대표 질의 검증

## 호출 흐름

### Offline Ingest
1. `data/*.htm`
2. `core/html_io.py`
3. `core/parser.py`
4. `core/table_processor.py`
5. `core/pipeline.py`
6. `core/sqlite_loader.py`
7. `retrieval/chroma_metadata.py`
8. `retrieval/chroma_store.py`
9. `offline_ingest.py`

Chroma 적재는 `vector_store.upsert_documents(documents)`에서 문서 전체를 한 번에 embed하지 않고, 배치 단위로 나눠 `Ollama`에 보낸다. 따라서 strict runtime에서 응답 지연이 생길 때는 먼저 배치 크기를 줄여 본다.

### Online Service
1. 사용자 질문
2. `service/runtime_factory.py`
3. `query/langchain_interpreter.py`
4. `query/policy.py`
5. `query/sql_templates.py`
6. `retrieval/fusion.py`
7. `retrieval/organizer.py`
8. `retrieval/evidence_planner.py`
9. `service/hybrid_qa.py`에서 부족 근거 축에 맞춰 `tables_registry` 추가 조회
10. `support/answering/composer.py`

## 파싱 전략

### 구조 보존
HTML을 먼저 아래 단위로 분해한다.

- `cover`
- `section_heading`
- `paragraph`
- `table`
- `footnote`
- `page_break`

### 표 정규화
표는 단순 `<table>` 문자열이 아니라, 다음 순서로 정규화한다.

1. `rowspan/colspan`을 펼쳐 logical grid 생성
2. 단위 행 추출 및 노이즈 제거
3. header path 복원
4. row hierarchy 복원
5. footnote 추출
6. 표 전체 markdown 생성
7. 숫자 cell만 fact로 추출
8. 표의 회계 의미를 `semantic_table_type`으로 분류

### 본문 chunking
본문은 section-aware 방식으로 자른다.

1. 섹션 제목을 먼저 인식
2. 섹션 내부 문단을 모음
3. 길이 제한을 넘으면 문단 경계로 분리
4. 각 chunk에 `section_type`, `section_title`, `page`, `near_table_id`를 저장

## 온라인 검색 전략

### 1차 라우팅
질문 해석 결과로 `need_sql / need_vdb / clarification_needed`를 확정한다.

- `metric_lookup`: SQL 우선
- `metric_with_explanation`: SQL + VDB 동시 조회
- `text_explanation`: section/table anchor를 보고 SQL 또는 VDB 또는 둘 다 조회
- `table_cell_lookup`: SQL 우선

### 2차 표 컨텍스트 보강
`EvidenceExpansionPlanner`가 먼저 아래를 계산한다.

- 질문이 요구하는 근거 축
- 현재 충족된 근거 축
- 부족한 근거 축
- 표 컨텍스트로 보강 가능한 축

그 다음 SQL 결과의 `table_id`와 vector hit의 `near_table_id`를 함께 후보로 모아 `tables_registry`를 조회한다.
즉, 특정 질의 타입별 분기보다 `evidence gap` 중심으로 표 보강 여부를 결정한다.

이 단계에서는 `table_id` 기준으로 다음 근거를 가져온다.

- `table_markdown`
- `table_unit`
- `footnotes`

다만 answer 단계에는 표 원문 전체를 그대로 넣지 않고, 질의와 SQL anchor에 맞춰 잘라낸 `focused_table_markdown`, `focused_footnotes`를 우선 사용한다.
즉, "표가 필요하면 표 전체를 붙이는 방식"이 아니라 "관련 행/열/각주만 축약해서 붙이는 방식"이다.

최종 answer prompt에는 `sql_results`, `vector_hits`, `table_contexts`, `evidence_requirements`, `evidence_gaps`가 함께 들어간다.

## 검수 원칙

앞으로 코드 수정 후 기본 검수 절차는 아래 순서를 따른다.

1. `data/audit_report_2014.htm` ~ `data/audit_report_2024.htm` 전 파일 parse/load
2. SQLite 산출물에서 `tables_registry / metric_facts / text_chunks` 점검
3. 표 단위, footnote, parent hierarchy 샘플 검토
4. `prototype/support/tests/regression_suite.py` 실행
5. 고정 질의 외에 회계 전문가 관점의 확장 질의로 `focused table context`와 `section match`까지 확인

즉, 단일 파일 예제만 통과하는 변경은 승인 기준으로 보지 않는다.

## 파싱 전략 예시
아래 예시는 실제 감사보고서의 표/본문이 runtime DB에 어떻게 보존되는지 보여준다.

### 예시 1. 2024 특수관계자 거래표
<table>
  <tr>
    <th align="left">실제 HTM 표 일부</th>
    <th align="left">SQLite `metric_facts` 저장 형태</th>
  </tr>
  <tr>
    <td valign="top">
      <pre>
표 제목:
나. 당기 및 전기 중 특수관계자와의
매출ㆍ매입 등 거래 내역은 다음과 같습니다. (1) 당기

상위 구분: 종속기업
기업명: Samsung Semiconductor, Inc. (SSI)
매출 등: 42,993,409
매입 등:   952,847
      </pre>
    </td>
    <td valign="top">
      <pre>
semantic_table_type = related_party_transaction_table
table_title          = 나. 당기 및 전기 중 특수관계자와의 매출ㆍ매입 등 거래 내역...
row_group_label      = 종속기업
raw_label            = Samsung Semiconductor, Inc. (SSI)
column_key           = 매출_등
value_raw            = 42,993,409
page_start           = 94

semantic_table_type = related_party_transaction_table
raw_label            = Samsung Semiconductor, Inc. (SSI)
column_key           = 매입_등
value_raw            = 952,847
page_start           = 94
      </pre>
    </td>
  </tr>
</table>

이 예시의 핵심은 `SSI`라는 이름을 찾는 것이 아니라,
이 표가 `회사 전체 매출표`가 아니라 `특수관계자 거래표`라는 의미까지 함께 보존한다는 점이다.

### 예시 2. 2024 특수관계자 잔액표
<table>
  <tr>
    <th align="left">실제 HTM 표 일부</th>
    <th align="left">SQLite `metric_facts` 저장 형태</th>
  </tr>
  <tr>
    <td valign="top">
      <pre>
표 제목:
다. 보고기간종료일 현재 특수관계자에 대한
채권ㆍ채무 등 잔액은 다음과 같습니다. (1) 당기말

상위 구분: 종속기업
기업명: Samsung Semiconductor, Inc. (SSI)
채권 등: 11,910,574
채무 등:    340,273
      </pre>
    </td>
    <td valign="top">
      <pre>
semantic_table_type = related_party_balance_table
row_group_label      = 종속기업
raw_label            = Samsung Semiconductor, Inc. (SSI)
column_key           = 채권_등_2
value_raw            = 11,910,574
page_start           = 98

semantic_table_type = related_party_balance_table
raw_label            = Samsung Semiconductor, Inc. (SSI)
column_key           = 채무_등_3
value_raw            = 340,273
page_start           = 98
      </pre>
    </td>
  </tr>
</table>

### 예시 3. 2019 요약재무정보의 연속 표
<table>
  <tr>
    <th align="left">실제 HTM 표 일부</th>
    <th align="left">SQLite `metric_facts` 저장 형태</th>
  </tr>
  <tr>
    <td valign="top">
      <pre>
앞 표 제목:
다. 보고기간종료일 현재 주요 종속기업 및 관계기업
투자의 요약 재무정보는 다음과 같습니다.
(1) 주요 종속기업 1) 당기

이어지는 다음 표 제목:
2) 전기

기업명: Samsung Semiconductor, Inc.(SSI)
자산:   9,306,621
부채:   4,288,544
매출액: 29,592,773
      </pre>
    </td>
    <td valign="top">
      <pre>
semantic_table_type = subsidiary_summary_financial_table
table_title          = 2) 전기
raw_label            = Samsung Semiconductor, Inc.(SSI)
column_key           = 자산
value_raw            = 9,306,621
page_start           = 53

semantic_table_type = subsidiary_summary_financial_table
raw_label            = Samsung Semiconductor, Inc.(SSI)
column_key           = 매출액
value_raw            = 29,592,773
page_start           = 53
      </pre>
    </td>
  </tr>
</table>

이 예시는 제목이 짧은 후속 표(`2) 전기`)라도
직전 문맥과 헤더를 보고 앞 표의 회계 의미를 상속하도록 보강한 사례다.

### 예시 4. 2024 우발부채 본문 chunk
<table>
  <tr>
    <th align="left">실제 HTM 본문 일부</th>
    <th align="left">SQLite `text_chunks` 저장 형태</th>
  </tr>
  <tr>
    <td valign="top">
      <pre>
16. 우발부채와 약정사항:
가. 지급보증한 내역
(1) 보고기간종료일 현재 회사가 해외종속기업의
자금조달 등을 위하여 제공하고 있는 채무보증 내역은...
(2) ...지급보증 한도액은 532,893 백만원입니다.
나. 소송 등
보고기간종료일 현재 회사는 다수의 회사 등과...
      </pre>
    </td>
    <td valign="top">
      <pre>
section_type  = contingent_liabilities_and_commitments
section_title = 16. 우발부채와 약정사항:
topic_hint    = contingent_liabilities_and_commitments
near_table_id = audit_report_2024_2024_b_00396
page_start    = 56
page_end      = 57
text_len      = 766
      </pre>
    </td>
  </tr>
</table>

이 구조 덕분에 정량표와 설명문을 서로 연결해서 사용할 수 있다.

## 사용자 질문은 어떻게 해석되는가
이 시스템은 사용자의 질문을 바로 SQL이나 답변으로 바꾸지 않는다.
먼저 질문의 **의도**를 작게 나눈 뒤, 그 결과를 바탕으로 어느 저장소를 볼지 결정한다.

비전공자 기준으로 쉽게 말하면 아래 순서다.

1. 사용자가 질문한다.
2. 시스템은 먼저 “이 질문이 숫자를 묻는가, 설명을 묻는가, 둘 다 묻는가”를 판단한다.
3. 숫자 질문이면 SQLite를 우선 보고, 설명 질문이면 Chroma를 우선 본다.
4. 필요한 근거를 모은 뒤, 마지막 LLM이 그 근거를 설명하는 문장으로 바꾼다.

즉 LLM이 처음부터 회계 전문가처럼 모든 것을 추론하는 구조가 아니다.
질문을 작게 나누고, 각 단계별로 시스템이 역할을 나눠 갖는 구조다.

### 질문 해석의 실제 예시
<table>
  <tr>
    <th align="left">사용자 질문</th>
    <th align="left">시스템 내부 해석</th>
    <th align="left">조회 대상</th>
  </tr>
  <tr>
    <td valign="top">
      <pre>
2024년 매출액이 얼마야?
      </pre>
    </td>
    <td valign="top">
      <pre>
intent            = metric_lookup
metric_candidates = [revenue]
year              = 2024
need_sql          = true
need_vdb          = false
      </pre>
    </td>
    <td valign="top">
      <pre>
SQLite `metric_facts`
우선 조회
      </pre>
    </td>
  </tr>
  <tr>
    <td valign="top">
      <pre>
2024년 감사의견이 뭐야?
      </pre>
    </td>
    <td valign="top">
      <pre>
intent            = text_explanation
section_candidates = [audit_opinion]
need_sql          = false
need_vdb          = true
      </pre>
    </td>
    <td valign="top">
      <pre>
Chroma `text_chunks`
우선 조회
      </pre>
    </td>
  </tr>
  <tr>
    <td valign="top">
      <pre>
2024년 매출액과 관련 설명을 알려줘
      </pre>
    </td>
    <td valign="top">
      <pre>
intent            = metric_with_explanation
metric_candidates = [revenue]
year              = 2024
need_sql          = true
need_vdb          = true
      </pre>
    </td>
    <td valign="top">
      <pre>
SQLite + Chroma
동시 조회
      </pre>
    </td>
  </tr>
  <tr>
    <td valign="top">
      <pre>
이 회사는 잘될 회사야?
      </pre>
    </td>
    <td valign="top">
      <pre>
intent               = metric_lookup
clarification_needed = true
clarification_reason = metric_required
      </pre>
    </td>
    <td valign="top">
      <pre>
즉시 답하지 않고
재질문 유도
      </pre>
    </td>
  </tr>
</table>

### 왜 이렇게 설계했는가
감사보고서 질의는 일반 검색보다 더 까다롭다.

- 숫자 질문과 설명 질문이 섞여 있다.
- 같은 “매출”이라는 단어도 회사 전체 매출과 특수관계자 거래 매출이 다르다.
- 연도, 단위, 표 종류, 주석 맥락이 중요하다.

그래서 질문을 먼저 구조화해서,
무엇을 어디서 찾을지 결정한 뒤 답하도록 설계했다.

## 이 구조는 RAG인가, 하드코딩인가
현재 구조는 **하드코딩된 답변 시스템이 아니라, 정책과 스키마로 안정화한 하이브리드 RAG**에 가깝다.

### 단순 하드코딩과 다른 점
하드코딩된 답변 시스템이라면:

- 질문별 답을 미리 정해둔다.
- 특정 단어가 나오면 고정 문자열을 반환한다.
- 예외 케이스를 계속 늘려간다.

현재 구조는 그렇지 않다.

- 답을 미리 정해두지 않는다.
- 먼저 질문을 구조화한다.
- 그 결과로 SQLite와 Chroma를 검색한다.
- 마지막 답변은 항상 검색된 근거를 바탕으로 만든다.

즉 “답 자체”를 하드코딩한 것이 아니라,
**검색 경로와 판단 기준을 명시적으로 설계한 것**이다.

### 왜 이런 정책이 필요한가
감사보고서 도메인에서는 단순 임베딩 검색만으로는 아래 문제가 생긴다.

- 회사 전체 매출과 특수관계자 거래 매출을 섞어버림
- 감사의견과 회계정책 설명을 혼동함
- 추상 질문에 대해 근거 없이 단정함

그래서 현재 시스템은 아래를 정책으로 고정한다.

- 질문 유형은 소수의 intent로 제한
- SQL은 자유 생성이 아니라 템플릿 기반
- Chroma는 metadata filter로 범위를 축소
- 추상 질문은 clarification으로 되돌림

### 한 줄 정리
이 시스템은 `LLM이 다 알아서 하는 구조`가 아니다.
대신,

- 파싱 단계에서 회계 의미를 구조화하고
- 질의 단계에서 질문을 구조화하고
- 검색 단계에서 SQL/VDB를 역할 분리한 뒤
- 최종 LLM은 근거를 설명만 하도록 제한한다.

즉 **회계 전문성을 모델에 전부 기대지 않고, 시스템 설계에 분산시킨 구조**라고 이해하면 된다.

### 남은 보완점
- `note_section`의 `topic_hint`는 아직 대부분 일반적이다.
- 일부 주석 문단은 본문 손실 가능성이 있다.
  - 예: `31. 보고기간 후 사건`은 현재 chunk가 제목만 남는 경우가 있다.
- 손익계산서/포괄손익계산서처럼 본표 내부 종류 구분을 더 적극적으로 활용할 여지가 있다.

## 테스트 기준
앞으로 검토는 아래 기준으로 본다.

1. 루트 `data/`의 `2014~2024` HTML 전체를 기준으로 한다.
2. `prototype/support/tests/regression_suite.py`로 전연도 회귀를 확인한다.
3. 새 `.runtime` 기준으로 아래를 같이 본다.
   - manifest runtime report
   - SQLite 적재값
   - Chroma 문서 수
   - 실제 HTM과 대표 샘플 대조

## 권장 실행

### Offline Ingest
```cmd
python src\ohsd_kimsb\prototype\support\cli\offline_ingest.py ^
  --db-path .runtime\audit_qa\sqlite\audit_reports.sqlite3 ^
  --chroma-dir .runtime\audit_qa\chroma\audit_chunks ^
  --manifest-path .runtime\audit_qa\manifests\offline_ingest.json ^
  --embedding-model qwen3-embedding:8b ^
  --ollama-base-url http://127.0.0.1:11434 ^
  --reset-db ^
  --reset-chroma ^
  --strict-runtime
```

### Online Service
```cmd
python src\ohsd_kimsb\prototype\support\cli\service_cli.py ^
  --manifest-path .runtime\audit_qa\manifests\offline_ingest.json ^
  --intent-model qwen3:8b ^
  --answer-model qwen3:8b ^
  --embedding-model qwen3-embedding:8b ^
  --strict-runtime
```
