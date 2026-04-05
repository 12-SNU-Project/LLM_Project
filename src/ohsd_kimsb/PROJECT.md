# Audit Report Structure-First Prototype

이 작업 공간은 삼성전자 감사보고서 HTML(`2014~2024`)을 `structure-first` 방식으로 파싱하고,
그 결과를 하이브리드 QA 시스템의 입력 데이터로 정규화하는 것을 목표로 한다.

핵심 원칙은 다음과 같다.

1. HTML을 먼저 markdown으로 바꾸지 않는다.
2. DOM/블록 구조를 보존한 intermediate representation을 먼저 만든다.
3. 표는 RDB 적재용 `row/value` 구조로 정규화한다.
4. 본문 텍스트는 섹션 단위로 정리해 VDB 적재용 chunk로 만든다.
5. markdown은 저장 원본이 아니라 검수용 파생 산출물이다.

## 프로젝트를 두 단계로 분리한 이유

이 프로젝트는 이제 아래 두 실행 경로로 나뉜다.

### 1. Offline Ingest
- 목적: `data/*.htm`를 미리 파싱하고 영속 산출물을 만든다.
- 출력:
  - SQLite DB
  - Chroma collection
  - ingest manifest JSON
- 특징:
  - 사용자의 질문을 받지 않는다.
  - 배치성 작업이다.
  - 서비스 시작 전에 한 번 실행한다.

### 2. Online Service
- 목적: 이미 구축된 SQLite/Chroma를 읽고 질문에 답한다.
- 입력:
  - prebuilt SQLite DB
  - prebuilt Chroma collection
  - 사용자 질문
- 특징:
  - HTML을 다시 파싱하지 않는다.
  - 질의 해석, SQL/VDB retrieval, 답변 생성만 수행한다.

이 분리를 통해 `데이터 구축`과 `실시간 질의 처리`의 책임을 분리했다.

## 폴더별 역할

### `prototype/core/`
- 감사보고서 HTML 파싱의 중심 계층이다.
- `html_io.py`: 인코딩 감지와 HTML 디코딩.
- `parser.py`: block/section 분해, 문서 메타 추출.
- `table_processor.py`: 표 분류, rowspan/colspan 해체, row/value 정규화.
- `pipeline.py`: 파싱 결과를 하나의 `FilingParseResult`로 조립.
- `sqlite_loader.py`: RDB 초안 스키마 기준 적재.
- `rdb_schema_draft.sql`: SQLite 기반 스키마 초안.

### `prototype/query/`
- 사용자 질문을 구조화된 질의 해석 결과로 바꾸는 계층이다.
- `schema.py`: intent, metric, year, routing flag 스키마 정의.
- `catalog.py`: metric/section alias 사전.
- `interpreter.py`: 규칙 기반 fallback 질의 해석기.
- `langchain_interpreter.py`: LangChain + 로컬 LLM 기반 질의 해석기.
- `policy.py`: LLM 출력 이후 최종 `need_sql / need_vdb / clarification_needed`를 확정.
- `sql_templates.py`: 자유 SQL 대신 템플릿 기반 SQL plan 생성/실행.

### `prototype/retrieval/`
- VDB metadata, 검색, fusion을 담당하는 계층이다.
- `chroma_metadata.py`: text chunk를 Chroma 문서 형식으로 변환.
- `chroma_store.py`: Chroma persistent collection 래퍼.
- `fusion.py`: SQL 결과와 vector hit를 결합하고 재랭킹.
- `organizer.py`: generation 직전 evidence 정리.
- `schema.py`: vector hit / evidence bundle 구조 정의.

### `prototype/llm/`
- 로컬 LLM 및 임베딩 모델 어댑터 계층이다.
- `langchain_local.py`: LangChain/Ollama 기반 chat model 호출.
- 같은 파일 안에 `LangChainLocalEmbedding`을 두어 Chroma용 외부 임베딩 함수를 연결한다.
- 기본 모델 설정:
  - 질의 해석 LLM: `qwen3-12b`
  - 최종 답변 LLM: `qwen3-12b`
  - 임베딩 모델: `qwen3-8b`

### `prototype/service/`
- 상위 orchestration 계층이다.
- `hybrid_qa.py`: `질의 해석 -> SQL -> retrieval -> answer composition`을 묶는다.
- `runtime_factory.py`: LangChain/Ollama/Chroma 운영 경로를 우선 사용하고, 필요 시 fallback을 결정한다.
- `artifact_paths.py`: SQLite/Chroma/manifest 기본 산출물 경로를 정의한다.

### `prototype/support/answering/`
- retrieval 결과를 근거로 최종 답변과 citation을 조합한다.
- LangChain LLM을 우선 사용하고, 실패 시 formatter fallback을 유지한다.

### `prototype/support/examples/`
- 수동 검증과 self-contained demo 실행용 스크립트다.
- 이 스크립트들은 이해와 점검을 위해 여전히 HTML을 직접 읽을 수 있다.

### `prototype/support/cli/`
- 실제 운영 흐름에 맞춘 CLI 엔트리포인트다.
- `offline_ingest.py`: HTML -> SQLite/Chroma/manifest 구축
- `service_cli.py`: prebuilt SQLite/Chroma를 읽어 질문에 답변

### `prototype/support/tests/`
- 전수 회귀 테스트 계층이다.
- `regression_suite.py`는 루트 `data/`의 `2014~2024` HTML 전체를 대상으로 파싱, retrieval, citation 흐름을 검증한다.

## 호출 플로우

### A. Offline Ingest
1. `data/*.htm` 입력
2. `prototype/core/html_io.py`
3. `prototype/core/parser.py`
4. `prototype/core/table_processor.py`
5. `prototype/core/pipeline.py`
6. `filings / sections / tables / table_rows / table_values / text_chunks / blocks` payload 생성
7. `prototype/core/sqlite_loader.py`로 SQLite 적재
8. `prototype/retrieval/chroma_metadata.py`로 Chroma 문서 생성
9. `prototype/service/runtime_factory.py`로 Chroma upsert
10. `offline_ingest.py`가 manifest 저장

### B. Online Service
1. 사용자 질문 입력
2. `prototype/service/runtime_factory.py`가 prebuilt Chroma collection 연결
3. `prototype/query/langchain_interpreter.py`
4. 실패 시 `prototype/query/interpreter.py`
5. `prototype/query/policy.py`가 최종 라우팅 확정
6. `prototype/query/sql_templates.py`가 SQL plan 생성
7. SQLite DB에서 정량 evidence 조회
8. `prototype/retrieval/chroma_store.py`가 Chroma query 수행
9. `prototype/retrieval/fusion.py`와 `prototype/retrieval/organizer.py`가 evidence 정리
10. `prototype/support/answering/composer.py`가 최종 답변 + citation/page 생성

## 기본 운영 경로

현재 기본 운영 경로는 아래와 같다.

1. 질의 해석: `LangChain + Ollama + qwen3-12b`
2. 최종 답변: `LangChain + Ollama + qwen3-12b`
3. 임베딩: `LangChain OllamaEmbeddings + qwen3-8b`
4. 벡터 저장소: `Chroma persistent collection`

`prototype/service/runtime_factory.py`가 이 경로를 우선 선택한다.
단, 샌드박스나 로컬 환경에서 의존성 또는 서버가 없으면 fallback을 유지한다.

fallback 규칙은 다음과 같다.

- 질의 해석 LLM 실패 -> 규칙 기반 `QueryInterpreter`
- Chroma/임베딩 실패 -> `InMemoryVectorStore`
- 최종 answer LLM 실패 -> formatter fallback

즉 현재 코드는 `운영 경로 우선 + 안전한 fallback 유지` 전략이다.

## 로컬에서 테스트하는 순서

### 0. 의존성 설치

```bash
pip install -r src/ohsd_kimsb/prototype/requirements-local-llm.txt
```

Ollama에는 아래 모델이 준비되어 있어야 한다.

- `qwen3-12b`
- `qwen3-8b`

### 1. 전체 회귀 테스트

```bash
python src/ohsd_kimsb/prototype/support/tests/regression_suite.py
```

- 목적: 현재 환경이 최소 동작하는지 확인
- 범위: `2014~2024` HTML 전체

### 2. 임베딩/Chroma 경로만 별도 점검

```bash
python src/ohsd_kimsb/prototype/support/examples/demo_embedding_runtime.py --year 2024 --embedding-model qwen3-8b
```

- 목적: embedding backend와 Chroma 연결 가능 여부 확인

### 3. 실제 운영 구조로 배치 ingest

```bash
python src/ohsd_kimsb/prototype/support/cli/offline_ingest.py
```

- 출력:
  - `.runtime/audit_qa/sqlite/audit_reports.sqlite3`
  - `.runtime/audit_qa/chroma/audit_chunks`
  - `.runtime/audit_qa/manifests/offline_ingest.json`

### 4. 실제 운영 구조로 서비스 질의

```bash
python src/ohsd_kimsb/prototype/support/cli/service_cli.py "2024년 감사의견이 뭐야?"
```

- 이 경로에서는 HTML을 재파싱하지 않는다.
- 오직 prebuilt DB/Chroma만 사용한다.

### 5. self-contained demo

```bash
python src/ohsd_kimsb/prototype/support/examples/demo_hybrid_query.py "2024년 매출액과 관련 설명을 알려줘" --year 2024
python src/ohsd_kimsb/prototype/support/examples/demo_final_answer.py "2024년 감사의견이 뭐야?" --year 2024
```

- 목적: 이해와 디버깅
- 특징: demo는 HTML을 직접 읽는 self-contained 경로다.

## 샌드박스 검증 결과

현재 샌드박스에서는 아래 패키지가 없다.

- `langchain_core`
- `langchain_ollama`
- `chromadb`

그래서 검증 결과는 다음과 같다.

- `regression_suite.py`: 통과
- `offline_ingest.py`: 통과
- `service_cli.py`: 통과
- 실제 사용된 backend:
  - query interpreter: LangChain 경로 시도 후 fallback
  - answer composer: LangChain 경로 시도 후 fallback
  - vector store: Chroma 경로 시도 후 `InMemoryVectorStore` fallback

즉 코드 구조는 분리되었고 작동하지만, 샌드박스에서는 운영 경로가 아니라 fallback 경로가 실행됐다.

## 현재 완성도

현재 상태를 단계별로 보면 다음과 같다.

- 구조-first 파싱: 높음
- RDB/VDB 정규화: 높음
- 질의 해석/라우팅: 중상
- SQL/VDB fusion: 중상
- Offline Ingest / Online Service 분리: 중상
- LangChain/Ollama/Chroma 운영 경로 실제 검증: 중
- 최종 answer generation 안정화: 중

대략적인 전체 완성도는 `75% 전후`로 본다.

## 아직 부족한 점

1. 샌드박스에는 `langchain_core`, `langchain_ollama`, `chromadb`가 없어 실제 운영 경로를 end-to-end로 검증하지 못했다.
2. Chroma persistent collection을 장기 운영 기준으로 관리하는 별도 update/rebuild 전략은 아직 단순하다.
3. `service_cli.py`는 CLI 수준이고, 실제 서버형 엔트리포인트는 아직 없다.
4. answer prompt와 formatting은 fallback 안정성은 확보했지만 운영형 품질 최적화가 더 필요하다.
5. 로그 기반 alias 보강과 abstract question 확장은 아직 초기 단계다.

## 검증 기준

앞으로 모든 검수는 아래 기준을 따른다.

1. 루트 `data/` 폴더의 `2014~2024` HTML 전체를 대상으로 한다.
2. `prototype/support/tests/regression_suite.py`를 기준 회귀로 사용한다.
3. 파싱 결과, SQL 결과, vector hit, citation/page까지 함께 확인한다.
