# 감사보고서 Structure-First Prototype

이 프로젝트는 삼성전자 감사보고서 HTML(`2014~2024`)을 `structure-first` 방식으로 파싱하고,
그 결과를 하이브리드 QA 파이프라인의 입력 데이터로 정규화하는 것을 목표로 한다.

핵심 원칙은 다음과 같다.

1. HTML을 먼저 markdown으로 바꾸지 않는다.
2. DOM/블록 구조를 보존한 intermediate representation을 먼저 만든다.
3. 표는 RDB 적재용 `row/value` 구조로 정규화한다.
4. 본문 텍스트는 섹션 단위로 정리해 VDB 적재용 chunk로 만든다.
5. markdown은 최종 저장 포맷이 아니라 검수용 파생 산출물로만 다룬다.

## 현재 구조

- `prototype/core/`
  - HTML 감사보고서를 structure-first로 파싱한다.
  - block, section, normalized table, text chunk를 만든다.
  - RDB 적재용 payload와 provenance metadata를 생성한다.
- `prototype/query/`
  - 사용자 질문을 `QueryInterpretation`으로 구조화한다.
  - 지원 intent를 제한된 범위 안에서 분류한다.
  - 자유 SQL이 아니라 템플릿 기반 SQL plan을 만든다.
- `prototype/retrieval/`
  - text chunk를 Chroma metadata 문서 형태로 변환한다.
  - 질의 해석 결과와 SQL 결과를 이용해 metadata filter를 구성한다.
  - SQL 결과와 VDB 결과를 fusion한다.
- `prototype/llm/`
  - LangChain 기반 로컬 LLM 어댑터를 제공한다.
  - 질의 해석 LLM과 최종 응답 생성 LLM을 분리해서 붙일 수 있다.
- `prototype/support/answering/`
  - retrieval 결과를 근거로 최종 답변과 citation을 조합한다.
- `prototype/service/`
  - `query -> SQL -> retrieval -> answer generation` 전체 흐름을 묶는다.
- `prototype/support/examples/`
  - end-to-end 실행 예제를 제공한다.
- `prototype/support/tests/`
  - `2014~2024` 전체 감사보고서 `.htm`에 대한 단계별 회귀 테스트를 제공한다.

## Intermediate Representation 예시

### Block

```json
{
  "block_id": "samsung_audit_2024_b0011",
  "block_type": "table",
  "dom_path": "/html/body/table[12]",
  "section_id": "sec_003",
  "section_type": "attached_financial_statements",
  "text": "과목 주석 제56(당)기 ..."
}
```

### Normalized Table

```json
{
  "table_id": "samsung_audit_2024_t0012",
  "table_role": "financial_table",
  "table_subrole": "primary_statement",
  "statement_type": "statement_of_financial_position",
  "unit": "백만원",
  "year_candidates": [2023, 2024],
  "cells": [
    {
      "cell_id": "c_0_0",
      "row_index": 0,
      "col_index": 0,
      "rowspan": 1,
      "colspan": 1,
      "is_header": true
    }
  ],
  "rows": [
    {
      "row_id": "r_0001",
      "normalized_label": "유동자산",
      "row_depth": 1,
      "parent_row_id": null
    }
  ],
  "values": [
    {
      "value_id": "v_0001",
      "row_id": "r_0001",
      "column_key": "제56(당)기",
      "period": "당기",
      "value_raw": "82,320,322",
      "value_numeric": 82320322.0
    }
  ]
}
```

### Text Chunk

```json
{
  "chunk_id": "samsung_audit_2024_ck00015",
  "section_type": "audit_opinion",
  "section_title": "감사의견",
  "topic_hint": "audit_opinion",
  "near_table_id": "samsung_audit_2024_t0012",
  "page_start": 2,
  "page_end": 2
}
```

### Query Interpretation

```json
{
  "intent": "metric_lookup",
  "metric_candidates": ["revenue"],
  "year": 2023,
  "year_range": null,
  "section_candidates": [],
  "need_sql": true,
  "need_vdb": false,
  "comparison_mode": null
}
```

## 현재 지원 범위

- 지원 intent
  - `metric_lookup`
  - `text_explanation`
  - `metric_with_explanation`
  - `trend_compare`
- SQL 생성 방식
  - LLM 자유 생성 금지
  - 템플릿 기반 SQL plan만 허용
- VDB 검색 방식
  - chunk metadata filter + semantic retrieval
- answer generation 방식
  - LangChain local LLM 우선
  - 패키지 미설치 또는 호출 실패 시 fallback formatter 사용

## 실행 및 검증

회귀 테스트:

```bash
python src/승담_수비/prototype/support/tests/regression_suite.py
```

로컬 LLM/Chroma 의존성 설치:

```bash
pip install -r src/승담_수비/prototype/requirements-local-llm.txt
```

기본 provider는 `ollama`를 가정한다.

## 예제 실행

```bash
python src/승담_수비/prototype/support/examples/demo_hybrid_query.py "2024년 매출액과 관련 설명을 알려줘" --year 2024
python src/승담_수비/prototype/support/examples/demo_final_answer.py "2024년 감사의견이 뭐야?" --year 2024
python src/승담_수비/prototype/support/examples/demo_final_answer.py "2024년 매출액과 관련 설명을 알려줘" --year 2024 --intent-model qwen2.5:7b --answer-model llama3.1:8b
```
