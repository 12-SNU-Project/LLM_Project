# 구조 우선 감사보고서 파이프라인

해당 모듈은 삼성전자 감사보고서 HTML(`2014~2024`)을 `structure-first` 방식으로 파싱.

1. HTML을 먼저 DOM 블록(`p/h/table`) 단위로 분해.
2. 표를 RDB 적재용 `row/value` 구조로 정규화.
3. 본문을 섹션 라벨 기반으로 VDB 청크로 생성.
4. Markdown은 최종 저장본이 아니라 검수용 파생 산출물로 생성.


## 파일 구성

- `structure_first_models.py`: 중간 객체 정의(`Block`, `NormalizedTable`, `TextChunk` 등)
- `structure_first_utils.py`: 인코딩/텍스트 정규화/숫자 파싱 유틸
- `structure_first_parser.py`: bs4 기반 메인 파서, 표 정규화, 메타데이터 생성, SQLite 저장
- `run_structure_first_pipeline.py`: 전체 파이프라인 실행 CLI
- `analyze_html_structure.py`: 연도별 HTML 구조 차이 분석 스크립트
- `schema.sql`: RDB 스키마 초안

## 실행 방법

프로젝트 루트에서 실행:

```bash
python -m src.승담_수비.parsing.run_structure_first_pipeline --input-dir "" --output-dir ""
```

연도별 구조 분석:

```bash
python -m src.승담_수비.parsing.analyze_html_structure --input-dir "" --output ""
```

## Intermediate Representation 예시

### Block 객체

```json
{
  "block_id": "samsung_audit_2024_b0011",
  "block_type": "table",
  "dom_path": "/html/body/table[12]",
  "section_id": "sec_003",
  "section_type": "financial_statements",
  "text": "과 목 주석 제 56 (당) 기 ..."
}
```

### Normalized Table 객체

```json
{
  "table_id": "samsung_audit_2024_t0012",
  "table_role": "financial_table",
  "statement_type": "statement_of_financial_position",
  "unit": "백만원",
  "year_candidates": [2023, 2024],
  "cells": [
    {"cell_id": "...", "row_index": 0, "col_index": 0, "rowspan": 1, "colspan": 1, "is_header": true}
  ],
  "rows": [
    {"row_id": "...", "normalized_label": "유동자산", "row_depth": 1, "parent_row_id": null}
  ],
  "values": [
    {"value_id": "...", "row_id": "...", "period": "2024", "value_raw": "82,320,322", "value_numeric": 82320322.0}
  ]
}
```

### Text Chunk 객체

```json
{
  "chunk_id": "samsung_audit_2024_ck00015",
  "section_type": "independent_auditor_report",
  "topic_hint": "audit_opinion",
  "near_table_id": "samsung_audit_2024_t0012"
}
```
