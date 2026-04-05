from __future__ import annotations

import json
from typing import Any, Dict, List

try:
    from ...llm.langchain_local import LangChainLocalLLM
    from ...retrieval.schema import EvidenceBundle
except ImportError:
    from llm.langchain_local import LangChainLocalLLM
    from retrieval.schema import EvidenceBundle

from .schema import GeneratedAnswer


class LangChainAnswerComposer:
    def __init__(self, llm: LangChainLocalLLM, max_sql_rows: int = 8, max_chunks: int = 6) -> None:
        self.llm = llm
        self.max_sql_rows = max_sql_rows
        self.max_chunks = max_chunks

    def compose(self, bundle: EvidenceBundle) -> GeneratedAnswer:
        sql_rows = bundle.sql_results[: self.max_sql_rows]
        text_hits = bundle.vector_hits[: self.max_chunks]
        system_prompt = (
            "당신은 감사보고서 QA 시스템의 최종 응답 생성기다. "
            "반드시 제공된 evidence만 사용하여 답하고, 마지막에 출처를 간단히 명시하라. "
            "근거가 부족하면 부족하다고 분명히 말하라."
        )
        user_prompt = self._build_user_prompt(bundle, sql_rows, text_hits)
        try:
            answer_text = self.llm.invoke_text(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception as exc:
            answer_text = self._compose_fallback(bundle, sql_rows, text_hits, error=exc)

        return GeneratedAnswer(
            answer_text=answer_text.strip(),
            citations=bundle.citations,
            used_sql_rows=len(sql_rows),
            used_text_chunks=len(text_hits),
            metadata={"intent": bundle.interpretation.intent.value},
        )

    def _build_user_prompt(self, bundle: EvidenceBundle, sql_rows: List[Dict[str, Any]], text_hits: List[Any]) -> str:
        sql_payload = [
            {
                "fiscal_year": row.get("fiscal_year"),
                "table_id": row.get("table_id"),
                "table_title": row.get("table_title"),
                "semantic_table_type": row.get("semantic_table_type"),
                "normalized_label": row.get("normalized_label"),
                "raw_label": row.get("raw_label"),
                "column_key": row.get("column_key"),
                "period": row.get("period"),
                "value_numeric": row.get("value_numeric"),
                "value_raw": row.get("value_raw"),
                "unit": row.get("unit"),
            }
            for row in sql_rows
        ]
        text_payload = [
            {
                "chunk_id": hit.document_id,
                "section_type": hit.metadata.get("section_type"),
                "section_title": hit.metadata.get("section_title"),
                "fiscal_year": hit.metadata.get("fiscal_year"),
                "page_start": hit.metadata.get("page_start"),
                "page_end": hit.metadata.get("page_end"),
                "text": hit.text,
            }
            for hit in text_hits
        ]
        return (
            f"질문: {bundle.interpretation.raw_question}\n"
            f"질의 해석: {json.dumps(bundle.interpretation.to_dict(), ensure_ascii=False)}\n"
            f"SQL evidence: {json.dumps(sql_payload, ensure_ascii=False)}\n"
            f"Text evidence: {json.dumps(text_payload, ensure_ascii=False)}\n"
            "응답 형식:\n"
            "1. 직답 응답\n"
            "2. 근거 요약\n"
            "3. 출처"
        )

    @staticmethod
    def _compose_fallback(bundle: EvidenceBundle, sql_rows: List[Dict[str, Any]], text_hits: List[Any], error: Exception) -> str:
        lines: List[str] = []
        if sql_rows:
            if bundle.interpretation.intent.value == "comparison_list_lookup":
                items: List[str] = []
                for row in sql_rows[:8]:
                    name = row.get("raw_label") or row.get("normalized_label") or "항목"
                    value = row.get("value_raw") or row.get("value_numeric")
                    unit = row.get("unit") or ""
                    items.append(f"{name} ({value}{(' ' + unit) if unit else ''})")
                if items:
                    lines.append(f"직답 응답: 조건에 맞는 항목은 {', '.join(items)} 입니다.")
                else:
                    lines.append("직답 응답: 조건에 맞는 항목을 찾지 못했습니다.")
            elif any(row.get("semantic_table_type") == "derived_ratio_metric" for row in sql_rows):
                yearly: Dict[Any, List[str]] = {}
                for row in sql_rows[:20]:
                    year = row.get("fiscal_year")
                    label = row.get("raw_label") or row.get("normalized_label") or "항목"
                    value = row.get("value_raw") or row.get("value_numeric")
                    yearly.setdefault(year, []).append(f"{label} {value}")
                if bundle.interpretation.intent.value == "trend_compare":
                    lines.append("직답 응답:")
                    for year in sorted(yearly):
                        lines.append(f"- {year}년: {', '.join(yearly[year])}")
                else:
                    first_year = next(iter(sorted(yearly)), None)
                    if first_year is not None:
                        lines.append(f"직답 응답: {first_year}년 기준 {', '.join(yearly[first_year])} 입니다.")
            elif "table_structure_lookup" in bundle.interpretation.notes:
                columns: List[str] = []
                for row in sql_rows:
                    key = row.get("column_key")
                    if not key or key in {"합계", "합_계", "계"} or key in columns:
                        continue
                    columns.append(key)
                title = sql_rows[0].get("table_title") or "관련 표"
                if columns:
                    lines.append(f"직답 응답: {title} 기준으로 보면 {', '.join(columns)} 항목이 포함됩니다.")
                else:
                    lines.append("직답 응답: 관련 표는 찾았지만 구성 항목을 정리하기엔 근거가 부족합니다.")
            elif bundle.interpretation.intent.value == "table_cell_lookup" and len(sql_rows) > 1:
                items: List[str] = []
                for row in sql_rows[:8]:
                    label = row.get("raw_label") or row.get("normalized_label") or "항목"
                    period = row.get("period")
                    value = row.get("value_raw") or row.get("value_numeric")
                    unit = row.get("unit") or ""
                    prefix = label if not period or period in label else f"{label}({period})"
                    items.append(f"{prefix}: {value}{(' ' + unit) if unit else ''}")
                lines.append(f"직답 응답: {', '.join(items)} 입니다.")
            else:
                top = sql_rows[0]
                lines.append(
                    f"직답 응답: {top.get('fiscal_year')}년 {top.get('normalized_label') or top.get('raw_label')} 값은 "
                    f"{top.get('value_raw')} {top.get('unit') or ''} 입니다."
                )
        elif text_hits:
            lines.append(f"직답 응답: {text_hits[0].text[:280]}")
        else:
            lines.append("직답 응답: 검색된 근거가 부족합니다.")

        if text_hits:
            lines.append("근거 요약:")
            for hit in text_hits[:3]:
                lines.append(
                    f"- [{hit.document_id}] {hit.metadata.get('section_title')}: "
                    f"{hit.text[:140]}"
                )
        if bundle.citations:
            lines.append("출처:")
            for citation in bundle.citations[:6]:
                if citation["kind"] == "text_chunk":
                    lines.append(
                        f"- TEXT {citation.get('chunk_id')} p.{citation.get('page_start')}-{citation.get('page_end')}"
                    )
                else:
                    lines.append(
                        f"- SQL {citation.get('table_id')} {citation.get('column_key')}"
                    )
        lines.append(f"(fallback reason: {type(error).__name__})")
        return "\n".join(lines)
