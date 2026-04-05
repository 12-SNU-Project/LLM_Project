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
            "당신은 감사보고서 QA 시스템의 최종 답변 생성기다. "
            "반드시 제공된 evidence만 사용해 답하고, 답변 마지막에 출처를 간단히 명시하라. "
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
                "normalized_label": row.get("normalized_label"),
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
            "답변 형식:\n"
            "1. 핵심 답변\n"
            "2. 근거 요약\n"
            "3. 출처"
        )

    @staticmethod
    def _compose_fallback(bundle: EvidenceBundle, sql_rows: List[Dict[str, Any]], text_hits: List[Any], error: Exception) -> str:
        lines: List[str] = []
        if sql_rows:
            top = sql_rows[0]
            lines.append(
                f"핵심 답변: {top.get('fiscal_year')}년 {top.get('normalized_label') or top.get('raw_label')} 값은 "
                f"{top.get('value_raw')} {top.get('unit') or ''} 입니다."
            )
        elif text_hits:
            lines.append(f"핵심 답변: {text_hits[0].text[:280]}")
        else:
            lines.append("핵심 답변: 검색된 근거가 부족합니다.")

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
