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
    def __init__(
        self,
        llm: LangChainLocalLLM,
        max_sql_rows: int = 8,
        max_chunks: int = 6,
        max_table_contexts: int = 3,
    ) -> None:
        self.llm = llm
        self.max_sql_rows = max_sql_rows
        self.max_chunks = max_chunks
        self.max_table_contexts = max_table_contexts

    def compose(self, bundle: EvidenceBundle) -> GeneratedAnswer:
        sql_rows = bundle.sql_results[: self.max_sql_rows]
        text_hits = bundle.vector_hits[: self.max_chunks]
        table_contexts = bundle.table_contexts[: self.max_table_contexts]
        system_prompt = (
            "당신은 감사보고서 QA 시스템의 최종 응답 생성기다. "
            "반드시 제공된 evidence만 사용하여 답하고, 마지막에 출처를 간단히 명시하라. "
            "근거가 부족하면 부족하다고 분명히 말하라."
        )
        user_prompt = self._build_user_prompt(bundle, sql_rows, text_hits, table_contexts)
        try:
            answer_text = self.llm.invoke_text(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception as exc:
            answer_text = self._compose_fallback(bundle, sql_rows, text_hits, table_contexts, error=exc)

        return GeneratedAnswer(
            answer_text=answer_text.strip(),
            citations=bundle.citations,
            used_sql_rows=len(sql_rows),
            used_text_chunks=len(text_hits),
            used_table_contexts=len(table_contexts),
            metadata={"intent": bundle.interpretation.intent.value},
        )

    def _build_user_prompt(
        self,
        bundle: EvidenceBundle,
        sql_rows: List[Dict[str, Any]],
        text_hits: List[Any],
        table_contexts: List[Dict[str, Any]],
    ) -> str:
        sql_payload = [
            {
                "fiscal_year": row.get("fiscal_year"),
                "table_id": row.get("table_id"),
                "table_title": row.get("table_title"),
                "semantic_table_type": row.get("semantic_table_type"),
                "normalized_label": row.get("normalized_label"),
                "raw_label": row.get("raw_label"),
                "company_kind": row.get("company_kind"),
                "row_group_label": row.get("row_group_label"),
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
        table_context_payload = [
            {
                "table_id": ctx.get("table_id"),
                "table_title": ctx.get("table_title"),
                "semantic_table_type": ctx.get("semantic_table_type"),
                "table_unit": ctx.get("table_unit"),
                "focus_row_labels": ctx.get("focus_row_labels", []),
                "focus_column_keys": ctx.get("focus_column_keys", []),
                "table_markdown": self._truncate(
                    str(ctx.get("focused_table_markdown") or ctx.get("table_markdown") or ""),
                    1200,
                ),
                "footnotes": self._truncate(str(ctx.get("focused_footnotes") or ""), 400),
            }
            for ctx in table_contexts
        ]
        return (
            f"질문: {bundle.interpretation.raw_question}\n"
            f"질의 해석: {json.dumps(bundle.interpretation.to_dict(), ensure_ascii=False)}\n"
            f"Evidence requirements: {json.dumps(bundle.evidence_requirements, ensure_ascii=False)}\n"
            f"Evidence gaps: {json.dumps(bundle.evidence_gaps, ensure_ascii=False)}\n"
            f"SQL evidence: {json.dumps(sql_payload, ensure_ascii=False)}\n"
            f"Text evidence: {json.dumps(text_payload, ensure_ascii=False)}\n"
            f"Table context evidence: {json.dumps(table_context_payload, ensure_ascii=False)}\n"
            "응답 형식:\n"
            "1. 직답 응답\n"
            "2. 근거 요약\n"
            "3. 출처"
        )

    @staticmethod
    def _truncate(text: str, limit: int) -> str:
        compact = (text or "").strip()
        if len(compact) <= limit:
            return compact
        return compact[:limit].rstrip() + "..."

    @staticmethod
    def _compose_fallback(
        bundle: EvidenceBundle,
        sql_rows: List[Dict[str, Any]],
        text_hits: List[Any],
        table_contexts: List[Dict[str, Any]],
        error: Exception,
    ) -> str:
        lines: List[str] = []
        structure_requested = "structure" in bundle.evidence_requirements
        classification_requested = any(
            token in (bundle.interpretation.raw_question or "")
            for token in ("종속기업", "관계기업", "공동기업")
        )
        if sql_rows:
            if classification_requested:
                kinds = []
                for row in sql_rows:
                    kind = str(row.get("company_kind") or "").strip()
                    if kind and kind not in kinds:
                        kinds.append(kind)
                if kinds:
                    label_map = {
                        "subsidiary": "종속기업",
                        "associate": "관계기업",
                        "joint_venture": "공동기업",
                    }
                    name = sql_rows[0].get("raw_label") or sql_rows[0].get("normalized_label") or "해당 회사"
                    if len(kinds) == 1:
                        lines.append(f"직답 응답: {name}는 {label_map.get(kinds[0], kinds[0])}입니다.")
                    else:
                        labels = ", ".join(label_map.get(kind, kind) for kind in kinds)
                        lines.append(f"직답 응답: {name}는 표 맥락에 따라 {labels}로 나타납니다.")
                else:
                    lines.append("직답 응답: 회사 분류를 판단할 직접 근거가 부족합니다.")
            elif bundle.interpretation.intent.value == "comparison_list_lookup":
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
            elif structure_requested:
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
                unique_targets = {
                    (
                        row.get("raw_label") or row.get("normalized_label") or "",
                        row.get("column_key") or "",
                    )
                    for row in sql_rows
                }
                if len(unique_targets) == 1:
                    top = sql_rows[0]
                    lines.append(
                        f"직답 응답: {top.get('fiscal_year')}년 {top.get('raw_label') or top.get('normalized_label')}의 "
                        f"{top.get('column_key')} 값은 {top.get('value_raw')} {top.get('unit') or ''} 입니다."
                    )
                else:
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
        elif table_contexts:
            top = table_contexts[0]
            lines.append(
                "직답 응답: 관련 표 맥락까지 확인했지만 직접 수치 또는 서술 근거는 제한적입니다. "
                f"우선 참조할 표는 `{top.get('table_title')}`이고 단위는 `{top.get('table_unit') or '미확인'}`입니다."
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
        if table_contexts:
            lines.append("표 근거 요약:")
            for ctx in table_contexts[:3]:
                markdown = str(ctx.get("focused_table_markdown") or ctx.get("table_markdown") or "")
                first_lines = " / ".join(line.strip() for line in markdown.splitlines()[:4] if line.strip())
                footnotes = str(ctx.get("focused_footnotes") or "")
                summary = first_lines[:180] if first_lines else "표 markdown 없음"
                if footnotes:
                    summary += f" / 각주: {footnotes[:120]}"
                lines.append(
                    f"- [{ctx.get('table_id')}] {ctx.get('table_title')} ({ctx.get('table_unit') or '미확인'}): {summary}"
                )
        if bundle.citations:
            lines.append("출처:")
            for citation in bundle.citations[:6]:
                if citation["kind"] == "text_chunk":
                    lines.append(
                        f"- TEXT {citation.get('chunk_id')} p.{citation.get('page_start')}-{citation.get('page_end')}"
                    )
                elif citation["kind"] == "table_context":
                    lines.append(
                        f"- TABLE {citation.get('table_id')} {citation.get('table_title') or ''}"
                    )
                else:
                    lines.append(
                        f"- SQL {citation.get('table_id')} {citation.get('column_key')}"
                    )
        lines.append(f"(fallback reason: {type(error).__name__})")
        return "\n".join(lines)
