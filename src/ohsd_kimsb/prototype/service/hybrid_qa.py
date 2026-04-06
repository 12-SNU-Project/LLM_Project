from __future__ import annotations

import sqlite3
from dataclasses import replace
from typing import Optional

try:
    from ..query.interpreter import QueryInterpreter
    from ..query.sql_templates import SQLTemplateEngine
    from ..retrieval.evidence_planner import EvidenceExpansionPlanner
    from ..retrieval.fusion import RetrievalFusionEngine, VectorStoreProtocol
    from ..retrieval.organizer import EvidenceOrganizer
    from ..retrieval.schema import EvidenceBundle
    from ..support.answering.composer import LangChainAnswerComposer
    from ..support.answering.schema import GeneratedAnswer
    from ..tools.ratio_tool import RatioAnalysisTool
except ImportError:
    from query.interpreter import QueryInterpreter
    from query.sql_templates import SQLTemplateEngine
    from retrieval.evidence_planner import EvidenceExpansionPlanner
    from retrieval.fusion import RetrievalFusionEngine, VectorStoreProtocol
    from retrieval.organizer import EvidenceOrganizer
    from retrieval.schema import EvidenceBundle
    from support.answering.composer import LangChainAnswerComposer
    from support.answering.schema import GeneratedAnswer
    from tools.ratio_tool import RatioAnalysisTool


class AuditQAService:
    def __init__(
        self,
        sql_engine: SQLTemplateEngine,
        retrieval_engine: RetrievalFusionEngine,
        answer_composer: LangChainAnswerComposer,
        vector_store: VectorStoreProtocol,
        query_interpreter: Optional[QueryInterpreter] = None,
        evidence_organizer: Optional[EvidenceOrganizer] = None,
        evidence_planner: Optional[EvidenceExpansionPlanner] = None,
        ratio_tool: Optional[RatioAnalysisTool] = None,
    ) -> None:
        self.sql_engine = sql_engine
        self.retrieval_engine = retrieval_engine
        self.answer_composer = answer_composer
        self.vector_store = vector_store
        self.query_interpreter = query_interpreter or QueryInterpreter()
        self.evidence_organizer = evidence_organizer or EvidenceOrganizer()
        self.evidence_planner = evidence_planner or EvidenceExpansionPlanner()
        self.ratio_tool = ratio_tool or RatioAnalysisTool()

    def answer(self, question: str, conn: sqlite3.Connection):
        interpretation = self.query_interpreter.interpret(question)
        if interpretation.clarification_needed:
            bundle = EvidenceBundle(
                interpretation=interpretation,
                applied_vector_filter={},
                sql_results=[],
                vector_hits=[],
                citations=[],
                retrieval_summary={"clarification_needed": True, "stage": "query_policy"},
            )
            answer = self._build_clarification_answer(interpretation)
            return {
                "interpretation": interpretation.to_dict(),
                "sql_plan": None,
                "bundle": bundle.to_dict(),
                "answer": answer.to_dict(),
            }

        if self.ratio_tool.supports(interpretation):
            sql_plan = self.ratio_tool.build_plan(interpretation)
            sql_rows = self.ratio_tool.execute(conn, interpretation)
        else:
            sql_plan = self.sql_engine.build(interpretation)
            sql_rows = self.sql_engine.execute(conn, sql_plan)
        bundle = self.retrieval_engine.retrieve(
            question=question,
            interpretation=interpretation,
            vector_store=self.vector_store,
            sql_rows=sql_rows,
        )
        bundle = self.evidence_organizer.organize(bundle)
        bundle = self._augment_table_contexts(conn, bundle)
        answer = self.answer_composer.compose(bundle)
        return {
            "interpretation": interpretation.to_dict(),
            "sql_plan": sql_plan.to_dict() if sql_plan else None,
            "bundle": bundle.to_dict(),
            "answer": answer.to_dict(),
        }

    @staticmethod
    def _build_clarification_answer(interpretation) -> GeneratedAnswer:
        examples = [
            "2024년 매출액이 얼마야?",
            "2024년 감사의견이 뭐야?",
            "최근 3년 매출 추이를 보여줘",
        ]
        message = (
            "질문 범위가 아직 너무 넓습니다. "
            "지원하는 질문 형태는 수치 조회, 설명 조회, 수치+설명, 추이 비교입니다. "
            "예: " + " / ".join(examples)
        )
        return GeneratedAnswer(
            answer_text=message,
            citations=[],
            used_sql_rows=0,
            used_text_chunks=0,
            metadata={
                "intent": interpretation.intent.value,
                "clarification_needed": True,
                "clarification_reason": interpretation.clarification_reason,
            },
        )

    def _augment_table_contexts(self, conn: sqlite3.Connection, bundle: EvidenceBundle) -> EvidenceBundle:
        assessment = self.evidence_planner.assess(bundle)
        summary = dict(bundle.retrieval_summary)
        summary.update(
            {
                "table_context_before": len(bundle.table_contexts),
                "table_context_after": len(bundle.table_contexts),
                "table_context_plan": assessment.table_expansion_dimensions,
                "table_context_candidate_ids": assessment.candidate_table_ids,
                "evidence_requirements": assessment.required_dimensions,
                "evidence_gaps_before": assessment.missing_dimensions,
            }
        )
        staged_bundle = replace(
            bundle,
            evidence_requirements=assessment.required_dimensions,
            evidence_gaps=assessment.missing_dimensions,
            retrieval_summary=summary,
        )
        if not self.evidence_planner.requires_table_contexts(assessment):
            summary["evidence_gaps_after"] = assessment.missing_dimensions
            return replace(staged_bundle, retrieval_summary=summary)

        table_contexts = self.sql_engine.fetch_table_contexts(
            conn=conn,
            table_ids=assessment.candidate_table_ids,
            filing_id=assessment.filing_id,
            semantic_types=assessment.semantic_table_types,
            table_title_terms=assessment.table_title_terms,
            sql_rows=bundle.sql_results,
            interpretation=bundle.interpretation,
            required_dimensions=assessment.table_expansion_dimensions,
        )
        if not table_contexts:
            summary.update(
                {
                    "table_context_after": len(bundle.table_contexts),
                    "table_context_augmented": False,
                    "evidence_gaps_after": assessment.missing_dimensions,
                }
            )
            return replace(staged_bundle, retrieval_summary=summary)

        merged_contexts = list(bundle.table_contexts)
        seen_table_ids = {str(context.get("table_id") or "") for context in merged_contexts}
        for context in table_contexts:
            table_id = str(context.get("table_id") or "")
            if table_id and table_id in seen_table_ids:
                continue
            if table_id:
                seen_table_ids.add(table_id)
            merged_contexts.append(context)

        citations = list(bundle.citations)
        new_citations = []
        seen_citations = {
            (citation.get("kind"), citation.get("table_id"))
            for citation in citations
        }
        for context in merged_contexts:
            citation_key = ("table_context", context.get("table_id"))
            if citation_key in seen_citations:
                continue
            new_citations.append(
                {
                    "kind": "table_context",
                    "table_id": context.get("table_id"),
                    "filing_id": context.get("filing_id"),
                    "semantic_table_type": context.get("semantic_table_type"),
                    "table_title": context.get("table_title"),
                }
            )
            seen_citations.add(citation_key)
        citations = new_citations + citations

        merged_bundle = replace(
            staged_bundle,
            table_contexts=merged_contexts,
            citations=citations,
        )
        final_assessment = self.evidence_planner.assess(merged_bundle)
        summary.update(
            {
                "table_context_after": len(merged_contexts),
                "table_context_augmented": True,
                "evidence_gaps_after": final_assessment.missing_dimensions,
            }
        )
        return replace(
            merged_bundle,
            table_contexts=merged_contexts,
            evidence_requirements=final_assessment.required_dimensions,
            evidence_gaps=final_assessment.missing_dimensions,
            citations=citations,
            retrieval_summary=summary,
        )
