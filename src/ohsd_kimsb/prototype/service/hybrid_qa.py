from __future__ import annotations

import sqlite3
from typing import Optional

try:
    from ..query.interpreter import QueryInterpreter
    from ..query.sql_templates import SQLTemplateEngine
    from ..retrieval.fusion import RetrievalFusionEngine, VectorStoreProtocol
    from ..retrieval.organizer import EvidenceOrganizer
    from ..retrieval.schema import EvidenceBundle
    from ..support.answering.composer import LangChainAnswerComposer
    from ..support.answering.schema import GeneratedAnswer
except ImportError:
    from query.interpreter import QueryInterpreter
    from query.sql_templates import SQLTemplateEngine
    from retrieval.fusion import RetrievalFusionEngine, VectorStoreProtocol
    from retrieval.organizer import EvidenceOrganizer
    from retrieval.schema import EvidenceBundle
    from support.answering.composer import LangChainAnswerComposer
    from support.answering.schema import GeneratedAnswer


class AuditQAService:
    def __init__(
        self,
        sql_engine: SQLTemplateEngine,
        retrieval_engine: RetrievalFusionEngine,
        answer_composer: LangChainAnswerComposer,
        vector_store: VectorStoreProtocol,
        query_interpreter: Optional[QueryInterpreter] = None,
        evidence_organizer: Optional[EvidenceOrganizer] = None,
    ) -> None:
        self.sql_engine = sql_engine
        self.retrieval_engine = retrieval_engine
        self.answer_composer = answer_composer
        self.vector_store = vector_store
        self.query_interpreter = query_interpreter or QueryInterpreter()
        self.evidence_organizer = evidence_organizer or EvidenceOrganizer()

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

        sql_plan = self.sql_engine.build(interpretation)
        sql_rows = self.sql_engine.execute(conn, sql_plan)
        bundle = self.retrieval_engine.retrieve(
            question=question,
            interpretation=interpretation,
            vector_store=self.vector_store,
            sql_rows=sql_rows,
        )
        bundle = self.evidence_organizer.organize(bundle)
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
