from __future__ import annotations

import json
from typing import Optional

try:
    from ..llm.langchain_local import LangChainLocalLLM
except ImportError:
    from llm.langchain_local import LangChainLocalLLM

from .catalog import METRIC_DEFINITIONS, SECTION_GROUPS
from .interpreter import QueryInterpreter
from .schema import QueryInterpretation


class LangChainQueryInterpreter(QueryInterpreter):
    def __init__(self, llm: LangChainLocalLLM) -> None:
        super().__init__()
        self.llm = llm

    def interpret(self, question: str, llm_output: Optional[str | dict] = None) -> QueryInterpretation:
        if llm_output is not None:
            return super().interpret(question, llm_output=llm_output)

        system_prompt = self.build_llm_instruction()
        user_prompt = (
            "질문을 해석하라.\n"
            f"- 지원 metric 후보: {json.dumps(sorted(METRIC_DEFINITIONS.keys()), ensure_ascii=False)}\n"
            f"- 지원 section 후보: {json.dumps(sorted({item for values in SECTION_GROUPS.values() for item in values}), ensure_ascii=False)}\n"
            f"- 사용자 질문: {question}\n"
            "JSON만 출력하라."
        )
        try:
            payload = self.llm.invoke_json(system_prompt=system_prompt, user_prompt=user_prompt)
            return super().interpret(question, llm_output=payload)
        except Exception as exc:
            fallback = self._interpret_with_rules(question)
            fallback.notes.append(f"langchain_llm_fallback={type(exc).__name__}")
            return self.routing_policy.apply(fallback)
