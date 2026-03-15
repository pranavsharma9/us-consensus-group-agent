import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import StructuredTool
from langgraph.graph import START, StateGraph
from langgraph.prebuilt import ToolNode
from pydantic import BaseModel

from app.core.config import Settings, get_settings
from app.graph.state import AgentState
from app.prompts.prompts import build_system_prompt
from app.services.llm_service import LLMService
from app.services.few_shot_retriever import FewShotRetriever
from app.services.snowflake_service import SnowflakeService
from app.services.agent_context import AgentContext

logger = logging.getLogger(__name__)

_LOG_FILE = "log.txt"

class RunSQLInput(BaseModel):
    sql: str

class QueryWorkflow:
    """
    ReAct agent that uses a single `run_sql` tool to:
      1. Query FIPS metadata for geography resolution
      2. Query field-description metadata for column codes
      3. Execute the final data query against ACS or redistricting tables
    """

    def __init__(
        self,
        settings: Settings | None = None,
        few_shot_retriever: FewShotRetriever | None = None,
        agent_context: AgentContext | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._snowflake_service = SnowflakeService(self._settings)
        self._llm_service = LLMService(self._settings)
        self._few_shot_retriever = few_shot_retriever
        self._agent_context = agent_context or AgentContext(self._settings)
        self._system_prompt = build_system_prompt(
            db=self._settings.snowflake_database,
            schema=self._settings.snowflake_schema,
        )

        run_sql_tool = self._build_run_sql_tool()
        tools = [run_sql_tool]

        llm = self._llm_service.get_llm(temperature=0.0)
        self._llm_with_tools = llm.bind_tools(tools)
        self._tool_node = ToolNode(tools)

        self._graph = self._build_graph()


    def _build_run_sql_tool(self) -> StructuredTool:
        svc = self._snowflake_service

        def run_sql(sql: str) -> str:
            """
            Execute a SQL query against the Snowflake database and return results as JSON.
            - Look up FIPS codes for geography resolution
            - Look up column codes from metadata tables (FIELD_DESCRIPTIONS)
            - Execute the final data query against ACS or redistricting tables

            Returns a JSON array of row dicts, or a SQL_ERROR string if it fails.
            """
            try:
                rows = svc.execute_query(sql)
                return json.dumps(rows[:200], default=str)
            except Exception as exc:
                logger.warning("run_sql error: %s | sql=%s", exc, sql)
                return f"SQL_ERROR: {exc}"

        return StructuredTool.from_function(
            func=run_sql,
            name="run_sql",
            description=run_sql.__doc__,
            args_schema=RunSQLInput,
        )

    def _build_graph(self):
        def agent_node(state: AgentState) -> AgentState:
            response = self._llm_with_tools.invoke(state["messages"])
            return {"messages": [response]}

        def should_continue(state: AgentState) -> Literal["tools", "__end__"]:
            last: BaseMessage = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                if self._count_sql_errors(state["messages"]) >= self._settings.max_attempts:
                    return "__end__"
                return "tools"
            return "__end__"

        graph = StateGraph(AgentState)
        graph.add_node("agent", agent_node)
        graph.add_node("tools", self._tool_node)
        graph.add_edge(START, "agent")
        graph.add_conditional_edges("agent", should_continue)
        graph.add_edge("tools", "agent")

        return graph.compile()

    def invoke(self, session_id: str, user_query: str, include_debug: bool = False) -> Dict[str, Any]:
        agent_context = self._agent_context
        history = agent_context.get_context(session_id)    
        dynamic_few_shots = self._build_dynamic_few_shots(user_query)
        system_prompt = self._system_prompt

        initial_messages=[]
        initial_messages.append(SystemMessage(content=system_prompt))

        if dynamic_few_shots:
            few_shots_examples = (
                "=====================================================================\n"
                "EXAMPLES\n"
                "=====================================================================\n"
                f"{dynamic_few_shots}"
            )
            initial_messages.append(SystemMessage(content=few_shots_examples))
        
        for turn in history:
            if turn["role"] == "user":
                initial_messages.append(HumanMessage(content=turn["content"]))
            elif turn["role"] == "assistant":
                initial_messages.append(AIMessage(content=turn["content"]))

        initial_messages.append(HumanMessage(content=user_query))

        try:
            result = self._graph.invoke(
                {"messages": initial_messages},
                config={"recursion_limit": self._settings.max_agent_steps},
            )
        except Exception as exc:
            logger.exception("Graph invocation failed: %s", exc)
            return {
                "status": "failed",
                "final_answer": "An internal error occurred. Please try again.",
                "error_message": str(exc),
                "sql": "",
                "rows": [],
                "attempt": 1,
            }

        messages: List[BaseMessage] = result.get("messages", [])
        sql_calls, tool_results, final_answer = self._extract_from_messages(messages)

        sql_error_count = self._count_sql_errors(messages)
        if not final_answer and sql_error_count >= self._settings.max_attempts:
            final_answer = (
                f"I could not complete your request after {self._settings.max_attempts} failed SQL attempts. "
                "Please refine the question and try again."
            )

        status = "success" if final_answer else "failed"
        output: Dict[str, Any] = {
            "status": status,
            "final_answer": final_answer,
            "error_message": (
                None
                if status == "success"
                else (
                    f"Reached max SQL attempts ({self._settings.max_attempts})."
                    if sql_error_count >= self._settings.max_attempts
                    else "No answer generated."
                )
            ),
            "sql": sql_calls,
            "rows": tool_results,
            "attempt": self._count_agent_turns(messages),
        }

        self._write_log(user_query, sql_calls, tool_results, final_answer, status)
        agent_context.add_context(session_id, "user", user_query)
        agent_context.add_context(session_id, "assistant", final_answer)
        return output

    def _build_dynamic_few_shots(self, user_query: str) -> str:
        if self._few_shot_retriever is None:
            return ""
        examples = self._few_shot_retriever.retrieve(user_query, k=self._settings.few_shot_top_k)
        if not examples:
            return ""
        lines: List[str] = []
        for i, ex in enumerate(examples, start=1):
            lines.append(f"[EXAMPLE {i}]")
            lines.append(ex)
            lines.append("")
        return "\n".join(lines).strip()

    def _extract_from_messages(
        self, messages: List[BaseMessage]
    ) -> tuple[List[str], List[str], str]:
        """Return (sql_calls, tool_results, final_answer)."""
        sql_calls: List[str] = []
        tool_results: List[str] = []
        final_answer = ""

        for msg in messages:
            if isinstance(msg, AIMessage):
                if msg.tool_calls:
                    for tc in msg.tool_calls:
                        sql = tc.get("args", {}).get("sql", "")
                        if sql:
                            sql_calls.append(sql)
                else:
                    if isinstance(msg.content, str) and msg.content.strip():
                        final_answer = msg.content.strip()
                    elif isinstance(msg.content, list):
                        parts = [
                            b["text"] for b in msg.content
                            if isinstance(b, dict) and b.get("type") == "text"
                        ]
                        final_answer = " ".join(parts).strip()
            elif msg.type == "tool":
                content = msg.content if isinstance(msg.content, str) else str(msg.content)
                tool_results.append(content)

        return sql_calls, tool_results, final_answer

    def _count_agent_turns(self, messages: List[BaseMessage]) -> int:
        return sum(1 for m in messages if isinstance(m, AIMessage))

    def _count_sql_errors(self, messages: List[BaseMessage]) -> int:
        count = 0
        for msg in messages:
            if msg.type != "tool":
                continue
            content = msg.content if isinstance(msg.content, str) else str(msg.content)
            if "SQL_ERROR:" in content:
                count += 1
        return count

    def _write_log(
        self,
        user_query: str,
        sql_calls: List[str],
        tool_results: List[str],
        final_answer: str,
        status: str,
    ) -> None:
        lines = [
            "----- QUERY EXECUTION -----",
            f"time_utc: {datetime.now(timezone.utc).isoformat()}",
            f"user_query: {user_query}",
            f"status: {status}",
            "",
            "steps:",
        ]

        for i, (sql, result) in enumerate(zip(sql_calls, tool_results), start=1):
            lines.append(f"  [{i}] SQL: {sql.strip()}")
            preview = result.replace("\n", " ")[:500]
            lines.append(f"      RESULT: {preview}")

        lines.append("")
        lines.append(f"final_answer: {final_answer}")
        lines.append("---------------------------\n")

        record = "\n".join(lines)
        try:
            with open(_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(record)
        except OSError as exc:
            logger.warning("Could not write to log file: %s", exc)
