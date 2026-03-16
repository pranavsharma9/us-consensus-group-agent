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
_GUARDRAIL_MESSAGE = """
Only answer questions that can be solved using this US Census / ACS / Redistricting dataset and its metadata tables.

IN SCOPE
- Demographics: total population, sex, age, race, ethnicity, Hispanic/Latino origin, citizenship / voting-age measures if present
- Economics: household income, per-capita income, earnings by sex, poverty, SNAP / public assistance, income distributions
- Employment: employed, unemployed, labor force, unemployment rate, occupation, industry, commute / transportation to work
- Education: school enrollment, educational attainment, bachelor's degree and bachelor's field
- Housing: housing units, occupancy, tenure, rent, home value, rooms / bedrooms, vacancy, owner costs, gross rent as % of income
- Technology / access: internet subscriptions, broadband, computer / device availability
- Other census topics: language spoken at home, English proficiency, veteran status, health insurance, marital status, household type
- Geography / metadata: FIPS codes, state/county/block-group hierarchy, block-group identifiers and counts, latitude/longitude, land/water area, county/state listings, geographic lookups
- Dataset inspection: schema, table families, field descriptions, data availability, which table/column should answer a query
- Redistricting: 2020 redistricting counts and metadata; decennial exact counts when explicitly requested and present

YEAR RULES
- Supported years are only 2019 and 2020.
- Default to 2019 when the user does not specify a year.
- Use 2020 only when the user explicitly requests 2020 or asks for the latest available year.
- If the user asks for any other year, explain that only 2019 and 2020 are available and offer one of those years instead.

GEOGRAPHY RULES
- This dataset is primarily at census block-group level and can be aggregated to county, state, and national levels.
- County, state, and block-group queries are in scope.
- FIPS / metadata lookup questions are in scope.
- If the user asks about a city and the dataset does not directly support city filtering, do NOT mark it out of scope automatically.
- Prefer one of these actions:
  1. Use a direct city-to-geography mapping only if supported by dataset metadata, or
  2. clearly explain that only county/state/block-group geography is directly supported and offer the containing county as an approximation.
- Never silently substitute a county for a city. State the approximation clearly.

DERIVED / APPROXIMATE METRICS
- Derived metrics are in scope if the underlying data exists, including rates, percentages, gaps, and ratios.
- If the requested metric is not directly stored as a single column but can be answered with a close dataset-backed alternative, explain the limitation and offer the closest supported metric.
- If a result is computed by averaging block-group medians or similar geography-level summaries, clearly label it as an approximation.

BEFORE DECLARING OUT OF SCOPE
1. Check whether the query can be answered from ACS, Census, Redistricting, or metadata tables.
2. Check the relevant table family / field descriptions first.
3. If the query is geography-related, check FIPS and geographic metadata first.
4. If the exact metric is unavailable but a close alternative exists, offer that alternative.
5. Only mark the query out of scope after confirming the dataset and metadata do not support it.

OUT OF SCOPE
- General world knowledge unrelated to this dataset
- News, current events, politics, or opinion/persuasion
- Medical, legal, or financial advice
- Coding help unrelated to this dataset
- Creative writing, jokes, entertainment, or roleplay
- NSFW or unsafe content
- Questions requiring external data not present in this dataset
- Pure prediction / forecasting
- Causal explanations that cannot be supported by the dataset

RESPONSE RULES
- Answer only from this dataset and its metadata tables.
- Do not use outside knowledge to fill data gaps.
- If the query is supported, proceed with metadata lookup and SQL.
- If the year is unsupported, explain the year limitation and offer 2019 or 2020.
- If the geography is approximate, say so explicitly.
- If the metric is only approximately computed, say so explicitly.
- If the query is truly unsupported, say that it cannot be answered from this dataset and briefly mention the types of census questions that are supported.

IMPORTANT REMINDERS
- Internet / broadband questions are in scope.
- Geography / FIPS / county-list / state-list questions are in scope.
- Derived metrics are in scope when the underlying data exists.
- Do not mark a query out of scope until metadata checks have been attempted.
- When unsure, prefer attempting a metadata-backed resolution over refusing.
""".strip()

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

        initial_messages.append(SystemMessage(content=_GUARDRAIL_MESSAGE))
        initial_messages.append(HumanMessage(content=user_query))

        try:
            result = self._graph.invoke(
                {"messages": initial_messages},
                config={"recursion_limit": self._settings.max_agent_steps},
            )
        except Exception as exc:
            logger.exception("Graph invocation failed: %s", exc)
            agent_context.persist(session_id=session_id)
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
        agent_context.persist(session_id=session_id)
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
