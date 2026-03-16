# Snowflake US Census Agent

## Access The Web App

- Live URL: [https://us-consensus-group-agent-pranav.streamlit.app/](https://us-consensus-group-agent-pranav.streamlit.app/)
- The application is already deployed on the internet and can be used directly in a browser. No local setup is required for evaluation.

## What This App Does

This app helps users ask natural-language questions about US Census data and get clear answers without writing SQL manually. It is built to handle common demographic and geography questions, keep session context, and return responses in a conversational way while still using real data from Snowflake.

### Question Categories In Scope

- Population totals by geography (state/county/block-group)
- Population by sex (male/female totals)
- Household and median income insights
- Employment and unemployment metrics
- Housing-related measures (rent, tenure, internet access)
- Educational attainment and Hispanic/Latino origin questions
- Geography-support questions like latitude/longitude lookup
- Redistricting race-focused queries (where available)
- Any questions which pertain to ACS/ Redistricting Data.

### Out Of Scope

- Non-US Census or non-neighborhood-insights datasets
- Unsupported years or question patterns not covered by current metadata and few-shot examples
- Requests that are NSFW or unrelated to demographic/census analytics
- Fully free-form general chat not grounded in dataset-backed answers
- CBG_Pattern tables

## Example Queries To Test

> - "What is the total population in Texas in 2019?"
> - "What is the total female population in California in 2019?"
> - "What is the median household income in Los Angeles County, California?"
> - "What percentage of housing units are renter-occupied in California?"
> - "How many Hispanic or Latino people live in Arizona?"

## APP Login (REQUIRED)

![App Login Screen](https://github.com/pranavsharma9/us-consensus-group-agent/blob/main/app/img/login_img.png)

## Sample Query 

![Sample Query Response](https://github.com/pranavsharma9/us-consensus-group-agent/blob/main/app/img/query_img.png)

## Core Components

- Data source: pulls Census dataset from Snowflake Marketplace - [SafeGraph US Open Census Data - Neighborhood Insights (Free)](https://urldefense.com/v3/__https://app.snowflake.com/marketplace/listing/GZSNZ2UNN0/safegraph-us-open-census-data-neighborhood-insights-free-dataset__;!!DZ3fjg!_NCgyKaLKqUiUbcn65anGBFr4LfW_3KoQOT5i3dpWmnAF3mxpVTYeNl8b1wHulXYk28r0AIjieZmE9slYga_WJWp3vcNNURE0g$)
- Agent orchestration: LangGraph + LangChain
- LLM inferencing: OpenAI `gpt-4o-mini`
- SQL generation: custom Natural Language to SQL tool
- Session memory: maintains context per session
- Dynamic few shots: retrieves examples based on user query (exemplar optimization)
- Guardrails: blocks NSFW and off-topic requests
- UI: Streamlit
- Backend deployment: Render
- Frontend deployment: Streamlit Community Cloud
- Abuse protection: rate limit set to 10 messages per minute

## Technical Development Process

- **Architecture and flow design:** I implemented Streamlit frontend and a FastAPI backend.

- **Agent orchestration strategy:** I used LangGraph with LangChain to model the reasoning loop as explicit stages: plan, tool execution, and final response. 

- **Natural language to SQL implementation:** I built a custom NL-to-SQL tool that turns user questions into executable Snowflake queries. The tool is designed to reference metadata first, resolve the right columns and tables, and then run SQL against the selected Census dataset.

- **Grounding with data and metadata:** Query generation is grounded on Snowflake metadata lookups to reduce hallucinations and wrong column selection. This helps the agent map user intent to real schema elements before execution, improving accuracy on demographic and geography-focused questions.

- **Dynamic few-shot retrieval:** I added a FAISS-backed few-shot retriever that selects relevant examples from curated prompts using embedding similarity. Injecting context-specific examples at runtime improved output consistency, especially for multi-step analytical questions and table/column filtering issues.

- **Session context management:** I added per session memory so follow-up questions can reference prior turns naturally. The app uses session IDs to preserve conversational continuity between frontend interactions and backend execution. User can add a new session to start fresh with no previous context and can repeatedly ask questions in the same context window as well. Same context window has all previous sessions.

- **Safety and topic guardrails:** I introduced guardrails to reject NSFW and out-of-scope prompts before unnecessary tool execution.

- **Rate Limiting:** The backend includes structured error handling and a strict rate limit of 10 requests per minute per client using SlowAPI. These controls help protect service availability and reduce misuse of api.

- **User Auth Access:** The user needs to signin with the provided credentials first to access the main app, else they wont be able to use it. This is to prevent misuse since there are multiple LLM APIs which incour cost.

- **Deployment:** I deployed the backend on Render and the UI on Streamlit Community.

- **AI quality improvements over time:** Prompt design, tool descriptions, and few-shot examples were iterated repeatedly using observed failures from real test queries. This iterative loop improved SQL correctness, handling of ambiguous requests, and clarity of final natural-language answers.

## Future Scope (If Given More Time)

- Personalization per user using agent memory.  
  This would let the agent adapt response style and depth based on user history and preferences.  
  It can also improve follow-up quality by preserving long-term intent across multiple sessions.

- Add more tools (for example, fuzzy matching of filter values before SQL retrieval).  
  A fuzzy matcher tool can help validate filters against real table values before query execution, since user sometimes misspel and filters should not suffer. This will help improve prompt eng.   
  This reduces SQL misses caused by typos, aliases, or slight variations in user phrasing.

- Multi-user sign-in and profile creation.  
  Introducing user accounts enables role based access and separation of session history per user.  Presently only one user exists and all sessions are created for that user

- Add practical agent skills where useful for this use case.  
  Skills can help remove recurring logic like geography normalization or metric disambiguation.  
  This keeps prompts smaller and cleaner, and reduces duplication (this can add small advantage and would be completely optional even in future)

- Expand and optimize few-shot examples using ground-truth query sets.  
  If some ground truth and queries which will be primarliy used could be provided,
  few shots could be improved and prompt could also be more focused.
  This should improve SQL precision, especially for ambiguous intents and multi-condition prompts.

- Move session context storage from per-session JSON to a database.  
  Database-backed memory would be safer for production. Presently context is saved in
  a .json file per session ID and on server restart, this will get lost. If extra time could be provided, we could offload this to a DB.
  It would also allow querying historical sessions for analytics, debugging, and product insights.

- Improve logging and use Supabase for centralized logs.  
  Centralized structured logs can make incident investigation and model behavior analysis much faster.  
  A log pipeline with request IDs and agent-step traces will improve observability end to end.
