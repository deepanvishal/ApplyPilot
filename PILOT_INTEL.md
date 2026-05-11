# pilot-intel — Technical Reference for Claude Code

## What is pilot-intel

A standalone analytics + intelligence layer on top of ApplyPilot data.
Read-only. No writes back to ApplyPilot. No pipeline integration.

Users ask natural language questions about their job search data.
The agent reasons over structured DB data and unstructured job descriptions
to surface insights: callback patterns, skill gaps, coverage analysis, error patterns.

Lives at: `ApplyPilot/pilot-intel/`
ApplyPilot DB: `~/.applypilot/applypilot.db` (SQLite, read-only access)

---

## Architecture

```
User Question (natural language)
        ↓
[supervisor_graph]
    ├── router node         classifies question type, sets scope
    ├── term_expander       LLM expands concepts (type 5 only)
    │
    ├── [analytics_subgraph]
    │       sql_node        SQLCoder generates + executes SQL
    │       summarizer      LLM over raw text results (type 3)
    │
    ├── [retrieval_subgraph]
    │       rag_node        Qdrant hybrid search (dense + BM25)
    │       reranker        bge-reranker-v2-m3 cross-encoder
    │
    └── [reasoning_subgraph]
            synthesizer     combines SQL + RAG results
            reflector       completeness + faithfulness check
            followup        generates next query if incomplete
            answer          formats final answer, streams to CLI
```

---

## Stack

| Layer | Tool | Notes |
|---|---|---|
| Embeddings | `BAAI/bge-m3` | 1.7GB, handles 8192 token JDs |
| Sparse vectors | `fastembed[sparse]` + BM25 | Required for Qdrant hybrid search |
| Vector DB | `qdrant-client` local mode | Persists to `~/.pilot-intel/qdrant/` |
| Reranker | `BAAI/bge-reranker-v2-m3` | Cross-encoder, top-20 → top-5 |
| Text-to-SQL | `defog/sqlcoder-7b-2` via Ollama | Wrapped as LangChain tool |
| LLM local | Ollama `llama3.1:8b` / `phi4` | phi4 for router, llama3.1 for reasoning |
| LLM API | Anthropic / OpenAI | Mirrors ApplyPilot env var pattern |
| Orchestration | `langgraph` + `langchain` | Multi-agent with subgraphs |
| Tracing | `langsmith` | `@traceable` on every node |
| Evaluation | `ragas` + `deepeval` + LangSmith | Three-layer eval strategy |
| Cache | SQLite | `(question+scope+model hash → answer)` |

All async. Use `async def` for all nodes. LangGraph `.astream()` for CLI streaming.

---

## 5 Question Types

| Type | Example | Path |
|---|---|---|
| `pure_sql` | "Response rate by ATS type" | SQL only |
| `pure_rag` | "Find JDs like my Amazon callback" | RAG only |
| `sql_summarize` | "Most common Workday errors" | SQL filter → LLM over text |
| `hybrid` | "Skills in callback JDs I'm missing" | Parallel SQL + RAG → synthesize |
| `term_expand` | "Am I applying to enough causal inference roles" | Expand → RAG → SQL count |

---

## State Schema

```python
from typing import TypedDict, Annotated
from operator import add

class AgentState(TypedDict):
    question:        str
    question_type:   str              # pure_sql | pure_rag | sql_summarize | hybrid | term_expand
    scope:           str              # SQL WHERE clause fragment
    qdrant_filter:   dict             # Qdrant metadata filter dict
    sql_queries:     Annotated[list[str], add]
    sql_results:     Annotated[list[dict], add]
    rag_queries:     Annotated[list[str], add]
    rag_results:     Annotated[list[str], add]
    expanded_terms:  list[str]
    summary:         str              # from summarizer node (type sql_summarize)
    synthesis:       str              # combined SQL + RAG answer
    reflection:      str              # what is missing or incomplete
    iterations:      int              # loop guard, max 3
    final_answer:    str
    langsmith_trace: str
```

`Annotated[list, add]` means new items append rather than overwrite on state updates.

---

## ApplyPilot Database Schema

### jobs (primary table)
```sql
CREATE TABLE jobs (
    -- Discovery
    url                   TEXT PRIMARY KEY,
    title                 TEXT,
    company               TEXT,
    salary                TEXT,
    description           TEXT,        -- short description
    location              TEXT,
    site                  TEXT,        -- linkedin | indeed | glassdoor | workday | greenhouse | ashby
    strategy              TEXT,        -- ATS type: workday | greenhouse | ashby | serper | apify
    discovered_at         TEXT,        -- ISO timestamp

    -- Enrichment
    full_description      TEXT,        -- full JD text, used for RAG
    application_url       TEXT,
    detail_scraped_at     TEXT,
    detail_error          TEXT,

    -- Scoring
    fit_score             INTEGER,     -- 1-10, LLM scored
    score_reasoning       TEXT,        -- LLM reasoning text
    scored_at             TEXT,

    -- Tailoring
    tailored_resume_path  TEXT,
    tailored_at           TEXT,
    tailor_attempts       INTEGER DEFAULT 0,

    -- Cover letter
    cover_letter_path     TEXT,
    cover_letter_at       TEXT,
    cover_attempts        INTEGER DEFAULT 0,

    -- Application
    applied_at            TEXT,
    apply_status          TEXT,        -- pending | in_progress | applied | failed | skipped
    apply_error           TEXT,        -- free text error, NOT categorical
    apply_attempts        INTEGER DEFAULT 0,
    agent_id              TEXT,
    last_attempted_at     TEXT,
    apply_duration_ms     INTEGER,
    apply_task_id         TEXT,
    apply_turns           INTEGER,
    apply_cost_usd        REAL,
    verification_confidence TEXT,

    -- Outcomes
    outcome               TEXT,        -- responded | no_response | rejected | interview
    outcome_at            TEXT,

    -- Prioritization
    optimizer_rank        INTEGER DEFAULT 0,
    last_optimizer_rank   INTEGER DEFAULT 0,
    embedding_score       REAL DEFAULT 0,

    -- Expiry
    predicted_expiry      TEXT,
    expiry_reason         TEXT,
    expiry_checked_at     TEXT,

    -- Source
    source                TEXT
)
```

### company_signals
```sql
CREATE TABLE company_signals (
    company_name      TEXT PRIMARY KEY,
    tier              TEXT,            -- tier1 | tier2 | tier3
    industry          TEXT,
    size_tier         TEXT,            -- startup | mid | enterprise
    public_private    TEXT,
    responded         INTEGER DEFAULT 0,
    notes             TEXT,
    updated_at        TEXT
)
```

### workday_portals, greenhouse_companies, ashby_companies
Portal-level discovery tracking. Columns: portal_url/company_name, last_explored_at,
explore_status, total_jobs_discovered, total_jobs_inserted.

### workday_runs, greenhouse_runs, ashby_runs
Run-level stats. Columns: started_at, ended_at, jobs_discovered, jobs_inserted,
jobs_skipped_not_us, status.

---

## Qdrant Collection Schema

Collection name: `job_descriptions`

Each point:
- `id`: hash of job URL
- `vector`: bge-m3 dense embedding of `full_description`
- `sparse_vector`: fastembed BM25 of `full_description`
- `payload`:
  ```json
  {
    "job_url": "...",
    "company": "...",
    "title": "...",
    "site": "...",
    "strategy": "...",
    "outcome": "...",
    "fit_score": 8,
    "apply_status": "applied",
    "embedding_score": 0.82,
    "discovered_at": "2025-01-15T10:00:00"
  }
  ```

---

## Node Responsibilities + Prompting Strategy

### router (supervisor_graph)
- Model: `phi4` via Ollama (fast, cheap)
- Input: `question`
- Output: `question_type`, `scope`, `qdrant_filter`
- Strategy: few-shot + structured JSON output
- Must handle edge cases: ambiguous questions default to `hybrid`
- Example output:
  ```json
  {
    "question_type": "sql_summarize",
    "scope": "strategy = 'workday' AND apply_error IS NOT NULL",
    "qdrant_filter": {}
  }
  ```

### term_expander (supervisor_graph, type term_expand only)
- Model: `llama3.1:8b` via Ollama
- Input: `question`
- Output: `expanded_terms` (list of 5-10 related terms)
- Strategy: zero-shot with domain hint
- Prompt hint: "in the context of Data Science and ML job descriptions"

### sql_node (analytics_subgraph)
- Model: `defog/sqlcoder-7b-2` via Ollama
- Input: `scope`, `sql_queries` (for retry context)
- Output: appends to `sql_queries`, `sql_results`
- Strategy: few-shot with full schema DDL + 5 example Q→SQL pairs
- On SQL error: rewrite once with error message in context
- IMPORTANT: `apply_error` is free text, never use = on it, always use LIKE or pass to summarizer

### rag_node (retrieval_subgraph)
- No LLM — pure vector search
- Input: `rag_queries`, `expanded_terms`, `qdrant_filter`
- Output: appends to `rag_results`
- Uses Qdrant hybrid search (dense + sparse simultaneously)
- Retrieves top-20, reranker reduces to top-5
- Each result includes payload metadata

### summarizer (analytics_subgraph, type sql_summarize only)
- Model: `llama3.1:8b`
- Input: `sql_results` (raw text rows)
- Output: `summary`
- Strategy: few-shot with examples of good pattern summaries
- Purpose: cluster and summarize free text (errors, reasoning, notes)

### synthesizer (reasoning_subgraph)
- Model: `llama3.1:8b`
- Input: `sql_results`, `rag_results`, `summary`
- Output: `synthesis`
- Strategy: zero-shot with explicit output format instruction
- Must cite which source (SQL or RAG) each claim comes from

### reflector (reasoning_subgraph)
- Model: `llama3.1:8b`
- Input: `question`, `synthesis`, `iterations`
- Output: `reflection`, routes to `followup` or `answer`
- Strategy: rubric-based — explicit criteria, not open-ended
- Termination criteria:
  1. Answer directly addresses the question
  2. All claims are traceable to SQL results or RAG chunks
  3. No obvious missing data that a follow-up query could provide
  4. OR `iterations >= 3`
- Output format: `{"complete": true/false, "missing": "..."}`

### followup_generator (reasoning_subgraph)
- Model: `llama3.1:8b`
- Input: `reflection`, `question_type`
- Output: appends new query to `sql_queries` or `rag_queries`
- Strategy: chain-of-thought — "what information is missing and how to get it"

### answer_node (reasoning_subgraph)
- Model: `llama3.1:8b` or Claude Sonnet (configurable)
- Input: `synthesis`, `question`
- Output: `final_answer`
- Strategy: system prompt sets tone (concise, data-driven, cite sources)
- Streams token-by-token to CLI

---

## Environment Variables

```bash
# ApplyPilot DB (read-only)
APPLYPILOT_DB=~/.applypilot/applypilot.db

# pilot-intel data dir
PILOT_INTEL_DIR=~/.pilot-intel

# LLM (pick one or set LLM_URL for local Ollama)
ANTHROPIC_API_KEY=...
OPENAI_API_KEY=...
LLM_URL=http://localhost:11434/v1    # Ollama
LLM_MODEL=llama3.1:8b               # override model

# Router model (fast, separate from main LLM)
ROUTER_MODEL=phi4
ROUTER_URL=http://localhost:11434/v1

# SQLCoder (text-to-SQL)
SQLCODER_URL=http://localhost:11434/v1
SQLCODER_MODEL=sqlcoder-7b-2-q4

# LangSmith tracing
LANGSMITH_API_KEY=...
LANGSMITH_PROJECT=pilot-intel

# Qdrant
QDRANT_PATH=~/.pilot-intel/qdrant   # local mode
# QDRANT_URL=http://localhost:6333  # server mode (optional)
```

---

## Repo Structure

```
ApplyPilot/pilot-intel/
├── pyproject.toml
├── .env.example
├── config.py                        # all paths + env var loading
├── ingest/
│   ├── __init__.py
│   ├── loader.py                    # reads applypilot.db, returns job rows
│   ├── embedder.py                  # bge-m3 dense + fastembed BM25
│   └── qdrant_store.py              # upsert, incremental sync, collection mgmt
├── retrieval/
│   ├── __init__.py
│   ├── rag.py                       # hybrid search + reranker
│   └── sql.py                       # SQLCoder tool + schema prompt + retry
├── agent/
│   ├── __init__.py
│   ├── state.py                     # AgentState TypedDict
│   ├── prompts.py                   # all prompt templates in one place
│   ├── nodes/
│   │   ├── __init__.py
│   │   ├── router.py
│   │   ├── term_expander.py
│   │   ├── sql_node.py
│   │   ├── rag_node.py
│   │   ├── summarizer.py
│   │   ├── synthesizer.py
│   │   ├── reflector.py
│   │   ├── followup.py
│   │   └── answer.py
│   ├── subgraphs/
│   │   ├── __init__.py
│   │   ├── analytics.py             # sql_node + summarizer
│   │   ├── retrieval.py             # rag_node + reranker
│   │   └── reasoning.py             # synthesizer + reflector + followup + answer
│   └── graph.py                     # supervisor graph, assembles all subgraphs
├── cache/
│   ├── __init__.py
│   └── query_cache.py               # SQLite cache keyed by hash(question+scope+model)
├── eval/
│   ├── __init__.py
│   ├── datasets/
│   │   ├── retrieval_evals.json     # question → expected job_urls in top-5
│   │   ├── sql_evals.json           # question → expected SQL + result schema
│   │   ├── answer_evals.json        # question → reference answer
│   │   └── faithfulness_evals.json  # answer → source chunks it must cite
│   ├── generate_evals.py            # pulls real DB data, LLM generates Q+A pairs
│   ├── ragas_eval.py                # context precision, recall, faithfulness, relevancy
│   ├── deepeval_eval.py             # hallucination, correctness, SQL validity
│   └── langsmith_eval.py            # LLM-as-judge, regression datasets
└── cli.py                           # typer CLI: ingest | ask | eval | status
```

---

## Build Order

Build strictly in this order. Each step must work standalone before moving to next.

1. `config.py` + `ingest/loader.py` — DB connection + verify schema
2. `ingest/embedder.py` + `ingest/qdrant_store.py` — ingest JDs into Qdrant
3. `retrieval/rag.py` — hybrid search + reranker, test standalone
4. `retrieval/sql.py` — SQLCoder tool, test against schema
5. `agent/state.py` + `agent/nodes/router.py` — single-node graph, test routing
6. `agent/nodes/sql_node.py` + `agent/nodes/rag_node.py` — test each path
7. `agent/nodes/summarizer.py` — test sql_summarize path
8. `agent/subgraphs/analytics.py` + `agent/subgraphs/retrieval.py`
9. `agent/nodes/synthesizer.py` + `agent/nodes/reflector.py` + loop
10. `agent/nodes/term_expander.py` + `agent/nodes/followup.py`
11. `agent/subgraphs/reasoning.py`
12. `agent/graph.py` — full supervisor graph
13. `cache/query_cache.py`
14. `cli.py` — wire everything to CLI
15. `eval/generate_evals.py` — generate labeled dataset
16. `eval/ragas_eval.py` + `eval/deepeval_eval.py` + `eval/langsmith_eval.py`

---

## Coding Conventions

- All async: `async def` for every node, `await` all IO
- No unnecessary comments
- No emojis
- Typed everywhere: TypedDict, dataclasses, Pydantic where needed
- Each node is a pure function: `async def node_name(state: AgentState) -> dict`
- Nodes return partial state dicts, not full state
- Errors: log + return graceful fallback state, never raise from nodes
- LangSmith: decorate every node with `@traceable`
- All prompts live in `agent/prompts.py`, never inline in node files

---

## Evaluation Metrics

### RAGAS
- `context_precision` — retrieved JDs are relevant to question
- `context_recall` — didn't miss important JDs
- `faithfulness` — answer grounded in retrieved content only
- `answer_relevancy` — answer addresses the question

### DeepEval
- `HallucinationMetric` — answer introduces facts not in sources
- `AnswerCorrectnessMetric` — vs reference answers in eval dataset
- Custom `SQLValidityMetric` — SQL parses and executes without error

### LangSmith
- LLM-as-judge on final answer quality
- Per-node latency tracking
- Token cost per query
- Regression suite: re-run on every model/prompt change

---

## Key Constraints

- READ ONLY — never write to `applypilot.db`
- `apply_error` is free text — never filter with `=`, always LIKE or pass to summarizer
- `full_description` can be NULL — always filter `WHERE full_description IS NOT NULL`
- `fit_score` is 1-10 INTEGER — outcome-anchored queries are more meaningful than score-anchored
- Reflector max iterations = 3 — hard cap, no exceptions
- bge-m3 model is 1.7GB — load once at startup, never reload per query
- All models loaded once at startup and reused across queries
