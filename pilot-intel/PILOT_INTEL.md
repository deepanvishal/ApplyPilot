# pilot-intel — Technical Architecture

> **Source of truth split:**
> - `CONTEXT.md` is the source of truth for LLM context (injected into every node at runtime).
> - `PILOT_INTEL.md` (this file) is the source of truth for technical architecture.

---

## What pilot-intel is

A natural language analytics layer on top of ApplyPilot's SQLite database. Users ask questions in plain English; pilot-intel routes them through a LangGraph agent that executes SQL queries, semantic RAG retrieval, and LLM synthesis to produce data-driven answers.

---

## Architecture overview

```
cli.py (Typer)
    └── agent/graph.py  (LangGraph supervisor)
            ├── agent/subgraphs/retrieval.py   term_expander → rag_node
            ├── agent/subgraphs/analytics.py   sql_node → summarizer
            └── agent/subgraphs/reasoning.py   synthesizer → reflector → followup (loop)
                                                          └── answer
```

**Entry points:** `pilot-intel ingest | ask | eval | status`

---

## Module map

| Path | Purpose |
|------|---------|
| `agent/graph.py` | Supervisor graph; `run(question)` async entry point |
| `agent/state.py` | `AgentState` TypedDict; `_keep_last` reducer |
| `agent/prompts.py` | All system prompts — never inline in node files |
| `agent/llm_client.py` | Backend-agnostic `chat()`; injects `CONTEXT.md` into every system prompt |
| `agent/nodes/` | 9 nodes: router, term_expander, sql_node, rag_node, summarizer, synthesizer, reflector, followup, answer |
| `agent/subgraphs/` | 3 sub-pipelines wired into the supervisor |
| `retrieval/sql.py` | `generate_sql()`, `execute_sql()`, `run_sql_tool()`, `_fix_known_errors()` |
| `retrieval/rag.py` | `hybrid_search()`, `run_rag_tool()`, CrossEncoder reranker |
| `ingest/loader.py` | Load jobs from ApplyPilot DB; `get_db_stats()` |
| `ingest/chunker.py` | Parent/child chunk splitting (600 / 150 tokens) |
| `ingest/embedder.py` | BGE-M3 dense + sparse embeddings |
| `ingest/qdrant_store.py` | `ingest_from_db()`, Qdrant upsert |
| `ingest/record_manager.py` | `SQLiteRecordManager` — deduplication index |
| `cache/query_cache.py` | SQLite query cache; 24-hour TTL |
| `config.py` | All env vars and path constants |
| `logging_config.py` | `setup_logging()`, `log_node_input/output`, `_DebugFilter` |

---

## ApplyPilot Database Schema

Database location: `~/.applypilot/applypilot.db` (read-only)

### jobs table — key columns

| Column | Type | Description |
|--------|------|-------------|
| `url` | TEXT PK | Job posting URL |
| `title` | TEXT | Job title |
| `company` | TEXT | Company name |
| `fit_score` | INTEGER | LLM resume match score 1–10 (NULL = unscored) |
| `apply_status` | TEXT | Pipeline state — see values below |
| `apply_error` | TEXT | Short error code when status = 'failed' |
| `strategy` | TEXT | Discovery method — see values below |
| `site` | TEXT | ATS/job board platform — see values below |
| `applied_at` | TEXT | ISO timestamp of successful submit |
| `apply_cost_usd` | REAL | LLM API cost per application |
| `apply_duration_ms` | INTEGER | Wall-clock ms per application |
| `apply_turns` | INTEGER | Agent turns used |
| `outcome` | TEXT | **ALL NULL** — no employer responses recorded yet |

#### apply_status values (real DB)

| Value | Count | Meaning |
|-------|-------|---------|
| NULL | 50,841 | Not yet processed |
| `'Not in US'` | 4,568 | Location-filtered (note: capital N, capital US) |
| `'applied'` | 4,196 | Successfully submitted |
| `'failed'` | 3,446 | Agent encountered a technical error |
| `'already_applied'` | 307 | Duplicate, skipped |
| `'manual'` | 16 | Flagged for manual application |
| `'in_progress'` | 4 | Currently being processed |

`'pending'` and `'skipped'` do NOT exist in this DB.

#### strategy values (real DB) — HOW the job was discovered

| Value | Count | Meaning |
|-------|-------|---------|
| `'serpapi'` | 27,218 | Google Jobs / SerpAPI search |
| `NULL` | 19,903 | Unknown / legacy |
| `'jobspy'` | 14,337 | JobSpy multi-board scraper |
| `'genie'` | 1,920 | Genie ATS portal crawler |

`'workday'`, `'greenhouse'`, `'linkedin'` are NOT strategy values — those appear in `site`.

#### site values (real DB) — WHERE the job lives

| Value | Count | Meaning |
|-------|-------|---------|
| `'linkedin'` | 28,265 | LinkedIn job board |
| `'workday'` | 15,133 | Workday ATS |
| `'direct'` | 10,825 | Company career page |
| `'indeed'` | 3,585 | Indeed job board |
| `'greenhouse'` | 3,039 | Greenhouse ATS |
| `'ashby'` | 1,331 | Ashby ATS |
| `'lever'` | 827 | Lever ATS |
| `'smartrecruiters'` | 230 | SmartRecruiters ATS |
| `'bamboohr'` | 85 | BambooHR ATS |
| `'jobvite'` | 58 | Jobvite ATS |

`'glassdoor'` is NOT a valid site value in this DB.

#### apply_error codes (real DB) — short codes, use LIKE for matching

| Code | Count |
|------|-------|
| `'expired'` | 2,091 |
| `'timed_out'` | 417 |
| `'unknown'` | 307 |
| `'no_result_line'` | 293 |
| `'login_issue'` | 212 |
| `'captcha'` | 134 |
| `'site_blocked'` | 102 |
| `'form_validation_loop'` | 71 |
| `'not_a_job_application'` | 59 |
| `'blocked_domain'` | 48 |
| `'not_eligible_work_auth'` | 32 |
| `'sso_required'` | 26 |
| `'manual ATS'` | 16 |
| `'page_error'` | 12 |
| `'stuck'` | 6 |

### company_signals table — key columns

| Column | Type | Values |
|--------|------|--------|
| `company_name` | TEXT PK | |
| `tier` | TEXT | `'enterprise'` (4,867) \| `'startup'` (2,072) \| `'unknown'` (1,462) \| `'tier2'` (1,060) \| `'faang'` (60) |
| `size_tier` | TEXT | `'50k+'` \| `'5k-50k'` \| `'500-5k'` \| `'50-500'` \| `'1-50'` \| `'unknown'` |
| `industry` | TEXT | `'other'` \| `'healthtech'` \| `'consulting'` \| `'saas'` \| `'fintech'` \| `'ai_ml'` \| … |
| `responded` | INTEGER | Always 0 — no employer responses recorded |

---

## LLM backends

Configured via environment variables in `config.py`:

| Setting | Default | Purpose |
|---------|---------|---------|
| `ANTHROPIC_API_KEY` | — | Enables Anthropic backend (preferred) |
| `ANTHROPIC_LLM_MODEL` | `claude-haiku-4-5-20251001` | Main nodes |
| `ANTHROPIC_ROUTER_MODEL` | `claude-haiku-4-5-20251001` | Router node |
| `LLM_URL` / `LLM_MODEL` | `http://localhost:11434/v1` / `llama3.1:8b` | Ollama fallback |
| `ROUTER_URL` / `ROUTER_MODEL` | same / `phi4` | Ollama router fallback |

---

## Qdrant vector store

- Local path: `~/.pilot-intel/qdrant/` (default)
- Collection: `job_descriptions`
- Vector strategy: BGE-M3 dense + BM25 sparse (hybrid search)
- Reranker: `BAAI/bge-reranker-v2-m3` (CrossEncoder)
- Parent/child chunking: 600-token parents, 150-token children (20-token overlap)

---

## Query cache

- Location: `~/.pilot-intel/cache.db`
- Key: `SHA256(question + scope + model)`
- TTL: 24 hours (`expires_at` column; NULL rows treated as expired)
- Invalidation: automatic on TTL expiry; manual via `cache_stats()` inspection

---

## Evaluation

Eval datasets live in `eval/datasets/` (JSON). Harness stubs in `eval/*.py` — not yet implemented.

```
eval/datasets/
    answer_evals.json
    faithfulness_evals.json
    retrieval_evals.json
    sql_evals.json
```

---

## Environment

All config loaded from (in priority order):
1. `~/.applypilot/.env`
2. `~/.pilot-intel/.env`
3. `.env` in current working directory

See `.env.example` for all available settings.
