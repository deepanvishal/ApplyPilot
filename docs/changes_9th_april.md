# Changes — April 9, 2026

## 1. Company Blocklist (`src/applypilot/company_blocklist.py`)

**New file.** Centralizes all company exclusion logic in one place.

- `COMPANY_BLOCKLIST` — LIKE patterns matched against `lower(company)`
- `COMPANY_BLOCKLIST_EXACT` — exact case-sensitive matches (e.g. `"EY"`)
- `purge_blocked_companies(dry_run)` — deletes matching jobs, always preserves `applied` and `manual` rows

**Companies in blocklist:**
`%walmart%`, `%sofi%`, `%so fi%`, `%microsoft%`, `%amazon%`, `%aws%`, `%prime%`,
`%deloitte%`, `%pwc%`, `%pricewaterhousecoopers%`, `%capital one%`, `%capitalone%`,
`%ernst young%`, `%ernst & young%`, `%meta%`, `EY` (exact)

---

## 2. Shared Titles Utility (`src/applypilot/utils/titles.py`)

**New file.** Eliminates `_DEFAULT_TITLES` / `_load_titles()` / `_write_default_titles()` that were copy-pasted across 4 modules.

- `DEFAULT_TITLES` — canonical list of 8 job titles
- `load_titles()` — loads from `~/.applypilot/titles.yaml`, creates defaults if missing
- `_write_default_titles(path)` — writes the default YAML file

**Modules updated to import from here (duplicate code removed):**
- `workday/pipeline.py`
- `greenhouse/pipeline.py`
- `ashby/pipeline.py`
- `genie/pipeline.py`
- `serper/pipeline.py` (was using its own `_DEFAULT_TITLES`)

Unused `from pathlib import Path` imports also removed from all four pipeline files.

---

## 3. Serper Pipeline (`src/applypilot/serper/pipeline.py`)

**Two new functions added:**

### `dedup_serper_jobs() -> dict`
Deduplicates `serper_jobs` table by `url`, keeping the highest `id` per URL.
Returns `{before, after, removed}`.

### `run_serper_combined(...) -> dict`
Runs Serper.dev (LinkedIn) and SerpAPI (Google Jobs) **in parallel** using a `ThreadPoolExecutor`, then calls `dedup_serper_jobs()` **sequentially** at the end.

Parameters mirror both individual pipelines:
- `tbs` — Serper.dev time filter
- `date_filter` — SerpAPI date filter
- `workers`, `dry_run`, `titles_override`, `locations_override`

Returns combined stats dict with `serper`, `serpapi`, and `dedup` sub-keys.

---

## 4. CLI (`src/applypilot/cli.py`)

### `exploreserper` — merged command
Previously two separate commands (`exploreserper` for LinkedIn, `exploregooglejobs` for Google Jobs). Now a single command that:
1. Runs both engines in parallel via `run_serper_combined()`
2. Deduplicates `serper_jobs` sequentially
3. Reports per-engine stats + dedup count

`exploregooglejobs` command removed.

New flags: `--tbs` (Serper.dev) and `--date-filter` (SerpAPI) both available on the same command.

### `run-discover` — new command
Calls `jobspy.run_discovery()` directly (Indeed, LinkedIn, Glassdoor, ZipRecruiter).

### `dedup-jobs` — now also purges blocklist
After deduplicating by `application_url`, automatically calls `purge_blocked_companies()`.
No longer need to run `purge-blocked` as a separate step.

### `purge-blocked` — still available standalone
Still exists for manual use, but is now also triggered inside `dedup-jobs`.

### `VALID_STAGES` — updated
```python
# Before
("discover", "enrich", "score", "tailor", "cover", "pdf")

# After
("exploreserper", "exploreemail", "run-genie", "enrich", "score",
 "prioritize", "tailor", "allocate", "apply")
```

---

## 5. Full Pipeline (`src/applypilot/pipeline.py`)

`applypilot run` now executes the complete end-to-end pipeline:

| # | Stage | What it does |
|---|---|---|
| 1 | `exploreserper` | Serper.dev + SerpAPI in parallel → `serper_jobs` → dedup → promote to `jobs` |
| 2 | `exploreemail` | Gmail job alert URLs → `jobs` |
| 3 | `run-genie` | All ATS portals → `genie_jobs` (incremental) → promote to `jobs` |
| 4 | `enrich` | Scrape full descriptions + apply URLs |
| 5 | `score` | LLM fit scoring 1–10 |
| 6 | `prioritize` | Embedding similarity rerank → `embedding_score` |
| 7 | `tailor` | LLM resume tailoring for score ≥ 7 |
| 8 | `allocate` | Bayesian queue allocation → `optimizer_rank` |
| 9 | `apply` | Auto-apply via Chrome + Claude Code (score ≥ 7, 3 workers) |

**Removed from full pipeline:** `cover` and `pdf` (can still be run individually via `applypilot run cover pdf`).

**Scoring flow:**
`score` → `prioritize` (embedding rerank) → `allocate` (Bayesian mix by tier, filtered score ≥ 7, sorted by `embedding_score`) → `apply`

**Stage dependencies (streaming mode):**
- `exploreserper`, `exploreemail`, `run-genie` — run independently (no upstream)
- `enrich` — waits for `run-genie`
- `score` → `prioritize` → `tailor` → `allocate` → `apply` — each waits for previous

---

## 6. Manual DB Cleanup (run during session)

Jobs removed from `jobs` table (applied/manual rows preserved throughout):

| Company pattern | Deleted |
|---|---|
| `%walmart%` | 65 |
| `%sofi%` | 11 |
| `%microsoft%` | 183 |
| `%prime%` | 6 |
| `%deloitte%` | 164 |
| `%pwc%` | 114 |
| `%pricewaterhousecoopers%` | 22 |
| `%capital one%` + `%capitalone%` | 62 |
| **Total** | **627** |

Amazon, AWS, so fi had 0 non-applied rows at time of deletion.

---

## 7. Prompt Update (`src/applypilot/apply/prompt.py`)

- **Never email recruiters** — rule strengthened from "no unsolicited emails" to an absolute ban under all circumstances
- **Email-only applications** — jobs where the only apply method is email now produce `RESULT:FAILED:email_only_application` instead of sending an email

---

## 8. Docs (`docs/jobs-table-commands.md`)

New reference doc listing every CLI command that writes to `jobs`, `genie_jobs`, or `serper_jobs`, including the data flow diagram.
