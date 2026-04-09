# ApplyPilot — Commands That Write to Jobs Tables

## Tables Overview

| Table | Purpose |
|---|---|
| `jobs` | Main pipeline table — all jobs flow through here |
| `genie_jobs` | Staging table used by `run-genie` before promotion |
| `serper_jobs` | Staging table used by serper/Google Jobs before promotion |

---

## Commands → `jobs` (direct insert)

### `applypilot exploreworkday`
Scrapes Workday ATS portals and inserts discovered jobs directly into `jobs`.

```
applypilot exploreworkday               # resume + insert (default)
applypilot exploreworkday 100           # limit to 100 portals
applypilot exploreworkday --no-resume   # fresh start
applypilot exploreworkday --dry-run     # preview, no insert
```

---

### `applypilot exploregreenhouse`
Scrapes Greenhouse ATS portals and inserts directly into `jobs`.

```
applypilot exploregreenhouse
applypilot exploregreenhouse 50
applypilot exploregreenhouse --no-resume
applypilot exploregreenhouse --dry-run
```

---

### `applypilot exploreashby`
Scrapes Ashby ATS portals and inserts directly into `jobs`.

```
applypilot exploreashby
applypilot exploreashby 50
applypilot exploreashby --no-resume
applypilot exploreashby --dry-run
```

---

### `applypilot exploreemail`
Extracts LinkedIn job URLs from Gmail job alert emails and inserts into `jobs`.

```
applypilot exploreemail
applypilot exploreemail --days 7
```

---

### `applypilot Genie-get_me_jobs`
Convenience wrapper — runs Workday → Greenhouse → Ashby in sequence and inserts all into `jobs`.

```
applypilot Genie-get_me_jobs
applypilot Genie-get_me_jobs --dry-run
```

---

### `applypilot promote-serper-jobs`
Promotes all records from `serper_jobs` into `jobs` (INSERT OR IGNORE). Run after `exploreserper` or `exploregooglejobs`.

```
applypilot promote-serper-jobs
```

---

## Commands → `genie_jobs` (staging)

### `applypilot run-genie`
Discovers jobs from all ATS portals (Workday, Greenhouse, Ashby, Lever, BambooHR) into `genie_jobs`. Default mode is incremental (only portals that previously had matching jobs).

```
applypilot run-genie                              # incremental (default)
applypilot run-genie --full                       # all ~12k portals
applypilot run-genie --limit 50
applypilot run-genie --no-resume
applypilot run-genie --dry-run
applypilot run-genie --ats workday --ats greenhouse
applypilot run-genie --workers 3
```

---

## Commands → `serper_jobs` (staging)

### `applypilot exploreserper`
Discovers LinkedIn jobs via Google Serper search and inserts into `serper_jobs`. Run `promote-serper-jobs` afterwards to move to `jobs`.

```
applypilot exploreserper
applypilot exploreserper --tbs qdr:d      # past day
applypilot exploreserper --tbs qdr:w      # past week (default)
applypilot exploreserper --tbs qdr:m      # past month
applypilot exploreserper --workers 5
applypilot exploreserper --dry-run
applypilot exploreserper --title "Data Scientist" --location "Remote"
```

---

### `applypilot exploregooglejobs`
Discovers jobs via SerpAPI Google Jobs engine into `serper_jobs`. Surfaces direct ATS postings with full descriptions. Run `promote-serper-jobs` afterwards.

```
applypilot exploregooglejobs
applypilot exploregooglejobs --date-filter "1 day"
applypilot exploregooglejobs --date-filter "1 month"
applypilot exploregooglejobs --workers 5
applypilot exploregooglejobs --dry-run
applypilot exploregooglejobs --title "ML Engineer" --location "New York, NY"
```

---

## Commands That Update `jobs` (not insert)

### `applypilot run`
Runs pipeline stages that enrich and score existing rows in `jobs`. Stages: `discover`, `enrich`, `score`, `tailor`, `cover`, `pdf`.

```
applypilot run                        # all stages
applypilot run discover enrich score  # specific stages
applypilot run --min-score 8
applypilot run --dry-run
```

### `applypilot apply`
Updates `apply_status`, `applied_at`, and related fields on `jobs` rows as applications are submitted.

```
applypilot apply
applypilot apply --limit 10 --workers 2
applypilot apply --mark-applied <url>
applypilot apply --mark-failed <url> --fail-reason sso_required
applypilot apply --reset-failed
```

### `applypilot dedup_jobs`
Deduplicates `jobs` by `application_url`, removing redundant rows.

```
applypilot dedup_jobs
```

### `applypilot release-locked-jobs`
Resets jobs stuck in `in_progress` back to the queue (sets `apply_status = NULL`).

```
applypilot release-locked-jobs
```

---

## Flow Summary

```
exploreserper / exploregooglejobs
        ↓
   serper_jobs
        ↓
promote-serper-jobs
        ↓
      jobs  ←── exploreworkday / exploregreenhouse / exploreashby / exploreemail / Genie-get_me_jobs
        ↓
run (enrich → score → tailor → cover → pdf)
        ↓
     apply
```

`run-genie` writes to `genie_jobs` separately and has its own promotion step.
