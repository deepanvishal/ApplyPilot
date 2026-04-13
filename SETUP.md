# ApplyPilot — New User Setup Guide

A step-by-step guide to get ApplyPilot running from scratch. Complete every section in order.

---

## Table of Contents

1. [Fork & Clone the Repository](#1-fork--clone-the-repository)
2. [Install Prerequisites](#2-install-prerequisites)
3. [Create Required Accounts & Get API Keys](#3-create-required-accounts--get-api-keys)
4. [Install ApplyPilot](#4-install-applypilot)
5. [Prepare Your Resume](#5-prepare-your-resume)
6. [Run the Setup Wizard](#6-run-the-setup-wizard)
7. [Fill In Your Personal Profile](#7-fill-in-your-personal-profile)
8. [Configure Job Search Preferences](#8-configure-job-search-preferences)
9. [Set API Keys in .env](#9-set-api-keys-in-env)
10. [Verify Your Setup](#10-verify-your-setup)
11. [Customize LLM Prompts (Optional)](#11-customize-llm-prompts-optional)
12. [Customize Job Sources (Optional)](#12-customize-job-sources-optional)
13. [Run the Pipeline](#13-run-the-pipeline)
14. [Web Dashboard (Optional)](#14-web-dashboard-optional)

---

## 1. Fork & Clone the Repository

**Step 1.1 — Fork on GitHub**

Go to [github.com/Pickle-Pixel/ApplyPilot](https://github.com/Pickle-Pixel/ApplyPilot) and click **Fork** (top-right). This creates your own copy so you can customize it.

**Step 1.2 — Clone your fork**

```bash
git clone https://github.com/YOUR_USERNAME/ApplyPilot.git
cd ApplyPilot
```

Replace `YOUR_USERNAME` with your GitHub username.

---

## 2. Install Prerequisites

### Python 3.11+

Check your version:
```bash
python --version
```

If below 3.11, download from [python.org/downloads](https://www.python.org/downloads/).

Create a virtual environment (recommended):
```bash
python -m venv venv

# Mac/Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

### Node.js 18+

Required only for the auto-apply stage (runs the Playwright MCP server).

Check if installed:
```bash
node --version
```

If missing, download from [nodejs.org](https://nodejs.org). Install the LTS version.

### Chrome or Chromium

Required only for auto-apply. ApplyPilot auto-detects Chrome from standard install locations. If you have Chrome installed, nothing extra is needed.

### Claude Code CLI

Required only for auto-apply.

Install from [claude.ai/code](https://claude.ai/code). Sign in with your Anthropic account. A Claude subscription is required.

---

## 3. Create Required Accounts & Get API Keys

Work through this list. Save each key — you will paste them into `.env` in [Step 9](#9-set-api-keys-in-env).

### 3.1 — Google AI Studio (Gemini) — **Required**

Used for: AI scoring, resume tailoring, cover letter generation.
Cost: **Free** — 15 requests/min, 1M tokens/day.

1. Go to [aistudio.google.com](https://aistudio.google.com)
2. Sign in with a Google account
3. Click **Get API Key** → **Create API Key**
4. Copy the key → save as `GEMINI_API_KEY`

> **Alternatively:** Use OpenAI (`OPENAI_API_KEY`) or a local model (`LLM_URL`). Gemini free tier is recommended for most users.

### 3.2 — Apify — Optional (LinkedIn job discovery)

Used for: Scraping LinkedIn job listings at higher volume.
Cost: Free tier available (~$5 credit on signup).

1. Go to [apify.com](https://apify.com) and create an account
2. Go to **Settings → Integrations → API tokens**
3. Create a new token → save as `APIFY_API_TOKEN`

### 3.3 — Serper.dev — Optional (Google Jobs search)

Used for: Searching Google Jobs via the Serper API.
Cost: 2,500 free searches on signup, then paid.

1. Go to [serper.dev](https://serper.dev) and create an account
2. Go to **Dashboard → API Key**
3. Copy the key → save as `SERPER_API_KEY`

### 3.4 — SerpAPI — Optional (alternative Google Jobs)

Used for: Alternative to Serper.dev for Google Jobs search.
Cost: 100 free searches/month, then paid.

1. Go to [serpapi.com](https://serpapi.com) and create an account
2. Go to **Dashboard → Your API Key**
3. Copy the key → save as `SERPAPI_API_KEY`

> You only need one of Serper.dev or SerpAPI, not both.

### 3.5 — CapSolver — Optional (CAPTCHA solving)

Used for: Automatically solving hCaptcha, reCAPTCHA, Turnstile, FunCaptcha during auto-apply.
Cost: Pay-per-solve (~$0.001–$0.003 per CAPTCHA). Without it, CAPTCHA-blocked applications fail gracefully.

1. Go to [capsolver.com](https://capsolver.com) and create an account
2. Go to **Dashboard → API Key**
3. Copy the key → save as `CAPSOLVER_API_KEY`

### 3.6 — Webshare or other proxy — Optional

Used for: Rotating residential proxies to avoid IP blocks during job scraping.
Cost: Paid.

If you have a proxy, save it in this format:
```
ROTATING_PROXY=http://user:pass@proxy.host:port
```

---

## 4. Install ApplyPilot

With your virtual environment active:

```bash
pip install applypilot

# python-jobspy requires a separate install (see note below)
pip install --no-deps python-jobspy
pip install pydantic tls-client requests markdownify regex
```

> **Why two install commands?** `python-jobspy` pins an exact numpy version that conflicts with pip's resolver but works fine at runtime. `--no-deps` skips the resolver; the second command installs jobspy's actual runtime requirements.

---

## 5. Prepare Your Resume

ApplyPilot needs two resume files. Place them anywhere on your computer for now — the wizard will copy them into the right location.

### resume.txt (Required)

Plain English text of your resume. The AI reads this for scoring and tailoring.

**Format rules:**
- Plain text only — no markdown, no tables, no special characters
- Include: work experience, education, skills, projects
- Do not fabricate — the AI will only reorganize what is here

Example structure:
```
John Doe
john@email.com | 555-123-4567 | linkedin.com/in/johndoe | github.com/johndoe

EXPERIENCE
Senior Software Engineer — Acme Corp (2022–Present)
- Built distributed data pipeline processing 10M events/day
- Reduced latency by 50% through query optimization

SOFTWARE ENGINEER — Startup Inc (2020–2022)
- Led backend development for payment service
- ...

EDUCATION
B.S. Computer Science — State University (2020)

SKILLS
Python, Go, SQL, JavaScript, React, FastAPI, Docker, AWS, PostgreSQL, Git
```

### resume.pdf (Optional)

A PDF version for upload to job applications. If you skip this, forms that require a PDF upload will be handled differently or skipped.

---

## 6. Run the Setup Wizard

The wizard walks you through creating all config files interactively.

```bash
applypilot init
```

It will prompt you for:
- Path to your `resume.txt` (and optionally `resume.pdf`)
- Personal information (name, email, phone, address, LinkedIn, etc.)
- Work authorization and compensation
- Skills
- Resume facts to preserve during tailoring
- Job search preferences
- LLM API key

After the wizard runs, your config files are at `~/.applypilot/`:
```
~/.applypilot/
├── .env              ← API keys
├── profile.json      ← Personal data
├── searches.yaml     ← Job search config
├── resume.txt        ← Copied from your input
└── resume.pdf        ← Copied from your input (if provided)
```

You can re-run `applypilot init` at any time to update any field, or edit the files directly.

---

## 7. Fill In Your Personal Profile

Open `~/.applypilot/profile.json` in a text editor and fill in every field. Use [`profile.example.json`](profile.example.json) as reference.

Below is every field explained:

### Personal Information

```json
"personal": {
  "full_name": "Your Legal Name",           // Used on application forms
  "preferred_name": "Your Nickname",        // Used in cover letter sign-offs
  "email": "you@email.com",                 // Application contact email
  "password": "YourPassword123",            // Generic password for job site accounts
  "phone": "555-123-4567",
  "address": "123 Main St",
  "city": "San Francisco",
  "province_state": "CA",
  "country": "United States",
  "postal_code": "94105",
  "linkedin_url": "https://linkedin.com/in/yourprofile",
  "github_url": "https://github.com/yourusername",
  "portfolio_url": "",                      // Leave blank if none
  "website_url": ""                         // Leave blank if none
}
```

### Work Authorization

```json
"work_authorization": {
  "legally_authorized_to_work": "Yes",      // "Yes" or "No"
  "require_sponsorship": "No",              // "Yes" or "No" (H1B, visa)
  "work_permit_type": ""                    // e.g. "PR", "Open Work Permit", or leave blank
}
```

### Availability

```json
"availability": {
  "earliest_start_date": "Immediately",     // Or a date: "2026-06-01"
  "available_for_full_time": "Yes",
  "available_for_contract": "No"
}
```

### Compensation

```json
"compensation": {
  "salary_expectation": "120000",           // Floor salary (numeric, no symbols)
  "salary_currency": "USD",
  "salary_range_min": "110000",
  "salary_range_max": "140000",
  "currency_conversion_note": ""            // e.g. "CAD" if applying cross-border
}
```

### Experience

```json
"experience": {
  "years_of_experience_total": "5",
  "education_level": "Bachelor's Degree",   // "Bachelor's Degree", "Master's", "PhD", "Self-taught"
  "current_job_title": "Software Engineer",
  "current_company": "Acme Corp",
  "target_role": "senior software engineer" // What you're applying for (used in scoring prompts)
}
```

### Skills Boundary

**Important:** Only list skills you actually have. The AI will never add skills not listed here.

```json
"skills_boundary": {
  "languages": ["Python", "SQL", "TypeScript"],
  "frameworks": ["FastAPI", "React", "Django"],
  "devops": ["Docker", "Kubernetes", "AWS", "CI/CD"],
  "databases": ["PostgreSQL", "Redis", "MongoDB"],
  "tools": ["Git", "Linux", "Terraform"]
}
```

### Resume Facts

These values are preserved exactly during tailoring — the AI will not alter them.

```json
"resume_facts": {
  "preserved_companies": ["Acme Corp", "Startup Inc"],  // All companies you've worked at
  "preserved_projects": ["ProjectName", "RepoName"],    // Notable projects
  "preserved_school": "State University",
  "real_metrics": ["50% latency reduction", "10x throughput improvement"]  // Real numbers from your resume
}
```

### EEO Voluntary (Equal Employment Opportunity)

These are voluntary — defaults to "decline to answer" for all.

```json
"eeo_voluntary": {
  "gender": "Decline to self-identify",
  "race_ethnicity": "Decline to self-identify",
  "veteran_status": "I am not a protected veteran",
  "disability_status": "I do not wish to answer"
}
```

---

## 8. Configure Job Search Preferences

Open `~/.applypilot/searches.yaml`. Use [`src/applypilot/config/searches.example.yaml`](src/applypilot/config/searches.example.yaml) as reference.

### Search Queries

List the job titles you want to search for. Tier 1 = most precise / highest priority.

```yaml
queries:
  - query: "software engineer"
    tier: 1
  - query: "backend engineer"
    tier: 1
  - query: "full stack developer"
    tier: 2
  - query: "python developer"
    tier: 2
```

### Locations

```yaml
locations:
  - location: "San Francisco, CA"
    remote: false
  - location: "Remote"
    remote: true
```

### Location Filtering

Patterns to accept or reject job locations (applied after scraping):

```yaml
location:
  accept_patterns:
    - "San Francisco"
    - "Remote"
    - "United States"
    - "US"
  reject_patterns:
    - "onsite only"
    - "London, UK"
    - "India"
```

### Country

Used for Indeed/LinkedIn/Google Jobs geographic filtering:
```yaml
country: "USA"
```

### Job Boards

Boards to search (all supported):
```yaml
boards:
  - indeed
  - linkedin
  - glassdoor
  - zip_recruiter
  - google
```

### Search Defaults

```yaml
defaults:
  results_per_site: 100   # Max results per board per query
  hours_old: 24           # Only show jobs posted within this many hours
```

### Exclude Titles

Job titles to skip entirely (case-insensitive):
```yaml
exclude_titles:
  - "intern"
  - "senior director"
  - "VP "
  - "clearance required"
  - "principal scientist"
```

---

## 9. Set API Keys in .env

Open `~/.applypilot/.env`. Use [`.env.example`](.env.example) as reference.

Fill in every key you obtained in [Step 3](#3-create-required-accounts--get-api-keys):

```bash
# LLM Provider — pick one
GEMINI_API_KEY=your_gemini_key_here
# OPENAI_API_KEY=your_openai_key_here
# LLM_URL=http://127.0.0.1:8080/v1       # For local Ollama/llama.cpp
# LLM_MODEL=gemini-2.0-flash             # Override model (optional)

# Job Discovery — optional but recommended
APIFY_API_TOKEN=your_apify_token_here
SERPER_API_KEY=your_serper_key_here
# SERPAPI_API_KEY=your_serpapi_key_here  # Alternative to SERPER_API_KEY

# Auto-Apply — optional
CAPSOLVER_API_KEY=your_capsolver_key_here

# Proxy — optional
# ROTATING_PROXY=http://user:pass@proxy.host:port
```

**Leave lines commented out (`#`) if you don't have that key.** Only `GEMINI_API_KEY` (or one LLM key) is strictly required.

---

## 10. Verify Your Setup

Run the doctor command. It checks every dependency and tells you what's missing:

```bash
applypilot doctor
```

Expected output when fully set up:
```
✓ Python 3.11+
✓ Node.js (for npx)
✓ Chrome detected
✓ Claude Code CLI
✓ resume.txt found
✓ profile.json found
✓ searches.yaml found
✓ GEMINI_API_KEY set
```

Fix any items marked with `✗` before proceeding.

---

## 11. Customize LLM Prompts (Optional)

ApplyPilot's prompts are dynamically generated from your `profile.json` — you don't edit prompt files directly. Instead, the fields below drive prompt behavior:

| What to change | Where |
|---|---|
| How you're described in cover letters | `profile.json` → `experience.target_role`, `skills_boundary` |
| Salary negotiation behavior | `profile.json` → `compensation.*` |
| What the AI never changes in your resume | `profile.json` → `resume_facts.*` |
| Screening question answers | `profile.json` → `work_authorization`, `availability`, `experience` |
| Hard rules during auto-apply | `profile.json` → `work_authorization.legally_authorized_to_work`, `require_sponsorship` |

The prompt source files (for advanced users who want to modify behavior directly):

| Prompt | File |
|---|---|
| Job scoring | [`src/applypilot/scoring/scorer.py`](src/applypilot/scoring/scorer.py) |
| Resume tailoring | [`src/applypilot/scoring/tailor.py`](src/applypilot/scoring/tailor.py) |
| Cover letter | [`src/applypilot/scoring/cover_letter.py`](src/applypilot/scoring/cover_letter.py) |
| Auto-apply form filling | [`src/applypilot/apply/prompt.py`](src/applypilot/apply/prompt.py) |

---

## 12. Customize Job Sources (Optional)

### Add Workday Companies

Edit [`src/applypilot/config/employers.yaml`](src/applypilot/config/employers.yaml) to add companies that use Workday ATS. 48 are preconfigured — add your targets:

```yaml
- name: "Company Name"
  url: "https://companyname.wd5.myworkdayjobs.com/careers"
```

### Add or Block Direct Career Sites

Edit [`src/applypilot/config/sites.yaml`](src/applypilot/config/sites.yaml) to:
- Add custom career page URLs
- Block specific domains from being scraped or applied to

---

## 13. Run the Pipeline

### Full pipeline (recommended):
```bash
applypilot run
```

### With parallel workers (faster):
```bash
applypilot run --workers 4
```

### Preview without executing:
```bash
applypilot run --dry-run
```

### Run specific stages only:
```bash
applypilot run discover        # Stage 1: find jobs
applypilot run enrich          # Stage 2: fetch full descriptions
applypilot run score           # Stage 3: AI scoring
applypilot run tailor          # Stage 4: tailor resumes
```

### Check pipeline status:
```bash
applypilot status
```

### Auto-apply (after pipeline has run):
```bash
applypilot apply               # Launch browser-driven submission
applypilot apply --dry-run     # Fill forms but don't submit
applypilot apply --workers 3   # 3 parallel Chrome instances
```

---

## 14. Web Dashboard (Optional)

ApplyPilot includes a React dashboard for browsing discovered jobs.

```bash
cd webui
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) in your browser.

The dashboard shows:
- **Dashboard** — Pipeline stats, score distribution, jobs by portal
- **Jobs** — Browse all discovered jobs, filter by score/stage/board/search term, mark applied or failed
- **Pipeline** — Run pipeline stages from the UI
- **Doctor** — Same diagnostics as `applypilot doctor`

---

## Quick Reference

| File | Purpose |
|---|---|
| `~/.applypilot/.env` | API keys |
| `~/.applypilot/profile.json` | Personal info, skills, resume facts |
| `~/.applypilot/searches.yaml` | Job search queries and preferences |
| `~/.applypilot/resume.txt` | Plain-text resume (required) |
| `~/.applypilot/resume.pdf` | PDF resume (optional) |
| `~/.applypilot/applypilot.db` | SQLite job database (auto-managed) |
| `~/.applypilot/tailored_resumes/` | AI-generated resumes per job (auto) |
| `~/.applypilot/cover_letters/` | AI-generated cover letters (auto) |
| `src/applypilot/config/employers.yaml` | Workday employer list |
| `src/applypilot/config/sites.yaml` | Career site list and blocklist |

| Account | Key | Required |
|---|---|---|
| Google AI Studio | `GEMINI_API_KEY` | Yes |
| Apify | `APIFY_API_TOKEN` | No (LinkedIn discovery) |
| Serper.dev | `SERPER_API_KEY` | No (Google Jobs) |
| SerpAPI | `SERPAPI_API_KEY` | No (alt Google Jobs) |
| CapSolver | `CAPSOLVER_API_KEY` | No (CAPTCHA solving) |
| Webshare/proxy | `ROTATING_PROXY` | No (IP rotation) |
