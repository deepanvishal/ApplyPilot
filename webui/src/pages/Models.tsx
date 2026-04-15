const MODELS = [
  {
    id: 'gemini',
    name: 'Gemini 2.0 Flash',
    provider: 'Google',
    color: '#4285f4',
    icon: '⚡',
    role: 'Scoring, Tailoring & Enrichment LLM',
    stages: ['Stage 3 — Score', 'Stage 2 — Enrich (fallback)', 'Stage 5 — Tailor', 'Cover Letter'],
    what: [
      'Reads your resume + a job description, scores fit on a scale of 1–10.',
      'Applies hard penalties (clearance required → 1, non-US → 1, hourly pay → 1) and boosts for deep-match roles (recommendation systems, embeddings → 10).',
      "For tailoring, rewrites each resume section to match the job's keywords and stack while preserving only your real metrics and companies.",
      'Returns structured JSON (title, summary, skills, experience bullets) that the PDF renderer assembles.',
    ],
    inputs: 'Resume text + job description (full or summary)',
    outputs: 'fit_score (1–10) + score_reasoning, or tailored resume JSON',
    env: 'GEMINI_API_KEY',
    fallback: 'Swap to OpenAI by setting OPENAI_API_KEY, or a local model via LLM_URL.',
  },
  {
    id: 'claude',
    name: 'Claude (via Claude Code CLI)',
    provider: 'Anthropic',
    color: '#d97706',
    icon: '🤖',
    role: 'Auto-Apply Form Filler',
    stages: ['Stage 6 — Apply'],
    what: [
      'Each apply worker launches Claude Code CLI as a subprocess, providing it a structured JSON prompt describing the application form.',
      'Claude navigates the ATS (Workday, Greenhouse, Ashby, Lever, etc.), fills every field, uploads the tailored resume PDF, and submits.',
      'Reports confidence level and field-by-field success back to ApplyPilot, which records apply_status and verification_confidence.',
    ],
    inputs: 'ATS URL + tailored resume path + profile.json (name, phone, work auth, salary expectations, etc.)',
    outputs: 'apply_status (applied/failed) + agent_id + apply_duration_ms + verification_confidence',
    env: 'claude CLI must be installed (claude.ai/code)',
    fallback: 'No fallback — Stage 6 requires Claude Code CLI.',
  },
  {
    id: 'embedding',
    name: 'Sentence Transformer (BGE / MiniLM)',
    provider: 'HuggingFace (local)',
    color: '#06b6d4',
    icon: '🔢',
    role: 'Embedding Similarity Reranker',
    stages: ['Stage 4 — Embedding / Prioritize'],
    what: [
      'Encodes your resume and every job description into dense vector representations.',
      'Computes cosine similarity between your resume embedding and each job embedding.',
      'Stores the result as embedding_score (0–1) — a content-level match score independent of keyword matching.',
      'Prefers fine-tuned bge-finetuned model if present in ~/.applypilot/; falls back to all-MiniLM-L6-v2.',
    ],
    inputs: 'Resume text + job full_description',
    outputs: 'embedding_score per job (0.0–1.0)',
    env: 'No API key — runs locally. CUDA auto-detected.',
    fallback: 'CPU inference if no GPU. MiniLM if BGE not available.',
  },
  {
    id: 'classifier',
    name: 'Local LLaMA / XGBoost Classifier',
    provider: 'Local (Ollama / sklearn)',
    color: '#10b981',
    icon: '🏷️',
    role: 'Company Tier Classifier',
    stages: ['Stage 5 — Optimize (classify-companies)'],
    what: [
      'Classifies each company into Tier 1 (FAANG-level), Tier 2 (mid-size known brands), or Tier 3 (other).',
      'ML path uses a trained XGBoost model on company name + industry features.',
      'LLM path sends batches of company names to a local Ollama model (LLaMA 3 or similar) for zero-shot classification when the ML model is uncertain.',
      'Tier affects the Bayesian allocator — Tier 1 companies get higher application priority.',
    ],
    inputs: 'Company name, industry, size tier (from company_signals or enrichment)',
    outputs: 'Tier label (1 / 2 / 3) stored in company_signals',
    env: 'Ollama optional (ollama.ai). Falls back to rule-based matching.',
    fallback: 'Rule-based keyword matching for well-known companies.',
  },
  {
    id: 'optimizer',
    name: 'Bayesian Queue Optimizer',
    provider: 'Built-in (Python)',
    color: '#a78bfa',
    icon: '📊',
    role: 'Application Queue Ranking',
    stages: ['Stage 5 — Optimize (optimize-queue)'],
    what: [
      'Assigns optimizer_rank to each ready-to-apply job, maximising expected positive outcomes.',
      'Combines fit_score, embedding_score, company tier (1–3), and historical response probability from company_signals.',
      'Higher-tier companies with higher scores and no prior non-response get ranked first.',
      "Responds to feedback — if a company has company_signals.responded=0 (no response), it's down-ranked in future runs.",
      'The allocation queue in the Dashboard shows the result of this ranking.',
    ],
    inputs: 'jobs table (score, embedding, tailored status) + company_signals (response history)',
    outputs: 'optimizer_rank per job — the apply order Chrome workers follow',
    env: 'No external dependency. Pure Python + SQLite.',
    fallback: 'Falls back to fit_score ordering if no tier/signal data available.',
  },
]

export default function Models() {
  return (
    <div>
      <div className="page-header">
        <div className="page-title">Models</div>
        <div className="page-subtitle">What each AI model does in the pipeline</div>
      </div>

      <div className="models-list">
        {MODELS.map(m => (
          <div key={m.id} className="model-card" style={{ borderLeftColor: m.color }}>
            {/* Header */}
            <div className="model-header">
              <div className="model-icon" style={{ background: m.color + '22', color: m.color }}>
                {m.icon}
              </div>
              <div className="model-title-block">
                <div className="model-name">{m.name}</div>
                <div className="model-provider">{m.provider}</div>
              </div>
              <div className="model-role-badge" style={{ background: m.color + '18', color: m.color }}>
                {m.role}
              </div>
            </div>

            {/* Stages */}
            <div className="model-stages">
              {m.stages.map(s => (
                <span key={s} className="model-stage-tag">{s}</span>
              ))}
            </div>

            {/* What it does */}
            <ul className="model-what">
              {m.what.map((line, i) => (
                <li key={i}>{line}</li>
              ))}
            </ul>

            {/* I/O row */}
            <div className="model-io">
              <div className="model-io-item">
                <span className="model-io-label">Inputs</span>
                <span className="model-io-val">{m.inputs}</span>
              </div>
              <div className="model-io-item">
                <span className="model-io-label">Outputs</span>
                <span className="model-io-val">{m.outputs}</span>
              </div>
              <div className="model-io-item">
                <span className="model-io-label">Config</span>
                <code className="model-io-code">{m.env}</code>
              </div>
              <div className="model-io-item">
                <span className="model-io-label">Fallback</span>
                <span className="model-io-val" style={{ color: 'var(--muted)' }}>{m.fallback}</span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
