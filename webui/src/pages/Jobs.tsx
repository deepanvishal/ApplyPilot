import { useEffect, useRef, useState } from 'react'
import { api, Job } from '../api'

const SCORE_STAGES = [
  { label: 'All', val: 0 },
  { label: '5+', val: 5 },
  { label: '7+', val: 7 },
  { label: '8+', val: 8 },
  { label: '9+', val: 9 },
]

const STAGE_OPTS = [
  { label: 'All', val: 'all' },
  { label: 'Ready to Apply', val: 'ready' },
  { label: 'Applied', val: 'applied' },
  { label: 'Tailored', val: 'tailored' },
  { label: 'Scored', val: 'scored' },
  { label: 'Enriched', val: 'enriched' },
  { label: 'Failed', val: 'failed' },
]

function scoreClass(score: number | null) {
  if (!score) return 'none'
  if (score >= 8) return 'high'
  if (score >= 7) return 'good'
  if (score >= 5) return 'mid'
  return 'low'
}

function JobCard({ job, onAction }: { job: Job; onAction: () => void }) {
  const [busy, setBusy] = useState(false)
  const sc = scoreClass(job.fit_score)
  const isApplied = !!job.applied_at

  const reasoningLines = (job.score_reasoning ?? '').split('\n')
  const kw = reasoningLines[0]?.slice(0, 130)
  const reasoning = reasoningLines[1]?.slice(0, 180)

  const doMarkApplied = async () => {
    if (!confirm(`Mark as applied?\n${job.title}`)) return
    setBusy(true)
    try { await api.markApplied(job.url); onAction() } finally { setBusy(false) }
  }

  const doMarkFailed = async () => {
    const reason = prompt('Reason for failure (optional):') ?? undefined
    setBusy(true)
    try { await api.markFailed(job.url, reason); onAction() } finally { setBusy(false) }
  }

  return (
    <div className={`job-card ${isApplied ? 'applied' : `score-${sc}`}`}>
      <div className="card-header">
        <span className={`score-pill ${sc}`}>
          {job.fit_score ?? '–'}
        </span>
        {job.url
          ? <a href={job.url} className="job-title" target="_blank" rel="noreferrer">{job.title ?? 'Untitled'}</a>
          : <span className="job-title">{job.title ?? 'Untitled'}</span>
        }
      </div>

      <div className="meta-row">
        {job.site && <span className="meta-tag meta-site">{job.site}</span>}
        {job.company && <span className="meta-tag" style={{ background: '#1e293b', color: '#94a3b8' }}>{job.company}</span>}
        {job.salary && <span className="meta-tag meta-salary">{job.salary}</span>}
        {job.location && <span className="meta-tag meta-location">{job.location.slice(0, 40)}</span>}
        {isApplied && <span className="meta-tag meta-applied">Applied</span>}
        {job.tailored_resume_path && !isApplied && <span className="meta-tag" style={{ background: '#1a2e1a', color: '#6ee7b7' }}>Tailored</span>}
        {job.cover_letter_path && <span className="meta-tag" style={{ background: '#1a1e2e', color: '#93c5fd' }}>Cover</span>}
      </div>

      {kw && <div className="reasoning">{kw}</div>}
      {reasoning && <div className="reasoning-sub">{reasoning}</div>}

      <div className="card-footer">
        {job.application_url && (
          <a href={job.application_url} className="btn btn-primary" target="_blank" rel="noreferrer">
            Apply
          </a>
        )}
        {!isApplied && (
          <button className="btn btn-success" onClick={doMarkApplied} disabled={busy}>
            ✓ Mark Applied
          </button>
        )}
        {!isApplied && (
          <button className="btn btn-danger" onClick={doMarkFailed} disabled={busy}>
            ✗ Fail
          </button>
        )}
      </div>
    </div>
  )
}

export default function Jobs() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [sites, setSites] = useState<string[]>([])

  const [minScore, setMinScore] = useState(0)
  const [stage, setStage] = useState('all')
  const [site, setSite] = useState('')
  const [search, setSearch] = useState('')
  const [offset, setOffset] = useState(0)
  const limit = 100

  const searchTimer = useRef<ReturnType<typeof setTimeout>>()

  const load = (opts?: { resetOffset?: boolean }) => {
    const off = opts?.resetOffset ? 0 : offset
    if (opts?.resetOffset) setOffset(0)
    setLoading(true)
    api.jobs({ min_score: minScore, stage, site, search, limit, offset: off })
      .then(r => { setJobs(r.jobs); setTotal(r.total) })
      .catch(console.error)
      .finally(() => setLoading(false))
  }

  useEffect(() => { api.sites().then(setSites).catch(() => {}) }, [])
  useEffect(() => { load({ resetOffset: true }) }, [minScore, stage, site])
  useEffect(() => {
    clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => load({ resetOffset: true }), 350)
  }, [search])
  useEffect(() => { load() }, [offset])

  const pages = Math.ceil(total / limit)
  const page = Math.floor(offset / limit)

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Jobs</div>
        <div className="page-subtitle">{total.toLocaleString()} jobs match current filters</div>
      </div>

      <div className="filter-bar">
        <span className="filter-label">Score:</span>
        {SCORE_STAGES.map(s => (
          <button
            key={s.val}
            className={`filter-btn ${minScore === s.val ? 'active' : ''}`}
            onClick={() => setMinScore(s.val)}
          >{s.label}</button>
        ))}
        <span className="filter-label" style={{ marginLeft: '0.5rem' }}>Stage:</span>
        <select className="select-input" value={stage} onChange={e => setStage(e.target.value)}>
          {STAGE_OPTS.map(o => <option key={o.val} value={o.val}>{o.label}</option>)}
        </select>
        {sites.length > 0 && (
          <>
            <span className="filter-label">Portal:</span>
            <select className="select-input" value={site} onChange={e => setSite(e.target.value)}>
              <option value="">All</option>
              {sites.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </>
        )}
        <span className="filter-label" style={{ marginLeft: '0.5rem' }}>Search:</span>
        <input
          className="search-input"
          placeholder="title, company, location…"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <button className="btn btn-ghost" onClick={() => load({ resetOffset: true })} style={{ marginLeft: 'auto' }}>
          ↻
        </button>
      </div>

      {loading
        ? <div className="loading-center"><div className="spinner" /> Loading…</div>
        : (
          <>
            <div className="jobs-count">
              Showing {offset + 1}–{Math.min(offset + limit, total)} of {total.toLocaleString()}
            </div>
            <div className="job-grid">
              {jobs.map(j => (
                <JobCard key={j.url} job={j} onAction={() => load()} />
              ))}
            </div>
            {total === 0 && <div className="empty-state">No jobs match these filters.</div>}
            {pages > 1 && (
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'center', marginTop: '1.5rem' }}>
                <button className="btn btn-ghost" disabled={page === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>← Prev</button>
                <span style={{ alignSelf: 'center', color: 'var(--muted)', fontSize: '0.82rem' }}>
                  Page {page + 1} / {pages}
                </span>
                <button className="btn btn-ghost" disabled={page >= pages - 1} onClick={() => setOffset(offset + limit)}>Next →</button>
              </div>
            )}
          </>
        )
      }
    </div>
  )
}
