import { useEffect, useRef, useState } from 'react'
import { api, Job } from '../api'

// ── Types ─────────────────────────────────────────────────────────────────

interface SegmentJob {
  url: string
  title: string | null
  company: string | null
  fit_score: number | null
  embedding_score: number | null
  salary: string | null
  location: string | null
  site: string | null
  apply_status: string | null
  applied_at: string | null
  application_url: string | null
  optimizer_rank: number | null
}

interface Segment {
  label: string
  count: number
  jobs: SegmentJob[]
}

// ── Constants ─────────────────────────────────────────────────────────────

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
  { label: 'Scored', val: 'scored' },
  { label: 'Enriched', val: 'enriched' },
  { label: 'Failed', val: 'failed' },
]

// ── Helpers ───────────────────────────────────────────────────────────────

function scoreClass(score: number | null) {
  if (!score) return 'none'
  if (score >= 8) return 'high'
  if (score >= 7) return 'good'
  if (score >= 5) return 'mid'
  return 'low'
}

const fmtEmb = (v: number | null) => v != null ? v.toFixed(2) : null

// ── Job card (list view) ──────────────────────────────────────────────────

function JobCard({ job, onAction }: { job: Job & { embedding_score?: number | null; optimizer_rank?: number | null }; onAction: () => void }) {
  const [busy, setBusy] = useState(false)
  const sc = scoreClass(job.fit_score)
  const isApplied = !!job.applied_at

  const reasoningLines = (job.score_reasoning ?? '').split('\n')
  const kw = reasoningLines[0]?.slice(0, 130)
  const reasoning = reasoningLines[1]?.slice(0, 160)

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
        <span className={`score-pill ${sc}`}>{job.fit_score ?? '–'}</span>
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
        {(job as { embedding_score?: number | null }).embedding_score != null && (
          <span className="meta-tag" style={{ background: '#0c2a3a', color: '#06b6d4' }}>
            emb {fmtEmb((job as { embedding_score?: number | null }).embedding_score!)}
          </span>
        )}
        {isApplied && <span className="meta-tag meta-applied">Applied</span>}
        {job.tailored_resume_path && !isApplied && (
          <span className="meta-tag" style={{ background: '#1a2e1a', color: '#6ee7b7' }}>Tailored</span>
        )}
      </div>

      {kw && <div className="reasoning">{kw}</div>}
      {reasoning && <div className="reasoning-sub">{reasoning}</div>}

      <div className="card-footer">
        {job.application_url && (
          <a href={job.application_url} className="btn btn-primary" target="_blank" rel="noreferrer">Apply</a>
        )}
        {!isApplied && (
          <button className="btn btn-success" onClick={doMarkApplied} disabled={busy}>✓ Mark Applied</button>
        )}
        {!isApplied && (
          <button className="btn btn-danger" onClick={doMarkFailed} disabled={busy}>✗ Fail</button>
        )}
      </div>
    </div>
  )
}

// ── Segment job card (Netflix view) ──────────────────────────────────────

function SegCard({ job }: { job: SegmentJob }) {
  const sc = scoreClass(job.fit_score)
  const isApplied = !!job.applied_at

  return (
    <div className={`seg-card ${isApplied ? 'applied' : `score-${sc}`}`}>
      <div className="seg-card-score">
        <span className={`score-pill ${sc}`} style={{ fontSize: '0.72rem', minWidth: '1.5rem', height: '1.5rem' }}>
          {job.fit_score ?? '–'}
        </span>
        {job.optimizer_rank != null && job.optimizer_rank > 0 && (
          <span className="seg-rank">#{job.optimizer_rank}</span>
        )}
      </div>
      <div className="seg-card-title">
        {job.url
          ? <a href={job.url} target="_blank" rel="noreferrer" className="seg-title-link">{job.title ?? 'Untitled'}</a>
          : <span>{job.title ?? 'Untitled'}</span>
        }
      </div>
      {job.company && <div className="seg-card-company">{job.company}</div>}
      <div className="seg-card-meta">
        {job.salary && <span className="seg-meta-item" style={{ color: '#6ee7b7' }}>{job.salary}</span>}
        {job.location && <span className="seg-meta-item">{job.location.slice(0, 25)}</span>}
        {job.embedding_score != null && (
          <span className="seg-meta-item" style={{ color: '#06b6d4' }}>emb {job.embedding_score.toFixed(2)}</span>
        )}
      </div>
      <div className="seg-card-status">
        {isApplied && <span className="seg-status-pill applied">Applied</span>}
        {job.apply_status === 'failed' && <span className="seg-status-pill failed">Failed</span>}
        {!isApplied && job.apply_status == null && job.application_url && (
          <a href={job.application_url} target="_blank" rel="noreferrer" className="btn btn-primary"
            style={{ fontSize: '0.68rem', padding: '0.2rem 0.5rem' }}>Apply</a>
        )}
      </div>
    </div>
  )
}

// ── Segment row (Netflix row) ─────────────────────────────────────────────

function SegmentRow({ segment }: { segment: Segment }) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const SCROLL_AMOUNT = 660

  const scroll = (dir: -1 | 1) => {
    scrollRef.current?.scrollBy({ left: dir * SCROLL_AMOUNT, behavior: 'smooth' })
  }

  return (
    <div className="seg-row">
      <div className="seg-row-header">
        <span className="seg-company">{segment.label}</span>
        <span className="seg-count">{segment.count} job{segment.count !== 1 ? 's' : ''}</span>
      </div>
      <div className="seg-scroll-wrap">
        <button className="seg-arrow left" onClick={() => scroll(-1)}>‹</button>
        <div className="seg-scroll-track" ref={scrollRef}>
          {segment.jobs.map((job, i) => (
            <SegCard key={job.url ?? i} job={job} />
          ))}
        </div>
        <button className="seg-arrow right" onClick={() => scroll(1)}>›</button>
      </div>
    </div>
  )
}

// ── Segment view ──────────────────────────────────────────────────────────

function SegmentView({ minScore, strict }: { minScore: number; strict: boolean }) {
  const [segments, setSegments] = useState<Segment[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    fetch(`/api/jobs/by-segment?min_score=${minScore}&strict=${strict}&max_segments=25&limit_per=50`)
      .then(r => r.json())
      .then(d => { setSegments(d.segments ?? []); setLoading(false) })
      .catch(() => setLoading(false))
  }, [minScore, strict])

  if (loading) return <div className="loading-center"><div className="spinner" /> Loading industries…</div>
  if (segments.length === 0) return (
    <div className="empty-state">No industries found for these filters.</div>
  )

  return (
    <div className="segments-container">
      {segments.map(seg => (
        <SegmentRow key={seg.label} segment={seg} />
      ))}
    </div>
  )
}

// ── Main Jobs page ────────────────────────────────────────────────────────

export default function Jobs() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [sites, setSites] = useState<string[]>([])

  const [minScore, setMinScore] = useState(7)
  const [stage, setStage] = useState('all')
  const [site, setSite] = useState('')
  const [search, setSearch] = useState('')
  const [offset, setOffset] = useState(0)
  const [strict, setStrict] = useState(false)
  const [viewMode, setViewMode] = useState<'list' | 'segments'>('segments')
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
        {/* View mode toggle */}
        <div className="view-toggle">
          <button
            className={`view-btn ${viewMode === 'list' ? 'active' : ''}`}
            onClick={() => setViewMode('list')}
          >≡ List</button>
          <button
            className={`view-btn ${viewMode === 'segments' ? 'active' : ''}`}
            onClick={() => setViewMode('segments')}
          >⊞ Industries</button>
        </div>

        <span className="filter-label">Score:</span>
        {SCORE_STAGES.map(s => (
          <button key={s.val}
            className={`filter-btn ${minScore === s.val ? 'active' : ''}`}
            onClick={() => setMinScore(s.val)}
          >{s.label}</button>
        ))}

        {viewMode === 'list' && (
          <>
            <span className="filter-label" style={{ marginLeft: '0.5rem' }}>Stage:</span>
            <select className="select-input" value={stage} onChange={e => setStage(e.target.value)}>
              {STAGE_OPTS.map(o => <option key={o.val} value={o.val}>{o.label}</option>)}
            </select>
          </>
        )}

        {sites.length > 0 && viewMode === 'list' && (
          <>
            <span className="filter-label">Portal:</span>
            <select className="select-input" value={site} onChange={e => setSite(e.target.value)}>
              <option value="">All</option>
              {sites.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </>
        )}

        {viewMode === 'list' && (
          <>
            <span className="filter-label" style={{ marginLeft: '0.5rem' }}>Search:</span>
            <input className="search-input" placeholder="title, company, location…"
              value={search} onChange={e => setSearch(e.target.value)} />
          </>
        )}

        {/* Strict toggle */}
        <label className="strict-toggle" title="Only ML/DS/AI titled roles">
          <input type="checkbox" checked={strict} onChange={e => setStrict(e.target.checked)} />
          <span className="strict-track" />
          <span className="strict-label">Strict</span>
        </label>

        {viewMode === 'list' && (
          <button className="btn btn-ghost" onClick={() => load({ resetOffset: true })} style={{ marginLeft: 'auto' }}>↻</button>
        )}
      </div>

      {viewMode === 'segments' ? (
        <SegmentView minScore={minScore} strict={strict} />
      ) : loading ? (
        <div className="loading-center"><div className="spinner" /> Loading…</div>
      ) : (
        <>
          <div className="jobs-count">
            Showing {offset + 1}–{Math.min(offset + limit, total)} of {total.toLocaleString()}
            {strict && <span style={{ color: 'var(--yellow)', marginLeft: '0.5rem', fontSize: '0.75rem' }}>· Strict (ML/DS/AI titles only)</span>}
          </div>
          <div className="job-grid">
            {jobs
              .filter(j => !strict || isStrictMatch(j.title))
              .map(j => <JobCard key={j.url} job={j} onAction={() => load()} />)
            }
          </div>
          {total === 0 && <div className="empty-state">No jobs match these filters.</div>}
          {pages > 1 && (
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'center', marginTop: '1.5rem' }}>
              <button className="btn btn-ghost" disabled={page === 0}
                onClick={() => setOffset(Math.max(0, offset - limit))}>← Prev</button>
              <span style={{ alignSelf: 'center', color: 'var(--muted)', fontSize: '0.82rem' }}>
                Page {page + 1} / {pages}
              </span>
              <button className="btn btn-ghost" disabled={page >= pages - 1}
                onClick={() => setOffset(offset + limit)}>Next →</button>
            </div>
          )}
        </>
      )}
    </div>
  )
}

// ── Strict filter helper ──────────────────────────────────────────────────

const STRICT_KW = [
  'data scientist', 'machine learning', 'ml engineer', 'ai scientist',
  'applied scientist', 'research scientist', 'deep learning', 'nlp',
  'recommendation', 'computer vision', 'llm', 'generative', 'data science',
]

function isStrictMatch(title: string | null) {
  if (!title) return false
  const tl = title.toLowerCase()
  return STRICT_KW.some(kw => tl.includes(kw))
}
