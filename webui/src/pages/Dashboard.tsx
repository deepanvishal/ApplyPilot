import { useEffect, useState } from 'react'
import {
  api, Stats,
  AnalyticsApplyStatus, AnalyticsEmbedding,
  AnalyticsFailures, AnalyticsAllocation, AnalyticsLastRun,
} from '../api'

// ── Types for new analytics ───────────────────────────────────────────────

interface OptMix {
  total: number
  applied: number
  apply_rate: number
  score_distribution: [number, number][]
  status_distribution: { status: string; count: number }[]
  top_companies: { company: string; count: number; avg_score: number }[]
}

// ── Colors ────────────────────────────────────────────────────────────────

const SITE_COLORS: Record<string, string> = {
  linkedin: '#0a66c2', indeed: '#2164f3', RemoteOK: '#10b981',
  Greenhouse: '#3b82f6', Ashby: '#8b5cf6', Lever: '#f59e0b',
  Workday: '#06b6d4', BambooHR: '#ec4899', 'Hacker News Jobs': '#ff6600',
  WelcomeToTheJungle: '#f59e0b', Glassdoor: '#0caa41', Dice: '#eb1c26',
}
const siteColor = (site: string) =>
  SITE_COLORS[site] ?? `hsl(${site.charCodeAt(0) * 37 % 360},55%,55%)`

const STATUS_COLORS: Record<string, string> = {
  applied: '#6366f1', failed: '#ef4444', in_progress: '#f59e0b',
  queued: '#64748b', pending: '#475569',
}

// ── Primitives ────────────────────────────────────────────────────────────

function StatCard({ num, label, cls }: { num: number; label: string; cls?: string }) {
  return (
    <div className={`stat-card ${cls ?? ''}`}>
      <div className="stat-num">{num.toLocaleString()}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

function HBar({ pct, color, label, count }: { pct: number; color: string; label: string; count: number }) {
  return (
    <div className="hbar-row">
      <div className="hbar-label" title={label}>{label}</div>
      <div className="hbar-track">
        <div className="hbar-fill" style={{ width: `${Math.max(pct, 0.5)}%`, background: color }} />
      </div>
      <div className="hbar-count">{count.toLocaleString()}</div>
    </div>
  )
}

// ── Score distribution ────────────────────────────────────────────────────

function ScoreDist({ dist }: { dist: [number, number][] }) {
  const map = Object.fromEntries(dist)
  const max = Math.max(...dist.map(([, c]) => c), 1)
  return (
    <div className="card">
      <div className="card-title">Score Distribution</div>
      {[10, 9, 8, 7, 6, 5, 4, 3, 2, 1].map(s => {
        const count = map[s] ?? 0
        const pct = (count / max) * 100
        const color = s >= 8 ? '#10b981' : s >= 7 ? '#34d399' : s >= 5 ? '#f59e0b' : '#ef4444'
        return (
          <div key={s} className="score-row">
            <span className="score-label">{s}</span>
            <div className="score-bar-track">
              <div className="score-bar-fill" style={{ width: `${pct}%`, background: color }} />
            </div>
            <span className="score-count">{count || ''}</span>
          </div>
        )
      })}
    </div>
  )
}

// ── Portal breakdown — horizontal bars ───────────────────────────────────

function PortalBreakdown({ bySite }: { bySite: [string, number][] }) {
  const filtered = bySite.filter(([s]) => s && s.trim())
  const max = Math.max(...filtered.map(([, c]) => c), 1)
  return (
    <div className="card">
      <div className="card-title">Jobs by Portal</div>
      {filtered.slice(0, 15).map(([site, total]) => {
        const color = siteColor(site)
        return (
          <HBar
            key={site}
            label={site}
            count={total}
            pct={(total / max) * 100}
            color={color}
          />
        )
      })}
    </div>
  )
}

// ── Pipeline funnel (trimmed — no tailored/cover) ─────────────────────────

function PipelineFunnel({ s }: { s: Stats }) {
  const steps = [
    { label: 'Discovered', val: s.total, color: '#60a5fa' },
    { label: 'Enriched', val: s.with_description, color: '#818cf8' },
    { label: 'Scored', val: s.scored, color: '#a78bfa' },
    { label: 'Ready to Apply', val: s.ready_to_apply, color: '#f59e0b' },
    { label: 'Applied', val: s.applied, color: '#6366f1' },
  ]
  const maxVal = s.total || 1
  return (
    <div className="card">
      <div className="card-title">Pipeline Funnel</div>
      <div className="funnel">
        {steps.map(({ label, val, color }) => (
          <div key={label} className="funnel-row">
            <div className="funnel-label">{label}</div>
            <div className="funnel-track">
              <div
                className="funnel-fill"
                style={{ width: `${Math.max((val / maxVal) * 100, 0.5)}%`, background: color }}
              >
                {val > 0 && (val / maxVal) > 0.06 ? val.toLocaleString() : ''}
              </div>
            </div>
            <div className="funnel-num" style={{ color }}>{val.toLocaleString()}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Apply status chart ────────────────────────────────────────────────────

function ApplyStatusChart({ data }: { data: AnalyticsApplyStatus }) {
  const byStatus: Record<string, number> = {}
  for (const row of data.data) {
    byStatus[row.status] = (byStatus[row.status] ?? 0) + row.count
  }
  const scoreBuckets: Record<string, number> = { '9–10': 0, '7–8': 0, '5–6': 0, '<5': 0 }
  for (const row of data.data) {
    if (row.status !== 'applied') continue
    const s = row.fit_score ?? 0
    const b = s >= 9 ? '9–10' : s >= 7 ? '7–8' : s >= 5 ? '5–6' : '<5'
    scoreBuckets[b] += row.count
  }
  const total = Object.values(byStatus).reduce((a, b) => a + b, 0) || 1
  const maxB = Math.max(...Object.values(scoreBuckets), 1)
  return (
    <div className="card">
      <div className="card-title">Results by Apply Status</div>
      <div className="two-mini-col">
        <div>
          <div className="mini-label">By Status</div>
          {Object.entries(byStatus).map(([status, count]) => (
            <HBar key={status} label={status} count={count} pct={(count / total) * 100}
              color={STATUS_COLORS[status] ?? '#64748b'} />
          ))}
        </div>
        <div>
          <div className="mini-label">Applied — Score Buckets</div>
          {Object.entries(scoreBuckets).map(([b, count]) => (
            <HBar key={b} label={b} count={count} pct={maxB > 0 ? (count / maxB) * 100 : 0}
              color={b === '9–10' ? '#10b981' : b === '7–8' ? '#34d399' : b === '5–6' ? '#f59e0b' : '#ef4444'} />
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Embedding distribution ────────────────────────────────────────────────

function EmbeddingDist({ data, titleFilter, onTitleFilter }: {
  data: AnalyticsEmbedding; titleFilter: string; onTitleFilter: (t: string) => void
}) {
  const bins = Array.from({ length: 10 }, (_, i) => ({
    range: `${(i * 0.1).toFixed(1)}`,
    count: 0,
  }))
  for (const s of data.scores) {
    const idx = Math.min(Math.floor(s * 10), 9)
    bins[idx].count++
  }
  const max = Math.max(...bins.map(b => b.count), 1)
  return (
    <div className="card">
      <div className="card-title" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span>Embedding Score Distribution</span>
        <select className="select-input" style={{ fontSize: '0.72rem', padding: '0.2rem 0.5rem' }}
          value={titleFilter} onChange={e => onTitleFilter(e.target.value)}>
          <option value="">All titles</option>
          {data.top_titles.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>
      <div style={{ marginBottom: '0.5rem', color: 'var(--muted)', fontSize: '0.75rem' }}>
        {data.scores.length.toLocaleString()} jobs{titleFilter && ` · ${titleFilter}`}
      </div>
      <div className="emb-bins">
        {bins.map(b => (
          <div key={b.range} className="emb-bin">
            <div className="emb-bin-fill"
              style={{ height: `${Math.max((b.count / max) * 80, b.count > 0 ? 4 : 0)}px`, background: '#06b6d4' }} />
            <div className="emb-bin-label">{b.range}</div>
            {b.count > 0 && <div className="emb-bin-count">{b.count}</div>}
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Optimization mix ──────────────────────────────────────────────────────

function OptimizationMix({ data }: { data: OptMix }) {
  if (data.total === 0) return (
    <div className="card">
      <div className="card-title">Optimization Mix</div>
      <div style={{ color: 'var(--muted)', fontSize: '0.82rem' }}>No ranked jobs — run Optimize in Stage 5.</div>
    </div>
  )
  const maxStatus = Math.max(...data.status_distribution.map(s => s.count), 1)
  const maxCompany = Math.max(...data.top_companies.map(c => c.count), 1)
  const scoreMap = Object.fromEntries(data.score_distribution)
  const scoreMax = Math.max(...data.score_distribution.map(([, c]) => c), 1)

  return (
    <div className="card">
      <div className="card-title">Optimization Mix — Queue Logic</div>

      {/* Summary row */}
      <div className="opt-mix-summary">
        <div className="oms-item">
          <div className="oms-num">{data.total.toLocaleString()}</div>
          <div className="oms-label">In Queue</div>
        </div>
        <div className="oms-item" style={{ color: 'var(--green)' }}>
          <div className="oms-num">{data.applied.toLocaleString()}</div>
          <div className="oms-label">Applied</div>
        </div>
        <div className="oms-item" style={{ color: 'var(--blue)' }}>
          <div className="oms-num">{data.apply_rate}%</div>
          <div className="oms-label">Apply Rate</div>
        </div>
        <div className="oms-item" style={{ color: 'var(--yellow)' }}>
          <div className="oms-num">{data.total - data.applied}</div>
          <div className="oms-label">Remaining</div>
        </div>
      </div>

      <div className="opt-mix-grid">
        {/* Status breakdown */}
        <div>
          <div className="mini-label">Queue Status</div>
          {data.status_distribution.map(s => (
            <HBar key={s.status} label={s.status} count={s.count}
              pct={(s.count / maxStatus) * 100} color={STATUS_COLORS[s.status] ?? '#64748b'} />
          ))}
        </div>

        {/* Score distribution within queue */}
        <div>
          <div className="mini-label">Score Mix in Queue</div>
          {[10, 9, 8, 7, 6, 5].map(s => {
            const count = scoreMap[s] ?? 0
            const color = s >= 8 ? '#10b981' : s >= 7 ? '#34d399' : '#f59e0b'
            return (
              <div key={s} className="score-row">
                <span className="score-label">{s}</span>
                <div className="score-bar-track">
                  <div className="score-bar-fill" style={{ width: `${scoreMax > 0 ? (count / scoreMax) * 100 : 0}%`, background: color }} />
                </div>
                <span className="score-count">{count || ''}</span>
              </div>
            )
          })}
        </div>

        {/* Top companies */}
        <div>
          <div className="mini-label">Top Companies in Queue</div>
          {data.top_companies.slice(0, 8).map(c => (
            <HBar key={c.company} label={c.company} count={c.count}
              pct={(c.count / maxCompany) * 100} color="#a78bfa" />
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Allocation queue ──────────────────────────────────────────────────────

function AllocationChart({ data }: { data: AnalyticsAllocation }) {
  if (data.queue.length === 0) return (
    <div className="card">
      <div className="card-title">Allocation Queue</div>
      <div style={{ color: 'var(--muted)', fontSize: '0.82rem' }}>No ranked jobs — run Optimize in Stage 5.</div>
    </div>
  )
  return (
    <div className="card">
      <div className="card-title">Allocation Queue — top 50 (responded companies excluded)</div>
      <div className="alloc-table-wrap">
        <table className="alloc-table">
          <thead>
            <tr><th>Rank</th><th>Company</th><th>Title</th><th>Fit</th><th>Emb</th><th>Status</th></tr>
          </thead>
          <tbody>
            {data.queue.map((row, i) => (
              <tr key={i}>
                <td style={{ color: 'var(--muted)', fontSize: '0.75rem' }}>#{row.optimizer_rank}</td>
                <td style={{ fontWeight: 600, fontSize: '0.82rem' }}>{row.company}</td>
                <td style={{ color: 'var(--muted)', fontSize: '0.78rem', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{row.title}</td>
                <td>
                  <span className="score-pill" style={{
                    background: (row.fit_score ?? 0) >= 8 ? '#10b981' : (row.fit_score ?? 0) >= 7 ? '#34d399' : '#f59e0b',
                    color: '#0f172a', minWidth: '1.5rem', height: '1.4rem',
                  }}>{row.fit_score ?? '—'}</span>
                </td>
                <td style={{ color: 'var(--muted)', fontSize: '0.75rem' }}>
                  {row.embedding_score != null ? row.embedding_score.toFixed(2) : '—'}
                </td>
                <td>
                  <span className="badge" style={{
                    background: (STATUS_COLORS[row.apply_status] ?? '#64748b') + '22',
                    color: STATUS_COLORS[row.apply_status] ?? '#64748b',
                  }}>{row.apply_status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Failure reasons ───────────────────────────────────────────────────────

function FailureReasons({ data }: { data: AnalyticsFailures }) {
  if (data.total === 0) return (
    <div className="card">
      <div className="card-title">Most Common Failure Reasons</div>
      <div style={{ color: 'var(--muted)', fontSize: '0.82rem' }}>No failures recorded.</div>
    </div>
  )
  const max = Math.max(...data.failures.map(f => f.count), 1)
  return (
    <div className="card">
      <div className="card-title">
        Most Common Failure Reasons
        <span style={{ color: 'var(--red)', fontWeight: 400, marginLeft: '0.5rem' }}>
          ({data.total.toLocaleString()} total)
        </span>
      </div>
      {data.failures.map(f => (
        <HBar key={f.reason} label={f.reason.slice(0, 65)} count={f.count}
          pct={(f.count / max) * 100} color="#ef4444" />
      ))}
    </div>
  )
}

// ── Last run summary ──────────────────────────────────────────────────────

function LastRunSummary({ data }: { data: AnalyticsLastRun }) {
  if (data.total === 0) return (
    <div className="card">
      <div className="card-title">Last Run Summary (24h)</div>
      <div style={{ color: 'var(--muted)', fontSize: '0.82rem' }}>No applications in the last 24 hours.</div>
    </div>
  )
  const maxC = Math.max(...data.by_company.map(r => r.count), 1)
  const maxT = Math.max(...data.by_title.map(r => r.count), 1)
  return (
    <div className="card">
      <div className="card-title">Last Run Summary (24h)</div>
      <div className="last-run-totals">
        <div className="lrt-item"><div className="lrt-num">{data.total}</div><div className="lrt-label">Total</div></div>
        <div className="lrt-item" style={{ color: 'var(--green)' }}><div className="lrt-num">{data.success}</div><div className="lrt-label">Applied</div></div>
        <div className="lrt-item" style={{ color: 'var(--red)' }}><div className="lrt-num">{data.failed}</div><div className="lrt-label">Failed</div></div>
        <div className="lrt-item" style={{ color: 'var(--blue)' }}>
          <div className="lrt-num">{data.total > 0 ? Math.round((data.success / data.total) * 100) : 0}%</div>
          <div className="lrt-label">Success Rate</div>
        </div>
      </div>
      <div className="two-mini-col" style={{ marginTop: '1rem' }}>
        <div>
          <div className="mini-label">By Company</div>
          {data.by_company.slice(0, 8).map(r => (
            <HBar key={r.company} label={r.company} count={r.count} pct={(r.count / maxC) * 100} color="#6366f1" />
          ))}
        </div>
        <div>
          <div className="mini-label">By Title</div>
          {data.by_title.slice(0, 8).map(r => (
            <HBar key={r.title} label={r.title.slice(0, 30)} count={r.count} pct={(r.count / maxT) * 100} color="#a78bfa" />
          ))}
        </div>
      </div>
      {data.failures.length > 0 && (
        <div style={{ marginTop: '1rem' }}>
          <div className="mini-label">Failure Reasons</div>
          {data.failures.map(f => (
            <div key={f.reason} className="failure-row">
              <span className="failure-reason">{f.reason.slice(0, 60)}</span>
              <span className="failure-count">{f.count}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Main Dashboard ─────────────────────────────────────────────────────────

export default function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [applyStatus, setApplyStatus] = useState<AnalyticsApplyStatus | null>(null)
  const [embedding, setEmbedding] = useState<AnalyticsEmbedding | null>(null)
  const [embTitleFilter, setEmbTitleFilter] = useState('')
  const [failures, setFailures] = useState<AnalyticsFailures | null>(null)
  const [allocation, setAllocation] = useState<AnalyticsAllocation | null>(null)
  const [lastRun, setLastRun] = useState<AnalyticsLastRun | null>(null)
  const [optMix, setOptMix] = useState<OptMix | null>(null)

  const load = () => {
    setLoading(true)
    Promise.all([
      api.stats(),
      api.analyticsApplyStatus(),
      api.analyticsEmbedding(),
      api.analyticsFailures(),
      api.analyticsAllocation(),
      api.analyticsLastRun(),
      fetch('/api/analytics/optimization-mix').then(r => r.json()),
    ])
      .then(([s, as_, emb, fail, alloc, lr, mix]) => {
        setStats(s as Stats)
        setApplyStatus(as_ as AnalyticsApplyStatus)
        setEmbedding(emb as AnalyticsEmbedding)
        setFailures(fail as AnalyticsFailures)
        setAllocation(alloc as AnalyticsAllocation)
        setLastRun(lr as AnalyticsLastRun)
        setOptMix(mix as OptMix)
        setLoading(false)
      })
      .catch(e => { setError(String(e)); setLoading(false) })
  }

  useEffect(() => { load() }, [])

  useEffect(() => {
    if (!stats) return
    api.analyticsEmbedding(embTitleFilter || undefined).then(setEmbedding)
  }, [embTitleFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) return <div className="loading-center"><div className="spinner" /> Loading dashboard…</div>
  if (error) return <div className="empty-state" style={{ color: 'var(--red)' }}>Error: {error}</div>
  if (!stats) return null

  const highFit = stats.score_distribution.filter(([s]) => s >= 7).reduce((a, [, c]) => a + c, 0)

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Dashboard</div>
        <div className="page-subtitle">
          {stats.total.toLocaleString()} jobs discovered
          <button className="btn btn-ghost" style={{ marginLeft: '1rem', fontSize: '0.75rem' }} onClick={load}>
            ↻ Refresh
          </button>
        </div>
      </div>

      {/* Summary stats — no tailored/cover */}
      <div className="stat-grid">
        <StatCard num={stats.total} label="Total Discovered" />
        <StatCard num={stats.scored} label="Scored by LLM" cls="blue" />
        <StatCard num={highFit} label="Strong Fit (7+)" cls="green" />
        <StatCard num={stats.ready_to_apply} label="Ready to Apply" cls="yellow" />
        <StatCard num={stats.applied} label="Applied" cls="purple" />
        <StatCard num={stats.apply_errors} label="Apply Errors" cls="red" />
        <StatCard num={stats.detail_errors} label="Enrich Errors" />
      </div>

      {/* Score dist + funnel */}
      <div className="two-col">
        <ScoreDist dist={stats.score_distribution} />
        <PipelineFunnel s={stats} />
      </div>

      {/* Portal breakdown — horizontal bars */}
      <div style={{ marginBottom: '1.5rem' }}>
        <PortalBreakdown bySite={stats.by_site} />
      </div>

      <div className="section-divider"><span>Analytics</span></div>

      {/* Last run */}
      {lastRun && <div style={{ marginBottom: '1.5rem' }}><LastRunSummary data={lastRun} /></div>}

      {/* Apply status + failures */}
      <div className="two-col">
        {applyStatus && <ApplyStatusChart data={applyStatus} />}
        {failures && <FailureReasons data={failures} />}
      </div>

      {/* Embedding distribution */}
      {embedding && (
        <div style={{ marginBottom: '1.5rem' }}>
          <EmbeddingDist data={embedding} titleFilter={embTitleFilter} onTitleFilter={setEmbTitleFilter} />
        </div>
      )}

      <div className="section-divider"><span>Optimization</span></div>

      {/* Optimization mix */}
      {optMix && <div style={{ marginBottom: '1.5rem' }}><OptimizationMix data={optMix} /></div>}

      {/* Allocation queue */}
      {allocation && <div style={{ marginBottom: '1.5rem' }}><AllocationChart data={allocation} /></div>}
    </div>
  )
}
