import { useEffect, useState } from 'react'
import { api, Stats } from '../api'

const SITE_COLORS: Record<string, string> = {
  linkedin: '#0a66c2', indeed: '#2164f3', RemoteOK: '#10b981',
  Greenhouse: '#3b82f6', Ashby: '#8b5cf6', Lever: '#f59e0b',
  Workday: '#06b6d4', BambooHR: '#ec4899', 'Hacker News Jobs': '#ff6600',
  WelcomeToTheJungle: '#f59e0b', Glassdoor: '#0caa41', Dice: '#eb1c26',
}

const siteColor = (site: string) =>
  SITE_COLORS[site] ?? `hsl(${site.charCodeAt(0) * 37 % 360},55%,55%)`

function StatCard({ num, label, cls }: { num: number; label: string; cls?: string }) {
  return (
    <div className={`stat-card ${cls ?? ''}`}>
      <div className="stat-num">{num.toLocaleString()}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

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

function PortalBreakdown({ bySite, scoreDist }: { bySite: [string, number][]; scoreDist: [number, number][] }) {
  // We only have total per site from stats; show total bar
  const max = Math.max(...bySite.map(([, c]) => c), 1)
  return (
    <div className="card">
      <div className="card-title">Jobs by Portal</div>
      {bySite.slice(0, 12).map(([site, total]) => {
        const color = siteColor(site)
        const pct = (total / max) * 100
        return (
          <div key={site} className="portal-row">
            <div className="portal-name" style={{ color }}>{site || 'Unknown'}</div>
            <div className="portal-stats">{total.toLocaleString()} jobs</div>
            <div className="portal-bar-track">
              <div className="portal-bar-seg" style={{ width: `${pct}%`, background: color + 'cc' }} />
            </div>
          </div>
        )
      })}
    </div>
  )
}

function PipelineFunnel({ s }: { s: Stats }) {
  const steps = [
    { label: 'Discovered', val: s.total, color: '#60a5fa' },
    { label: 'Enriched', val: s.with_description, color: '#818cf8' },
    { label: 'Scored', val: s.scored, color: '#a78bfa' },
    { label: 'Tailored (7+)', val: s.tailored, color: '#34d399' },
    { label: 'With Cover', val: s.with_cover_letter, color: '#10b981' },
    { label: 'Ready to Apply', val: s.ready_to_apply, color: '#f59e0b' },
    { label: 'Applied', val: s.applied, color: '#6366f1' },
  ]
  const max = s.total || 1
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
                style={{ width: `${Math.max((val / max) * 100, 0.5)}%`, background: color }}
              >
                {val > 0 && (val / max) > 0.06 ? val.toLocaleString() : ''}
              </div>
            </div>
            <div className="funnel-num" style={{ color }}>{val.toLocaleString()}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const load = () => {
    setLoading(true)
    api.stats()
      .then(setStats)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  if (loading) return <div className="loading-center"><div className="spinner" /> Loading stats…</div>
  if (error) return <div className="empty-state" style={{ color: 'var(--red)' }}>Error: {error}</div>
  if (!stats) return null

  const highFit = stats.score_distribution.filter(([s]) => s >= 7).reduce((a, [, c]) => a + c, 0)
  const pending7 = stats.untailored_eligible

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Dashboard</div>
        <div className="page-subtitle">
          {stats.total.toLocaleString()} jobs discovered &middot; last refreshed just now
          <button className="btn btn-ghost" style={{ marginLeft: '1rem', fontSize: '0.75rem' }} onClick={load}>
            ↻ Refresh
          </button>
        </div>
      </div>

      {/* Summary stats */}
      <div className="stat-grid">
        <StatCard num={stats.total} label="Total Discovered" />
        <StatCard num={stats.scored} label="Scored by LLM" cls="blue" />
        <StatCard num={highFit} label="Strong Fit (7+)" cls="green" />
        <StatCard num={stats.ready_to_apply} label="Ready to Apply" cls="yellow" />
        <StatCard num={stats.applied} label="Applied" cls="purple" />
        <StatCard num={pending7} label="Pending Tailoring (7+)" cls="yellow" />
        <StatCard num={stats.apply_errors} label="Apply Errors" cls="red" />
        <StatCard num={stats.detail_errors} label="Enrich Errors" />
      </div>

      {/* Score dist + funnel */}
      <div className="two-col">
        <ScoreDist dist={stats.score_distribution} />
        <PipelineFunnel s={stats} />
      </div>

      {/* Portal breakdown */}
      <PortalBreakdown bySite={stats.by_site} scoreDist={stats.score_distribution} />
    </div>
  )
}
