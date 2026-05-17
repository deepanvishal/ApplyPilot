import { useEffect, useState } from 'react'

interface JobCard {
  company: string
  title: string
  site: string
  score: number
  location: string
  salary: string
  reasoning: string
}

interface NextJob extends JobCard {}
interface LastApplied extends JobCard { applied_at: string }
interface LastFailed  extends JobCard { apply_error: string; attempted_at: string }

interface ActiveJob {
  company: string
  title: string
  site: string
  elapsed_ms: number
  avg_ms: number
}

interface LiveData {
  today: {
    applied: number
    failed: number
    already_applied: number
    in_progress: number
    failed_reasons: Record<string, number>
  }
  all_time: Record<string, number>
  session_start: string
  as_of: string
  active_jobs: ActiveJob[]
  queue: {
    total: number
    by_score: { score: number; count: number }[]
    by_ats: { site: string; count: number }[]
    next_jobs: NextJob[]
  }
  last_applied: LastApplied[]
  last_failed: LastFailed[]
  estimated_seconds: number
}

const ATS_COLOR: Record<string, string> = {
  workday:         '#4fc3f7',
  greenhouse:      '#66bb6a',
  ashby:           '#ab47bc',
  lever:           '#ffa726',
  smartrecruiters: '#ef5350',
  bamboohr:        '#26c6da',
  jobvite:         '#8d6e63',
  linkedin:        '#0288d1',
  direct:          '#90a4ae',
  indeed:          '#f57c00',
}

const REASON_LABEL: Record<string, string> = {
  expired: 'Expired', no_result_line: 'No result', login_issue: 'Login issue',
  timed_out: 'Timed out', site_blocked: 'Site blocked', captcha: 'Captcha',
  not_a_job_application: 'Not a job', form_validation_loop: 'Form loop',
  not_eligible_work_auth: 'Work auth', page_error: 'Page error', stuck: 'Stuck',
}

function siteColor(site: string) { return ATS_COLOR[site] ?? '#888' }

function scoreColor(s: number) {
  if (s >= 10) return '#ffd54f'
  if (s >= 9)  return '#4fc3f7'
  if (s >= 8)  return '#66bb6a'
  return '#888'
}

function SiteBadge({ site }: { site: string }) {
  return (
    <span style={{
      fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 0.5,
      background: siteColor(site) + '22', color: siteColor(site),
      padding: '2px 5px', borderRadius: 4, flexShrink: 0,
    }}>{site}</span>
  )
}

function ScorePill({ score }: { score: number }) {
  return (
    <span style={{
      fontSize: 11, fontWeight: 800, color: scoreColor(score),
      background: scoreColor(score) + '22', borderRadius: 6,
      padding: '2px 7px', flexShrink: 0,
    }}>★{score}</span>
  )
}

function fmtElapsed(ms: number) {
  const s = Math.floor(ms / 1000)
  return s >= 60 ? `${Math.floor(s / 60)}m ${s % 60}s` : `${s}s`
}

function fmtTime(sec: number) {
  if (sec >= 3600) return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`
  if (sec >= 60)   return `${Math.floor(sec / 60)}m`
  return `${sec}s`
}

function fmtAgo(iso: string) {
  if (!iso) return ''
  try {
    const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
    if (diff < 60)   return `${diff}s ago`
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
    return `${Math.floor(diff / 3600)}h ago`
  } catch { return '' }
}

function Bar({ pct, color, height = 4 }: { pct: number; color: string; height?: number }) {
  return (
    <div style={{ height, background: '#2a2a3e', borderRadius: 2 }}>
      <div style={{ width: `${Math.min(pct, 1) * 100}%`, height: '100%', background: color, borderRadius: 2, transition: 'width 1s ease' }} />
    </div>
  )
}

function StatCard({ value, label, color }: { value: number; label: string; color: string }) {
  return (
    <div style={{
      background: '#1a1a2e', borderRadius: 12, padding: '12px 10px',
      flex: 1, minWidth: 0, textAlign: 'center', borderTop: `3px solid ${color}`,
    }}>
      <div style={{ fontSize: 32, fontWeight: 800, color, lineHeight: 1.1 }}>{value}</div>
      <div style={{ fontSize: 10, color: '#666', marginTop: 3, textTransform: 'uppercase', letterSpacing: 1 }}>{label}</div>
    </div>
  )
}

function KanbanCard({ job, badge }: { job: JobCard; badge?: React.ReactNode }) {
  return (
    <div style={{
      background: '#12122a', borderRadius: 12, padding: '12px 14px', marginBottom: 8,
      border: '1px solid #1e1e38',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 5, flexWrap: 'wrap' }}>
        <ScorePill score={job.score} />
        <SiteBadge site={job.site} />
        {badge && <span style={{ marginLeft: 'auto' }}>{badge}</span>}
      </div>
      <div style={{ fontSize: 13, fontWeight: 600, color: '#eee', marginBottom: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {job.title}
      </div>
      <div style={{ fontSize: 11, color: '#555', marginBottom: job.reasoning ? 5 : 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {[job.company, job.location].filter(Boolean).join(' · ')}
        {job.salary ? <span style={{ color: '#4caf50', marginLeft: 6 }}>{job.salary}</span> : null}
      </div>
      {job.reasoning && (
        <div style={{ fontSize: 11, color: '#444', lineHeight: 1.4, overflow: 'hidden', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}>
          {job.reasoning}
        </div>
      )}
    </div>
  )
}

function ActiveCard({ job }: { job: ActiveJob }) {
  const pct = job.elapsed_ms / job.avg_ms
  const overdue = pct > 1
  const color = overdue ? '#ffa726' : siteColor(job.site)
  return (
    <div style={{
      background: '#12122a', borderRadius: 12, padding: '12px 14px', marginBottom: 8,
      border: `1px solid ${color}44`,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 5 }}>
        <SiteBadge site={job.site} />
        <span style={{ fontSize: 11, color: overdue ? '#ffa726' : '#4fc', marginLeft: 'auto' }}>
          {fmtElapsed(job.elapsed_ms)}{overdue ? ' ⚠' : ''}
        </span>
      </div>
      <div style={{ fontSize: 13, fontWeight: 600, color: '#eee', marginBottom: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {job.title || job.company}
      </div>
      {job.company && job.title && (
        <div style={{ fontSize: 11, color: '#555', marginBottom: 5, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {job.company}
        </div>
      )}
      <Bar pct={pct} color={color} height={5} />
    </div>
  )
}

function KanbanColumn({ title, count, color, children, empty }: {
  title: string; count: number; color: string; children?: React.ReactNode; empty?: string
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10,
        paddingBottom: 8, borderBottom: `2px solid ${color}`,
      }}>
        <span style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color }}>{title}</span>
        <span style={{ fontSize: 11, background: color + '22', color, borderRadius: 10, padding: '1px 7px', fontWeight: 700 }}>{count}</span>
      </div>
      <div style={{ overflowY: 'auto', maxHeight: 'calc(100vh - 230px)' }}>
        {count === 0
          ? <div style={{ fontSize: 12, color: '#2a2a3e', textAlign: 'center', padding: '24px 0' }}>{empty ?? 'None'}</div>
          : children}
      </div>
    </div>
  )
}

export default function Live() {
  const [data, setData] = useState<LiveData | null>(null)
  const [error, setError] = useState(false)

  useEffect(() => {
    const fetchData = () => {
      fetch('/api/live', { cache: 'no-store' })
        .then(r => r.json())
        .then(d => { setData(d); setError(false) })
        .catch(() => setError(true))
    }
    fetchData()
    const id = setInterval(fetchData, 5000)
    const onVisible = () => { if (document.visibilityState === 'visible') fetchData() }
    document.addEventListener('visibilitychange', onVisible)
    return () => { clearInterval(id); document.removeEventListener('visibilitychange', onVisible) }
  }, [])

  if (error) return <div style={{ padding: 32, textAlign: 'center', color: '#e55' }}>Cannot reach server</div>
  if (!data)  return <div style={{ padding: 32, textAlign: 'center', color: '#888' }}>Loading...</div>

  const { today, all_time, session_start, as_of, active_jobs = [], queue, last_applied = [], last_failed = [], estimated_seconds } = data
  const sessionLabel = session_start ? session_start.slice(11, 16) + ' UTC' : ''

  return (
    <div style={{ padding: '16px 20px', fontFamily: 'system-ui, sans-serif', color: '#eee', minHeight: '100vh', background: '#0d0d1a' }}>

      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 18, fontWeight: 700 }}>Live Dashboard</span>
          {sessionLabel && <span style={{ fontSize: 11, color: '#333' }}>since {sessionLabel}</span>}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          {today.in_progress > 0
            ? <span style={{ fontSize: 12, color: '#4fc' }}>● {today.in_progress} running</span>
            : <span style={{ fontSize: 12, color: '#333' }}>idle</span>}
          {estimated_seconds > 0 && <span style={{ fontSize: 11, color: '#444' }}>ETA ~{fmtTime(estimated_seconds)}</span>}
          <span style={{ fontSize: 11, color: '#252535' }}>{as_of?.slice(11, 19)} UTC · 5s</span>
        </div>
      </div>

      {/* Stats */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <StatCard value={today.applied}            label="Applied"   color="#4fc3f7" />
        <StatCard value={today.failed}             label="Failed"    color="#ef5350" />
        <StatCard value={today.already_applied}    label="Already"   color="#ffa726" />
        <StatCard value={queue?.total ?? 0}        label="In Queue"  color="#ab47bc" />
        <StatCard value={all_time['applied'] ?? 0} label="All Time"  color="#444"    />
      </div>

      {/* Kanban */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16 }}>

        <KanbanColumn title="Up Next" count={queue?.next_jobs?.length ?? 0} color="#888" empty="Queue is empty">
          {queue?.next_jobs?.map((job, i) => <KanbanCard key={i} job={job} />)}
        </KanbanColumn>

        <KanbanColumn title="Applying" count={active_jobs.length} color="#4fc" empty="No active workers">
          {active_jobs.map((job, i) => <ActiveCard key={i} job={job} />)}
        </KanbanColumn>

        <KanbanColumn title="Applied" count={last_applied.length} color="#4fc3f7" empty="None yet">
          {last_applied.map((job, i) => (
            <KanbanCard key={i} job={job} badge={
              <span style={{ fontSize: 10, color: '#444' }}>{fmtAgo(job.applied_at)}</span>
            } />
          ))}
        </KanbanColumn>

        <KanbanColumn title="Failed" count={last_failed.length} color="#ef5350" empty="No failures">
          {last_failed.map((job, i) => (
            <KanbanCard key={i} job={job} badge={
              <span style={{ fontSize: 9, color: '#ef5350', background: '#ef535022', borderRadius: 4, padding: '2px 5px' }}>
                {job.apply_error}
              </span>
            } />
          ))}
        </KanbanColumn>

      </div>

      {/* Failure reasons */}
      {Object.keys(today.failed_reasons).length > 0 && (
        <div style={{ marginTop: 14, background: '#12122a', borderRadius: 12, padding: '10px 16px', border: '1px solid #1e1e38' }}>
          <span style={{ fontSize: 11, color: '#333', textTransform: 'uppercase', letterSpacing: 1, marginRight: 12 }}>Failures</span>
          {Object.entries(today.failed_reasons).sort((a, b) => b[1] - a[1]).map(([reason, count]) => (
            <span key={reason} style={{ fontSize: 11, background: '#1a1a2e', borderRadius: 6, padding: '3px 8px', color: '#ef5350', marginRight: 6 }}>
              {REASON_LABEL[reason] ?? reason} <strong>{count}</strong>
            </span>
          ))}
        </div>
      )}
    </div>
  )
}
