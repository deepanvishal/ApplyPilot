import { useEffect, useState } from 'react'

interface ActiveJob {
  company: string
  title: string
  site: string
  elapsed_ms: number
  avg_ms: number
}

interface QueueStats {
  total: number
  by_score: { score: number; count: number }[]
  by_ats: { site: string; count: number }[]
  next_jobs: { company: string; title: string; site: string; score: number }[]
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
  queue: QueueStats
  estimated_seconds: number
}

const REASON_LABEL: Record<string, string> = {
  expired:               'Expired',
  no_result_line:        'No result',
  login_issue:           'Login issue',
  timed_out:             'Timed out',
  site_blocked:          'Site blocked',
  captcha:               'Captcha',
  not_a_job_application: 'Not a job',
  blocked_domain:        'Blocked domain',
  sso_required:          'SSO required',
  form_validation_loop:  'Form loop',
  not_eligible_work_auth:'Work auth',
  page_error:            'Page error',
  stuck:                 'Stuck',
  unknown:               'Unknown',
}

const ATS_COLOR: Record<string, string> = {
  workday:        '#4fc3f7',
  greenhouse:     '#66bb6a',
  ashby:          '#ab47bc',
  lever:          '#ffa726',
  smartrecruiters:'#ef5350',
  bamboohr:       '#26c6da',
  jobvite:        '#8d6e63',
  linkedin:       '#0288d1',
  direct:         '#90a4ae',
  indeed:         '#f57c00',
}

function siteColor(site: string) {
  return ATS_COLOR[site] ?? '#888'
}

function SiteBadge({ site }: { site: string }) {
  return (
    <span style={{
      fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 0.5,
      background: siteColor(site) + '22', color: siteColor(site),
      padding: '2px 6px', borderRadius: 4, flexShrink: 0,
    }}>{site}</span>
  )
}

function StatCard({ value, label, color }: { value: number; label: string; color: string }) {
  return (
    <div style={{
      background: '#1a1a2e', borderRadius: 16, padding: '16px 12px',
      flex: 1, minWidth: 0, textAlign: 'center',
      borderTop: `4px solid ${color}`,
    }}>
      <div style={{ fontSize: 38, fontWeight: 800, color, lineHeight: 1.1 }}>{value}</div>
      <div style={{ fontSize: 11, color: '#888', marginTop: 4, textTransform: 'uppercase', letterSpacing: 1 }}>{label}</div>
    </div>
  )
}

function Bar({ pct, color, height = 4 }: { pct: number; color: string; height?: number }) {
  return (
    <div style={{ height, background: '#2a2a3e', borderRadius: 2 }}>
      <div style={{ width: `${Math.min(pct, 1) * 100}%`, height: '100%', background: color, borderRadius: 2, transition: 'width 1s ease' }} />
    </div>
  )
}

function fmtTime(sec: number) {
  if (sec >= 3600) return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`
  if (sec >= 60)   return `${Math.floor(sec / 60)}m`
  return `${sec}s`
}

function fmtElapsed(ms: number) {
  const s = Math.floor(ms / 1000)
  return s >= 60 ? `${Math.floor(s / 60)}m ${s % 60}s` : `${s}s`
}

const CARD_STYLE = {
  background: '#1a1a2e', borderRadius: 16, padding: '16px 20px', marginBottom: 12,
}

const SECTION_LABEL = {
  fontSize: 11, color: '#555', textTransform: 'uppercase' as const, letterSpacing: 1, marginBottom: 10,
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

  const { today, all_time, session_start, as_of, active_jobs = [], queue, estimated_seconds } = data
  const reasons = Object.entries(today.failed_reasons).sort((a, b) => b[1] - a[1])
  const sessionLabel = session_start ? session_start.slice(11, 16) + ' UTC' : ''

  return (
    <div style={{ maxWidth: 480, margin: '0 auto', padding: '16px 12px', fontFamily: 'system-ui, sans-serif', color: '#eee' }}>

      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 700 }}>🚀 Current Session</div>
          {sessionLabel && <div style={{ fontSize: 11, color: '#555', marginTop: 2 }}>since {sessionLabel}</div>}
        </div>
        <div style={{ fontSize: 11, textAlign: 'right' }}>
          {today.in_progress > 0
            ? <span style={{ color: '#4fc' }}>● {today.in_progress} running</span>
            : <span style={{ color: '#555' }}>idle</span>}
        </div>
      </div>

      {/* Session stat cards */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
        <StatCard value={today.applied}         label="Applied"  color="#4fc3f7" />
        <StatCard value={today.failed}          label="Failed"   color="#ef5350" />
        <StatCard value={today.already_applied} label="Already"  color="#ffa726" />
      </div>

      {/* Active jobs */}
      {active_jobs.length > 0 && (
        <div style={CARD_STYLE}>
          <div style={SECTION_LABEL}>Applying now</div>
          {active_jobs.map((job, i) => {
            const pct = job.elapsed_ms / job.avg_ms
            const overdue = pct > 1
            const barColor = overdue ? '#ffa726' : siteColor(job.site)
            return (
              <div key={i} style={{ marginBottom: i < active_jobs.length - 1 ? 14 : 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
                  <SiteBadge site={job.site} />
                  <span style={{ fontSize: 13, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {job.company || job.title}
                  </span>
                  <span style={{ fontSize: 11, color: overdue ? '#ffa726' : '#666', flexShrink: 0 }}>
                    {fmtElapsed(job.elapsed_ms)}{overdue ? ' ⚠' : ''}
                  </span>
                </div>
                {job.company && (
                  <div style={{ fontSize: 11, color: '#666', marginBottom: 4, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {job.title}
                  </div>
                )}
                <Bar pct={pct} color={barColor} height={5} />
              </div>
            )
          })}
        </div>
      )}

      {/* Queue overview */}
      {queue && queue.total > 0 && (
        <div style={CARD_STYLE}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <div style={SECTION_LABEL}>Queue</div>
            <div style={{ fontSize: 12, color: '#aaa' }}>
              <span style={{ fontWeight: 700, color: '#eee' }}>{queue.total}</span> jobs
              {estimated_seconds > 0 && <span style={{ color: '#666' }}> · ETA ~{fmtTime(estimated_seconds)}</span>}
            </div>
          </div>

          {/* By score */}
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 11, color: '#444', marginBottom: 6 }}>Score</div>
            <div style={{ display: 'flex', gap: 8 }}>
              {queue.by_score.map(({ score, count }) => (
                <div key={score} style={{ textAlign: 'center', flex: 1 }}>
                  <div style={{ fontSize: 18, fontWeight: 700, color: score >= 9 ? '#4fc3f7' : score === 8 ? '#66bb6a' : '#888' }}>{count}</div>
                  <div style={{ fontSize: 10, color: '#555' }}>★{score}</div>
                </div>
              ))}
            </div>
          </div>

          {/* By ATS */}
          <div>
            <div style={{ fontSize: 11, color: '#444', marginBottom: 6 }}>ATS</div>
            {queue.by_ats.map(({ site, count }) => (
              <div key={site} style={{ marginBottom: 7 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                  <span style={{ fontSize: 12, color: siteColor(site) }}>{site}</span>
                  <span style={{ fontSize: 12, color: '#888' }}>{count}</span>
                </div>
                <Bar pct={count / queue.total} color={siteColor(site)} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Failure reasons */}
      {reasons.length > 0 && (
        <div style={CARD_STYLE}>
          <div style={SECTION_LABEL}>Failure reasons — session</div>
          {reasons.map(([reason, count]) => {
            const pct = today.failed > 0 ? count / today.failed : 0
            return (
              <div key={reason} style={{ marginBottom: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                  <span style={{ fontSize: 12 }}>{REASON_LABEL[reason] ?? reason}</span>
                  <span style={{ fontSize: 12, color: '#ef5350', fontWeight: 600 }}>{count}</span>
                </div>
                <Bar pct={pct} color="#ef5350" />
              </div>
            )
          })}
        </div>
      )}

      {/* Next jobs */}
      {queue && queue.next_jobs.length > 0 && (
        <div style={CARD_STYLE}>
          <div style={SECTION_LABEL}>Up next</div>
          {queue.next_jobs.map((job, i) => (
            <div key={i} style={{
              padding: '7px 0', borderBottom: i < queue.next_jobs.length - 1 ? '1px solid #1e1e32' : 'none',
              display: 'flex', alignItems: 'center', gap: 8,
            }}>
              <SiteBadge site={job.site} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 12, color: '#888', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {job.company || '—'}
                </div>
                <div style={{ fontSize: 13, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {job.title}
                </div>
              </div>
              <span style={{ fontSize: 12, color: job.score >= 9 ? '#4fc3f7' : job.score === 8 ? '#66bb6a' : '#888', flexShrink: 0 }}>
                ★{job.score}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* All-time totals */}
      <div style={CARD_STYLE}>
        <div style={SECTION_LABEL}>All time</div>
        {(['applied', 'already_applied', 'failed'] as const).map(status => (
          <div key={status} style={{ display: 'flex', justifyContent: 'space-between', padding: '5px 0', borderBottom: '1px solid #1e1e32' }}>
            <span style={{ fontSize: 12, color: '#aaa' }}>{status}</span>
            <span style={{ fontSize: 13, fontWeight: 600, color: status === 'applied' ? '#4fc3f7' : status === 'already_applied' ? '#ffa726' : '#ef5350' }}>
              {all_time[status] ?? 0}
            </span>
          </div>
        ))}
      </div>

      <div style={{ textAlign: 'center', fontSize: 11, color: '#333', marginTop: 4 }}>
        updated {as_of.slice(11, 19)} UTC · every 5s
      </div>
    </div>
  )
}
