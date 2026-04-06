import { useEffect, useState } from 'react'
import { api, DoctorResponse } from '../api'

export default function Doctor() {
  const [data, setData] = useState<DoctorResponse | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.doctor().then(setData).catch(console.error).finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading-center"><div className="spinner" /> Checking setup…</div>
  if (!data) return null

  const ok = data.checks.filter(c => c.ok).length
  const total = data.checks.length

  return (
    <div>
      <div className="page-header">
        <div className="page-title">System Check</div>
        <div className="page-subtitle">{ok}/{total} checks passing</div>
      </div>

      <div className="card" style={{ marginBottom: '1.25rem' }}>
        <div className="tier-badge">
          Tier {data.tier} — {data.tier_label}
        </div>
        <div style={{ marginTop: '0.75rem', fontSize: '0.82rem', color: 'var(--muted)' }}>
          {data.tier === 1 && 'Tier 2 unlocks: scoring, tailoring, cover letters (needs LLM API key)'}
          {data.tier === 2 && 'Tier 3 unlocks: auto-apply (needs Claude Code CLI + Chrome + Node.js)'}
          {data.tier === 3 && 'All features unlocked!'}
        </div>
      </div>

      <div className="card">
        {data.checks.map(c => (
          <div key={c.name} className="check-row">
            <div className="check-icon">
              {c.ok ? '✅' : '❌'}
            </div>
            <div className="check-name">{c.name}</div>
            <div className="check-note">{c.note}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
