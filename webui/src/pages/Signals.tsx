import { useEffect, useState } from 'react'
import { api, Signal, SignalsResponse } from '../api'

function SummaryCards({ s }: { s: SignalsResponse['summary'] }) {
  return (
    <div className="stat-grid" style={{ marginBottom: '1.5rem' }}>
      <div className="stat-card blue">
        <div className="stat-num">{s.total}</div>
        <div className="stat-label">Companies Tracked</div>
      </div>
      <div className="stat-card green">
        <div className="stat-num">{s.responded}</div>
        <div className="stat-label">Responded</div>
      </div>
      <div className="stat-card">
        <div className="stat-num">{s.no_response}</div>
        <div className="stat-label">No Response</div>
      </div>
      <div className="stat-card yellow">
        <div className="stat-num">{s.response_rate}%</div>
        <div className="stat-label">Response Rate</div>
      </div>
    </div>
  )
}

function LogOutcomeForm({ onDone }: { onDone: () => void }) {
  const [company, setCompany] = useState('')
  const [outcome, setOutcome] = useState<'responded' | 'no_response'>('responded')
  const [notes, setNotes] = useState('')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState('')

  const submit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!company.trim()) return
    setBusy(true)
    try {
      await api.logOutcome({ company: company.trim(), outcome, notes: notes.trim() || undefined })
      setMsg(`✓ Logged: ${company}`)
      setCompany(''); setNotes('')
      onDone()
    } catch (err) {
      setMsg(`Error: ${err}`)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="card" style={{ marginBottom: '1.5rem' }}>
      <div className="card-title">Log Company Outcome</div>
      <form onSubmit={submit}>
        <div className="form-row">
          <div className="form-group">
            <label className="form-label">Company Name</label>
            <input
              className="form-input"
              value={company}
              onChange={e => setCompany(e.target.value)}
              placeholder="e.g. Stripe"
              required
            />
          </div>
          <div className="form-group">
            <label className="form-label">Outcome</label>
            <select
              className="form-input select-input"
              value={outcome}
              onChange={e => setOutcome(e.target.value as 'responded' | 'no_response')}
            >
              <option value="responded">Responded</option>
              <option value="no_response">No Response</option>
            </select>
          </div>
          <div className="form-group">
            <label className="form-label">Notes (optional)</label>
            <input
              className="form-input"
              value={notes}
              onChange={e => setNotes(e.target.value)}
              placeholder="e.g. recruiter screen scheduled"
              style={{ minWidth: '220px' }}
            />
          </div>
          <div className="form-group" style={{ justifyContent: 'flex-end' }}>
            <button className="btn btn-primary" type="submit" disabled={busy || !company.trim()}>
              {busy ? 'Saving…' : 'Log'}
            </button>
          </div>
        </div>
        {msg && <div style={{ fontSize: '0.8rem', color: msg.startsWith('✓') ? 'var(--green)' : 'var(--red)', marginTop: '0.25rem' }}>{msg}</div>}
      </form>
    </div>
  )
}

export default function Signals() {
  const [data, setData] = useState<SignalsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [respondedOnly, setRespondedOnly] = useState(false)
  const [search, setSearch] = useState('')
  const [taskMsg, setTaskMsg] = useState('')

  const load = () => {
    setLoading(true)
    api.signals({ responded_only: respondedOnly })
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [respondedOnly])

  const filtered = (data?.signals ?? []).filter(s =>
    !search || s.company_name.toLowerCase().includes(search.toLowerCase())
  )

  const runTask = async (fn: () => Promise<{ task_id: string }>, label: string) => {
    setTaskMsg(`${label}…`)
    try {
      const { task_id } = await fn()
      setTaskMsg(`Started: ${task_id}`)
      setTimeout(() => setTaskMsg(''), 5000)
    } catch (e) {
      setTaskMsg(`Error: ${e}`)
    }
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Company Signals</div>
        <div className="page-subtitle">Track recruiter responses and company outcomes</div>
      </div>

      {data && <SummaryCards s={data.summary} />}

      <LogOutcomeForm onDone={load} />

      {/* Actions */}
      <div style={{ display: 'flex', gap: '0.75rem', marginBottom: '1.25rem', flexWrap: 'wrap', alignItems: 'center' }}>
        <button className="btn btn-ghost" onClick={() => runTask(api.syncOutcomes, 'Syncing Gmail')}>
          📬 Sync Gmail Outcomes
        </button>
        <button className="btn btn-ghost" onClick={() => runTask(api.buildSignals, 'Building signals')}>
          📊 Build Signals
        </button>
        {taskMsg && <span style={{ fontSize: '0.78rem', color: 'var(--muted)' }}>{taskMsg}</span>}
      </div>

      {/* Filters */}
      <div className="filter-bar" style={{ marginBottom: '1rem' }}>
        <button
          className={`filter-btn ${!respondedOnly ? 'active' : ''}`}
          onClick={() => setRespondedOnly(false)}
        >All</button>
        <button
          className={`filter-btn ${respondedOnly ? 'active' : ''}`}
          onClick={() => setRespondedOnly(true)}
        >Responded Only</button>
        <input
          className="search-input"
          placeholder="Search company…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{ marginLeft: 'auto' }}
        />
        <button className="btn btn-ghost" onClick={load}>↻</button>
      </div>

      {loading
        ? <div className="loading-center"><div className="spinner" /></div>
        : (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <table className="signals-table">
              <thead>
                <tr>
                  <th>Company</th>
                  <th>Tier</th>
                  <th>Industry</th>
                  <th>Responded</th>
                  <th>Notes</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map(s => (
                  <tr key={s.company_name}>
                    <td style={{ fontWeight: 600 }}>{s.company_name}</td>
                    <td>{s.tier ? <span className="badge badge-tier">{s.tier}</span> : <span className="tag-no">—</span>}</td>
                    <td style={{ color: 'var(--muted)', fontSize: '0.78rem' }}>{s.industry ?? '—'}</td>
                    <td>
                      {s.responded
                        ? <span className="badge badge-yes">YES</span>
                        : <span className="badge badge-no">no</span>
                      }
                    </td>
                    <td style={{ color: 'var(--muted)', fontSize: '0.78rem', maxWidth: 250 }}>{s.notes ?? ''}</td>
                    <td style={{ color: 'var(--muted)', fontSize: '0.75rem' }}>
                      {s.updated_at ? s.updated_at.split('T')[0] : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {filtered.length === 0 && (
              <div className="empty-state">No company signals yet. Log an outcome or run sync-outcomes.</div>
            )}
          </div>
        )
      }
    </div>
  )
}
