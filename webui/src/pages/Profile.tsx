import { useEffect, useState } from 'react'
import { api } from '../api'

type ProfileData = Record<string, unknown>

const SECTION_LABELS: Record<string, string> = {
  personal: 'Personal Info',
  work_authorization: 'Work Authorization',
  availability: 'Availability',
  compensation: 'Compensation',
  experience: 'Experience & Role',
  skills_boundary: 'Skills Boundary',
  resume_facts: 'Resume Facts',
  disability: 'Disability & Demographics',
  preferences: 'Preferences',
}

const SECTION_ICONS: Record<string, string> = {
  personal: '👤',
  work_authorization: '📋',
  availability: '📅',
  compensation: '💰',
  experience: '🎯',
  skills_boundary: '🛠️',
  resume_facts: '📄',
  disability: '♿',
  preferences: '⚙️',
}

// ── Field renderer ──────────────────────────────────────────────────────────

function FieldValue({ val, depth = 0 }: { val: unknown; depth?: number }) {
  if (val === null || val === undefined) {
    return <span style={{ color: 'var(--muted)' }}>—</span>
  }
  if (Array.isArray(val)) {
    if (val.length === 0) return <span style={{ color: 'var(--muted)' }}>empty</span>
    return (
      <div className="profile-array">
        {(val as unknown[]).map((item, i) => (
          <span key={i} className="profile-tag">{String(item)}</span>
        ))}
      </div>
    )
  }
  if (typeof val === 'object') {
    return (
      <div className={depth > 0 ? 'profile-nested' : ''}>
        {Object.entries(val as Record<string, unknown>).map(([k, v]) => (
          <div key={k} className="profile-subrow">
            <span className="profile-subkey">{k.replace(/_/g, ' ')}</span>
            <FieldValue val={v} depth={depth + 1} />
          </div>
        ))}
      </div>
    )
  }
  const s = String(val)
  // Mask passwords
  if (s && (s.includes('password') || s.toLowerCase().includes('pass'))) {
    return <span className="profile-value">••••••</span>
  }
  return <span className="profile-value">{s || <span style={{ color: 'var(--muted)' }}>—</span>}</span>
}

// ── Section card ──────────────────────────────────────────────────────────

function SectionCard({
  sectionKey,
  data,
  onEdit,
}: {
  sectionKey: string
  data: Record<string, unknown>
  onEdit: () => void
}) {
  const label = SECTION_LABELS[sectionKey] ?? sectionKey.replace(/_/g, ' ')
  const icon = SECTION_ICONS[sectionKey] ?? '📝'

  return (
    <div className="profile-card">
      <div className="profile-card-header">
        <div className="profile-card-title">
          <span>{icon}</span> {label}
        </div>
        <button className="btn btn-ghost" style={{ fontSize: '0.72rem' }} onClick={onEdit}>
          Edit
        </button>
      </div>
      <div className="profile-fields">
        {Object.entries(data).map(([k, v]) => {
          // Skip password fields from display
          if (k.toLowerCase().includes('password')) return null
          return (
            <div key={k} className="profile-field-row">
              <span className="profile-field-key">{k.replace(/_/g, ' ')}</span>
              <div className="profile-field-val">
                <FieldValue val={v} />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Edit modal ────────────────────────────────────────────────────────────

function EditModal({
  sectionKey,
  data,
  onSave,
  onClose,
}: {
  sectionKey: string
  data: Record<string, unknown>
  onSave: (updated: Record<string, unknown>) => void
  onClose: () => void
}) {
  const [raw, setRaw] = useState(JSON.stringify(data, null, 2))
  const [error, setError] = useState('')

  const save = () => {
    try {
      const parsed = JSON.parse(raw)
      setError('')
      onSave(parsed)
    } catch (e) {
      setError(String(e))
    }
  }

  const label = SECTION_LABELS[sectionKey] ?? sectionKey

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span className="modal-title">{SECTION_ICONS[sectionKey] ?? '📝'} Edit — {label}</span>
          <button className="btn btn-ghost" style={{ fontSize: '0.75rem' }} onClick={onClose}>✕</button>
        </div>
        <textarea
          className="json-editor"
          value={raw}
          onChange={e => setRaw(e.target.value)}
          spellCheck={false}
        />
        {error && <div style={{ color: 'var(--red)', fontSize: '0.75rem', padding: '0.25rem 0' }}>{error}</div>}
        <div className="modal-footer">
          <button className="btn btn-ghost" onClick={onClose}>Cancel</button>
          <button className="btn btn-primary" onClick={save}>Save</button>
        </div>
      </div>
    </div>
  )
}

// ── Main Profile page ─────────────────────────────────────────────────────

export default function Profile() {
  const [profile, setProfile] = useState<ProfileData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [editing, setEditing] = useState<string | null>(null)

  const load = () => {
    setLoading(true)
    api.getProfile()
      .then(p => { setProfile(p as ProfileData); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }

  useEffect(() => { load() }, [])

  const handleSectionSave = async (sectionKey: string, updated: Record<string, unknown>) => {
    if (!profile) return
    const next = { ...profile, [sectionKey]: updated }
    setSaving(true)
    try {
      await api.saveProfile(next)
      setProfile(next)
      setSaved(true)
      setEditing(null)
      setTimeout(() => setSaved(false), 2500)
    } catch (e) {
      setError(String(e))
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <div className="loading-center"><div className="spinner" /> Loading profile…</div>
  if (error) return <div className="empty-state" style={{ color: 'var(--red)' }}>Error: {error}</div>
  if (!profile || Object.keys(profile).length === 0) {
    return (
      <div className="empty-state">
        <div style={{ fontSize: '2rem', marginBottom: '0.75rem' }}>👤</div>
        <div>No profile found. Run <code style={{ background: 'var(--border)', padding: '0.1rem 0.4rem', borderRadius: '4px' }}>applypilot init</code> to set up your profile.</div>
      </div>
    )
  }

  // Summary row from personal section
  const personal = profile.personal as Record<string, string> | undefined
  const experience = profile.experience as Record<string, string> | undefined

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Profile</div>
        <div className="page-subtitle">
          {personal?.full_name && <strong>{personal.full_name}</strong>}
          {experience?.target_role && <> &middot; {experience.target_role}</>}
          {experience?.years_of_experience_total && <> &middot; {experience.years_of_experience_total}y exp</>}
          {saved && <span style={{ color: 'var(--green)', marginLeft: '1rem', fontSize: '0.82rem' }}>✓ Saved</span>}
          {saving && <span style={{ color: 'var(--yellow)', marginLeft: '1rem', fontSize: '0.82rem' }}>Saving…</span>}
        </div>
      </div>

      {/* Quick summary strip */}
      {personal && (
        <div className="profile-summary-strip">
          <div className="profile-summary-item">
            <span className="ps-label">Email</span>
            <span className="ps-value">{personal.email || '—'}</span>
          </div>
          <div className="profile-summary-item">
            <span className="ps-label">Phone</span>
            <span className="ps-value">{personal.phone || '—'}</span>
          </div>
          {profile.work_authorization != null && (
            <div className="profile-summary-item">
              <span className="ps-label">Work Auth</span>
              <span className="ps-value">{String((profile.work_authorization as Record<string, string>).work_permit_type ?? '—')}</span>
            </div>
          )}
          {profile.compensation != null && (
            <div className="profile-summary-item">
              <span className="ps-label">Salary Target</span>
              <span className="ps-value">
                ${Number((profile.compensation as Record<string, string>).salary_expectation || 0).toLocaleString()}
              </span>
            </div>
          )}
          {personal.linkedin_url && (
            <div className="profile-summary-item">
              <span className="ps-label">LinkedIn</span>
              <a href={personal.linkedin_url} target="_blank" rel="noreferrer" className="ps-link">
                View →
              </a>
            </div>
          )}
        </div>
      )}

      {/* Section cards */}
      <div className="profile-grid">
        {Object.entries(profile).map(([key, val]) => {
          if (typeof val !== 'object' || val === null || Array.isArray(val)) return null
          return (
            <SectionCard
              key={key}
              sectionKey={key}
              data={val as Record<string, unknown>}
              onEdit={() => setEditing(key)}
            />
          )
        })}
      </div>

      {/* Edit modal */}
      {editing != null && typeof profile[editing] === 'object' && profile[editing] !== null && (
        <EditModal
          sectionKey={editing}
          data={profile[editing] as Record<string, unknown>}
          onSave={updated => handleSectionSave(editing, updated)}
          onClose={() => setEditing(null)}
        />
      )}
    </div>
  )
}
