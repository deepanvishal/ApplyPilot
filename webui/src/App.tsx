import { useState } from 'react'
import Dashboard from './pages/Dashboard'
import Jobs from './pages/Jobs'
import Pipeline from './pages/Pipeline'
import Signals from './pages/Signals'
import Doctor from './pages/Doctor'

type Page = 'dashboard' | 'jobs' | 'pipeline' | 'signals' | 'doctor'

const NAV: { id: Page; label: string; icon: string }[] = [
  { id: 'dashboard', label: 'Dashboard', icon: '📊' },
  { id: 'jobs',      label: 'Jobs',      icon: '💼' },
  { id: 'pipeline',  label: 'Pipeline',  icon: '⚡' },
  { id: 'signals',   label: 'Signals',   icon: '📡' },
  { id: 'doctor',    label: 'Doctor',    icon: '🩺' },
]

export default function App() {
  const [page, setPage] = useState<Page>('dashboard')

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar-logo">
          <span>🚀</span>
          <span className="text">ApplyPilot</span>
        </div>
        <nav>
          {NAV.map(n => (
            <button
              key={n.id}
              className={`nav-item ${page === n.id ? 'active' : ''}`}
              onClick={() => setPage(n.id)}
            >
              <span className="icon">{n.icon}</span>
              <span className="label">{n.label}</span>
            </button>
          ))}
        </nav>
        <div className="sidebar-footer">ApplyPilot WebUI</div>
      </aside>

      <main className="main">
        {page === 'dashboard' && <Dashboard />}
        {page === 'jobs'      && <Jobs />}
        {page === 'pipeline'  && <Pipeline />}
        {page === 'signals'   && <Signals />}
        {page === 'doctor'    && <Doctor />}
      </main>
    </div>
  )
}
