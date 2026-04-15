import { useState } from 'react'
import Cockpit from './pages/Cockpit'
import Dashboard from './pages/Dashboard'
import Jobs from './pages/Jobs'
import Models from './pages/Models'
import Profile from './pages/Profile'
import Signals from './pages/Signals'
import Doctor from './pages/Doctor'

type Page = 'cockpit' | 'dashboard' | 'jobs' | 'signals' | 'doctor' | 'profile' | 'models'

const NAV: { id: Page; label: string; icon: string; group?: string }[] = [
  { id: 'cockpit',   label: 'Cockpit',   icon: '🎛️',  group: 'main' },
  { id: 'dashboard', label: 'Dashboard', icon: '📊',  group: 'main' },
  { id: 'jobs',      label: 'Jobs',      icon: '💼',  group: 'main' },
  { id: 'models',    label: 'Models',    icon: '🧠',  group: 'main' },
  { id: 'signals',   label: 'Signals',   icon: '📡',  group: 'tools' },
  { id: 'doctor',    label: 'Doctor',    icon: '🩺',  group: 'tools' },
  { id: 'profile',   label: 'Profile',   icon: '👤',  group: 'tools' },
]

export default function App() {
  const [page, setPage] = useState<Page>('cockpit')

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar-logo">
          <span>🚀</span>
          <span className="text">ApplyPilot</span>
        </div>
        <nav>
          <div className="nav-group-label">Pipeline</div>
          {NAV.filter(n => n.group === 'main').map(n => (
            <button
              key={n.id}
              className={`nav-item ${page === n.id ? 'active' : ''}`}
              onClick={() => setPage(n.id)}
            >
              <span className="icon">{n.icon}</span>
              <span className="label">{n.label}</span>
            </button>
          ))}
          <div className="nav-group-label" style={{ marginTop: '0.75rem' }}>Tools</div>
          {NAV.filter(n => n.group === 'tools').map(n => (
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

      <main className={`main ${page === 'cockpit' ? 'main-cockpit' : ''}`}>
        {page === 'cockpit'   && <Cockpit />}
        {page === 'dashboard' && <Dashboard />}
        {page === 'jobs'      && <Jobs />}
        {page === 'models'    && <Models />}
        {page === 'signals'   && <Signals />}
        {page === 'doctor'    && <Doctor />}
        {page === 'profile'   && <Profile />}
      </main>
    </div>
  )
}
