import { useEffect, useRef, useState } from 'react'
import { api } from '../api'

// ── Task log component ──────────────────────────────────────────────────────

function TaskLog({ taskId, onClose }: { taskId: string; onClose: () => void }) {
  const [logs, setLogs] = useState<string[]>([])
  const [status, setStatus] = useState<'running' | 'done' | 'failed'>('running')
  const [cmd, setCmd] = useState('')
  const bodyRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    // Load existing logs + status first
    api.task(taskId).then(t => {
      setLogs(t.logs)
      setCmd(t.cmd)
      setStatus(t.status)
      if (t.status !== 'running') return

      // Stream new lines
      const es = new EventSource(api.taskStreamUrl(taskId))
      es.onmessage = e => {
        if (e.data.startsWith('__DONE__:')) {
          const st = e.data.split(':')[1] as 'done' | 'failed'
          setStatus(st)
          es.close()
        } else {
          setLogs(prev => [...prev, e.data])
        }
      }
      es.onerror = () => es.close()
      return () => es.close()
    })
  }, [taskId])

  useEffect(() => {
    if (bodyRef.current) {
      bodyRef.current.scrollTop = bodyRef.current.scrollHeight
    }
  }, [logs])

  const lineClass = (l: string) => {
    const lower = l.toLowerCase()
    if (lower.includes('error') || lower.includes('failed') || lower.includes('traceback')) return 'err'
    if (lower.includes('warning') || lower.includes('warn')) return 'warn'
    if (lower.includes('ok') || lower.includes('done') || lower.includes('success') || lower.includes('inserted')) return 'ok'
    return ''
  }

  return (
    <div className="task-log-panel">
      <div className="task-log-header">
        <div className="task-log-title">
          {status === 'running' && <span className="pulse" />}
          {status === 'done' && <span style={{ color: 'var(--green)' }}>✓</span>}
          {status === 'failed' && <span style={{ color: 'var(--red)' }}>✗</span>}
          <code style={{ fontSize: '0.78rem' }}>applypilot {cmd}</code>
        </div>
        <button className="btn btn-ghost" style={{ fontSize: '0.75rem' }} onClick={onClose}>✕ Close</button>
      </div>
      <div className="task-log-body" ref={bodyRef}>
        {logs.map((l, i) => (
          <span key={i} className={`log-line ${lineClass(l)}`}>{l}{'\n'}</span>
        ))}
        {status === 'done' && <div className="log-done">✓ Completed</div>}
        {status === 'failed' && <div className="log-failed">✗ Failed</div>}
        {status === 'running' && logs.length === 0 && <span style={{ color: 'var(--muted)' }}>Starting…</span>}
      </div>
    </div>
  )
}

// ── Action card ─────────────────────────────────────────────────────────────

interface ActionDef {
  name: string
  desc: string
  icon: string
  run: () => Promise<{ task_id: string }>
}

function ActionCard({ action, onTask }: { action: ActionDef; onTask: (id: string) => void }) {
  const [state, setState] = useState<'idle' | 'running' | 'done' | 'failed'>('idle')

  const click = async () => {
    if (state === 'running') return
    setState('running')
    try {
      const { task_id } = await action.run()
      onTask(task_id)
      // Poll for completion
      const poll = setInterval(async () => {
        const t = await api.task(task_id)
        if (t.status !== 'running') {
          setState(t.status === 'done' ? 'done' : 'failed')
          clearInterval(poll)
        }
      }, 2000)
    } catch {
      setState('failed')
    }
  }

  return (
    <div className={`action-card ${state !== 'idle' ? state : ''}`} onClick={click}>
      <div className="action-name">
        <span>{action.icon}</span>
        {action.name}
        {state === 'running' && <span className="pulse" style={{ marginLeft: 'auto' }} />}
      </div>
      <div className="action-desc">{action.desc}</div>
      {state !== 'idle' && (
        <div className={`action-status ${state}`}>
          {state === 'running' ? '● Running…' : state === 'done' ? '✓ Done' : '✗ Failed'}
        </div>
      )}
    </div>
  )
}

// ── Main Pipeline page ────────────────────────────────────────────────────

export default function Pipeline() {
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null)
  const [taskHistory, setTaskHistory] = useState<string[]>([])

  const pushTask = (id: string) => {
    setActiveTaskId(id)
    setTaskHistory(prev => [id, ...prev.filter(x => x !== id)].slice(0, 20))
  }

  const sections: { title: string; icon: string; actions: ActionDef[] }[] = [
    {
      title: 'Pipeline Stages',
      icon: '⚡',
      actions: [
        { name: 'Run All', desc: 'Run full pipeline: discover → cover', icon: '🚀',
          run: () => api.pipelineRun({ stages: ['all'] }) },
        { name: 'Discover', desc: 'Scrape job boards for new listings', icon: '🔍',
          run: () => api.pipelineRun({ stages: ['discover'] }) },
        { name: 'Enrich', desc: 'Fetch full JDs and apply URLs', icon: '📄',
          run: () => api.pipelineRun({ stages: ['enrich'] }) },
        { name: 'Score', desc: 'LLM fit scoring (7+ = strong)', icon: '⭐',
          run: () => api.pipelineRun({ stages: ['score'] }) },
        { name: 'Tailor', desc: 'Tailor resumes for 7+ jobs', icon: '✏️',
          run: () => api.pipelineRun({ stages: ['tailor'] }) },
        { name: 'Cover Letter', desc: 'Generate cover letters', icon: '📝',
          run: () => api.pipelineRun({ stages: ['cover'] }) },
        { name: 'PDF', desc: 'Render tailored resumes to PDF', icon: '📑',
          run: () => api.pipelineRun({ stages: ['pdf'] }) },
      ],
    },
    {
      title: 'Discover',
      icon: '🌐',
      actions: [
        { name: 'Workday', desc: 'Explore Workday ATS portals', icon: '⚙️',
          run: () => api.exploreWorkday({ limit: 0, resume: true }) },
        { name: 'Greenhouse', desc: 'Explore Greenhouse company portals', icon: '🌿',
          run: () => api.exploreGreenhouse({ limit: 0, resume: true }) },
        { name: 'Ashby', desc: 'Explore Ashby ATS portals', icon: '🏢',
          run: () => api.exploreAshby({ limit: 0, resume: true }) },
        { name: 'Genie', desc: 'Run all ATS portals (Workday + GH + Ashby)', icon: '🧞',
          run: () => api.exploreGenie({ limit: 0, resume: true }) },
        { name: 'Serper (Google)', desc: 'LinkedIn jobs via Google search', icon: '🔎',
          run: () => api.exploreSerper({ tbs: 'qdr:w', workers: 10 }) },
        { name: 'Email (Gmail)', desc: 'Extract job URLs from Gmail alerts', icon: '📧',
          run: () => api.exploreEmail({ days: 30 }) },
      ],
    },
    {
      title: 'Enrichment',
      icon: '🔬',
      actions: [
        { name: 'Enrich (General)', desc: 'Scrape full JDs for all ATS types', icon: '📋',
          run: () => api.pipelineEnrich({ limit: 100, workers: 3 }) },
        { name: 'Enrich LinkedIn', desc: 'Fetch LinkedIn JDs via guest API', icon: '💼',
          run: () => api.pipelineEnrichLinkedin() },
        { name: 'Prioritize', desc: 'Rank 7+ jobs by embedding similarity', icon: '🎯',
          run: () => api.pipelinePrioritize({ min_score: 7 }) },
      ],
    },
    {
      title: 'Auto-Apply',
      icon: '🤖',
      actions: [
        { name: 'Apply (Dry Run)', desc: 'Preview apply queue without submitting', icon: '👁️',
          run: () => api.pipelineApply({ dry_run: true, workers: 1 }) },
        { name: 'Apply (Headless)', desc: 'Submit applications in background', icon: '🤖',
          run: () => api.pipelineApply({ headless: true, workers: 1 }) },
        { name: 'Reset Failed', desc: 'Re-queue all failed applications', icon: '↩️',
          run: async () => { await api.resetFailed(); return { task_id: 'local' } } },
        { name: 'Release Locked', desc: 'Unstick in_progress jobs', icon: '🔓',
          run: async () => { await api.releaseLocked(); return { task_id: 'local' } } },
      ],
    },
    {
      title: 'Utilities',
      icon: '🛠️',
      actions: [
        { name: 'Dedup Jobs', desc: 'Remove duplicate entries from DB', icon: '🧹',
          run: async () => { const r = await api.dedup(); console.log(r); return { task_id: 'local' } } },
        { name: 'Optimize Queue', desc: 'Build Bayesian-ranked apply queue', icon: '📊',
          run: () => api.optimizeQueue({ batch_size: 200, min_score: 7, preview: true }) },
        { name: 'Classify Companies', desc: 'LLM-classify all applied companies', icon: '🏷️',
          run: () => api.classifyCompanies() },
        { name: 'Sync Outcomes', desc: 'Scan Gmail for recruiter responses', icon: '📬',
          run: () => api.syncOutcomes() },
      ],
    },
  ]

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Pipeline</div>
        <div className="page-subtitle">Run any command — logs stream live below each action</div>
      </div>

      {sections.map(sec => (
        <div key={sec.title} className="section">
          <div className="section-title"><span>{sec.icon}</span> {sec.title}</div>
          <div className="action-grid">
            {sec.actions.map(a => (
              <ActionCard
                key={a.name}
                action={a}
                onTask={id => { if (id !== 'local') pushTask(id) }}
              />
            ))}
          </div>
        </div>
      ))}

      {activeTaskId && activeTaskId !== 'local' && (
        <TaskLog taskId={activeTaskId} onClose={() => setActiveTaskId(null)} />
      )}

      {taskHistory.filter(id => id !== 'local').length > 1 && (
        <div className="section">
          <div className="section-title" style={{ fontSize: '0.82rem' }}>Recent Runs</div>
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            {taskHistory.filter(id => id !== 'local').map(id => (
              <button
                key={id}
                className={`btn btn-ghost ${id === activeTaskId ? 'active' : ''}`}
                style={{ fontSize: '0.75rem' }}
                onClick={() => setActiveTaskId(id)}
              >
                {id}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
