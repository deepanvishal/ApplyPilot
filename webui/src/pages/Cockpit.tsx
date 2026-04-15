import { useEffect, useRef, useState } from 'react'
import { api, SystemHealth } from '../api'

// ── Types ─────────────────────────────────────────────────────────────────

interface ParamDef {
  key: string
  label: string
  type: 'number' | 'boolean' | 'string' | 'select'
  default: unknown
  options?: string[]
  min?: number
  max?: number
  hint?: string
}

interface CmdDef {
  id: string
  name: string
  icon: string
  desc: string
  params: ParamDef[]
  run: (p: Record<string, unknown>) => Promise<{ task_id: string }>
}

interface StageDef {
  id: string
  number: string
  title: string
  color: string
  commands: CmdDef[]
}

// ── Stage definitions ─────────────────────────────────────────────────────

const STAGES: StageDef[] = [
  {
    id: 'discover',
    number: '1',
    title: 'Discovery',
    color: '#60a5fa',
    commands: [
      {
        id: 'email',
        name: 'Email (Gmail)',
        icon: '📧',
        desc: 'Extract LinkedIn job URLs from Gmail job alerts',
        params: [{ key: 'days', label: 'Look-back days', type: 'number', default: 30, min: 1, max: 365 }],
        run: p => api.exploreEmail({ days: p.days }),
      },
      {
        id: 'jobspy',
        name: 'Jobspy',
        icon: '🔍',
        desc: 'Scrape Indeed, LinkedIn, Glassdoor, ZipRecruiter (uses searches.yaml)',
        params: [],
        run: () => api.exploreJobspy(),
      },
      {
        id: 'apify',
        name: 'Apify (LinkedIn)',
        icon: '🤖',
        desc: 'Deep LinkedIn scrape via Apify actor',
        params: [
          { key: 'days', label: 'Posted within days', type: 'number', default: 7, min: 1, max: 30 },
          { key: 'workers', label: 'Workers', type: 'number', default: 5, min: 1, max: 20 },
          { key: 'limit', label: 'Limit (0 = unlimited)', type: 'number', default: 0, min: 0 },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.exploreApify(p),
      },
      {
        id: 'serper',
        name: 'Serper / SerpAPI',
        icon: '🔎',
        desc: 'LinkedIn jobs via Serper.dev + Google Jobs via SerpAPI',
        params: [
          {
            key: 'tbs', label: 'Date filter', type: 'select', default: 'qdr:w',
            options: ['qdr:d', 'qdr:w', 'qdr:m'],
            hint: 'qdr:d=today, qdr:w=week, qdr:m=month',
          },
          { key: 'workers', label: 'Workers', type: 'number', default: 10, min: 1, max: 30 },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.exploreSerper(p),
      },
      {
        id: 'genie',
        name: 'Run Genie',
        icon: '🧞',
        desc: 'Unified ATS discovery: Workday, Greenhouse, Ashby, Lever, BambooHR',
        params: [
          { key: 'workers', label: 'Workers', type: 'number', default: 5, min: 1, max: 20 },
          { key: 'limit', label: 'Limit (0 = unlimited)', type: 'number', default: 0, min: 0 },
          { key: 'full', label: 'Full scan (all ~12k portals)', type: 'boolean', default: false },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.exploreGenie({ ...p, resume: !p.full }),
      },
      {
        id: 'workday',
        name: 'Explore Workday',
        icon: '⚙️',
        desc: 'Discover jobs from known Workday portals',
        params: [
          { key: 'limit', label: 'Limit (0 = unlimited)', type: 'number', default: 0, min: 0 },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.exploreWorkday({ ...p, resume: true }),
      },
      {
        id: 'greenhouse',
        name: 'Explore Greenhouse',
        icon: '🌿',
        desc: 'Discover jobs from known Greenhouse portals',
        params: [
          { key: 'limit', label: 'Limit (0 = unlimited)', type: 'number', default: 0, min: 0 },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.exploreGreenhouse({ ...p, resume: true }),
      },
      {
        id: 'ashby',
        name: 'Explore Ashby',
        icon: '🏢',
        desc: 'Discover jobs from known Ashby portals',
        params: [
          { key: 'limit', label: 'Limit (0 = unlimited)', type: 'number', default: 0, min: 0 },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.exploreAshby({ ...p, resume: true }),
      },
    ],
  },
  {
    id: 'promote',
    number: '1.1',
    title: 'Promote to Jobs Table',
    color: '#a78bfa',
    commands: [
      {
        id: 'promote-genie',
        name: 'Promote Genie Jobs',
        icon: '⬆️',
        desc: 'Move staged genie_jobs → jobs table',
        params: [],
        run: () => api.promoteGenie(),
      },
      {
        id: 'promote-serper',
        name: 'Promote Serper Jobs',
        icon: '⬆️',
        desc: 'Move staged serper_jobs → jobs table',
        params: [],
        run: () => api.promoteSerper(),
      },
    ],
  },
  {
    id: 'enrich',
    number: '2',
    title: 'Enrich',
    color: '#34d399',
    commands: [
      {
        id: 'enrich',
        name: 'Run Enrich',
        icon: '📄',
        desc: 'Scrape full JDs and apply URLs (JSON-LD → CSS → LLM cascade)',
        params: [
          { key: 'limit', label: 'Limit', type: 'number', default: 100, min: 1 },
          { key: 'workers', label: 'Workers', type: 'number', default: 3, min: 1, max: 10 },
        ],
        run: p => api.pipelineEnrich(p),
      },
      {
        id: 'enrich-linkedin',
        name: 'Enrich LinkedIn',
        icon: '💼',
        desc: 'Fetch LinkedIn job descriptions via guest API',
        params: [],
        run: () => api.pipelineEnrichLinkedin(),
      },
    ],
  },
  {
    id: 'score',
    number: '3',
    title: 'Score',
    color: '#f59e0b',
    commands: [
      {
        id: 'score',
        name: 'Run Score',
        icon: '⭐',
        desc: 'LLM fit scoring 1–10 per job description vs. your resume',
        params: [
          { key: 'workers', label: 'Workers', type: 'number', default: 5, min: 1, max: 20 },
          { key: 'limit', label: 'Limit (0 = all)', type: 'number', default: 0, min: 0 },
        ],
        run: p => api.pipelineScore(p),
      },
    ],
  },
  {
    id: 'embedding',
    number: '4',
    title: 'Embedding',
    color: '#06b6d4',
    commands: [
      {
        id: 'prioritize',
        name: 'Run Embedding',
        icon: '🎯',
        desc: 'Cosine similarity ranking — embeds resume + all JDs',
        params: [
          { key: 'min_score', label: 'Min fit score', type: 'number', default: 7, min: 1, max: 10 },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.pipelinePrioritize(p),
      },
    ],
  },
  {
    id: 'tailor',
    number: '5',
    title: 'Tailor & Optimize',
    color: '#10b981',
    commands: [
      {
        id: 'tailor',
        name: 'Run Tailor',
        icon: '✏️',
        desc: 'LLM-rewrite resume per job, generate tailored PDF',
        params: [
          { key: 'min_score', label: 'Min fit score', type: 'number', default: 7, min: 1, max: 10 },
          { key: 'workers', label: 'Workers', type: 'number', default: 3, min: 1, max: 10 },
          {
            key: 'validation', label: 'Validation mode', type: 'select', default: 'normal',
            options: ['lenient', 'normal', 'strict'],
            hint: 'strict: LLM judge; normal: keyword check; lenient: skip all',
          },
        ],
        run: p => api.pipelineTailor(p),
      },
      {
        id: 'allocate',
        name: 'Run Optimize',
        icon: '📊',
        desc: 'Bayesian queue allocation — rank jobs by tier + response probability',
        params: [
          { key: 'batch_size', label: 'Batch size', type: 'number', default: 200, min: 10 },
          { key: 'min_score', label: 'Min fit score', type: 'number', default: 7, min: 1, max: 10 },
        ],
        run: p => api.optimizeQueue(p),
      },
      {
        id: 'purge-blocked',
        name: 'Purge Blocked',
        icon: '🚫',
        desc: 'Remove jobs from blocklisted companies (preserves applied/manual)',
        params: [
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
        ],
        run: p => api.purgeBlocked(p),
      },
    ],
  },
  {
    id: 'apply',
    number: '6',
    title: 'Apply',
    color: '#6366f1',
    commands: [
      {
        id: 'apply',
        name: 'Run Apply',
        icon: '🚀',
        desc: 'Chrome + Claude Code CLI auto-fills and submits applications',
        params: [
          { key: 'workers', label: 'Workers', type: 'number', default: 1, min: 1, max: 5 },
          { key: 'min_score', label: 'Min fit score', type: 'number', default: 7, min: 1, max: 10 },
          { key: 'limit', label: 'Limit (0 = unlimited)', type: 'number', default: 0, min: 0 },
          { key: 'headless', label: 'Headless mode', type: 'boolean', default: false },
          { key: 'dry_run', label: 'Dry run', type: 'boolean', default: false },
          { key: 'continuous', label: 'Continuous mode', type: 'boolean', default: false },
        ],
        run: p => api.pipelineApply(p),
      },
    ],
  },
]

// ── Param form ────────────────────────────────────────────────────────────

function ParamForm({
  params,
  values,
  onChange,
}: {
  params: ParamDef[]
  values: Record<string, unknown>
  onChange: (k: string, v: unknown) => void
}) {
  if (params.length === 0) return <p style={{ color: 'var(--muted)', fontSize: '0.82rem' }}>No configuration needed.</p>
  return (
    <div className="param-form">
      {params.map(p => (
        <div key={p.key} className="param-row">
          <label className="param-label">
            {p.label}
            {p.hint && <span className="param-hint">{p.hint}</span>}
          </label>
          {p.type === 'boolean' ? (
            <label className="toggle">
              <input
                type="checkbox"
                checked={Boolean(values[p.key] ?? p.default)}
                onChange={e => onChange(p.key, e.target.checked)}
              />
              <span className="toggle-track" />
            </label>
          ) : p.type === 'select' ? (
            <select
              className="select-input"
              value={String(values[p.key] ?? p.default)}
              onChange={e => onChange(p.key, e.target.value)}
            >
              {p.options!.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
          ) : (
            <input
              className="num-input"
              type="number"
              value={String(values[p.key] ?? p.default)}
              min={p.min}
              max={p.max}
              onChange={e => onChange(p.key, p.type === 'number' ? Number(e.target.value) : e.target.value)}
            />
          )}
        </div>
      ))}
    </div>
  )
}

// ── Command panel ─────────────────────────────────────────────────────────

function CommandPanel({
  cmd,
  stage,
  onTask,
}: {
  cmd: CmdDef
  stage: StageDef
  onTask: (id: string) => void
}) {
  const [values, setValues] = useState<Record<string, unknown>>(() =>
    Object.fromEntries(cmd.params.map(p => [p.key, p.default]))
  )
  const [state, setState] = useState<'idle' | 'running' | 'done' | 'failed'>('idle')

  const set = (k: string, v: unknown) => setValues(prev => ({ ...prev, [k]: v }))

  const run = async () => {
    if (state === 'running') return
    setState('running')
    try {
      const { task_id } = await cmd.run(values)
      onTask(task_id)
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
    <div className="cmd-panel">
      <div className="cmd-panel-header">
        <span className="cmd-panel-stage-tag" style={{ background: stage.color + '22', color: stage.color }}>
          Stage {stage.number} — {stage.title}
        </span>
        <h2 className="cmd-panel-title">
          <span>{cmd.icon}</span> {cmd.name}
        </h2>
        <p className="cmd-panel-desc">{cmd.desc}</p>
      </div>

      <div className="cmd-panel-body">
        <div className="cmd-section-label">Configuration</div>
        <ParamForm params={cmd.params} values={values} onChange={set} />
      </div>

      <div className="cmd-panel-footer">
        <button
          className={`btn btn-run ${state}`}
          onClick={run}
          disabled={state === 'running'}
        >
          {state === 'running' ? (
            <><span className="pulse" /> Running…</>
          ) : state === 'done' ? (
            '✓ Done — Run Again'
          ) : state === 'failed' ? (
            '✗ Failed — Retry'
          ) : (
            `▶ Run — ${cmd.name}`
          )}
        </button>
        <div className={`cmd-status-text ${state}`}>
          {state === 'done' && 'Completed successfully'}
          {state === 'failed' && 'Command failed — check terminal below'}
        </div>
      </div>
    </div>
  )
}

// ── Stage sidebar ─────────────────────────────────────────────────────────

function StageSidebar({
  selected,
  onSelect,
}: {
  selected: { stageId: string; cmdId: string } | null
  onSelect: (s: { stageId: string; cmdId: string }) => void
}) {
  const [expanded, setExpanded] = useState<Set<string>>(
    new Set(STAGES.map(s => s.id))
  )
  const toggle = (id: string) =>
    setExpanded(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })

  return (
    <div className="stage-sidebar">
      {STAGES.map(stage => (
        <div key={stage.id} className="stage-group">
          <button
            className="stage-group-header"
            onClick={() => toggle(stage.id)}
            style={{ borderLeftColor: stage.color }}
          >
            <span className="stage-num" style={{ color: stage.color }}>{stage.number}</span>
            <span className="stage-name">{stage.title}</span>
            <span className="stage-chevron">{expanded.has(stage.id) ? '▾' : '▸'}</span>
          </button>
          {expanded.has(stage.id) && (
            <div className="stage-cmds">
              {stage.commands.map(cmd => {
                const active = selected?.stageId === stage.id && selected?.cmdId === cmd.id
                return (
                  <button
                    key={cmd.id}
                    className={`stage-cmd-btn ${active ? 'active' : ''}`}
                    onClick={() => onSelect({ stageId: stage.id, cmdId: cmd.id })}
                  >
                    <span className="stage-cmd-icon">{cmd.icon}</span>
                    <span>{cmd.name}</span>
                  </button>
                )
              })}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

// ── Health pane ───────────────────────────────────────────────────────────

function HealthPane({ health, onKill }: { health: SystemHealth | null; onKill: (id: string) => void }) {
  const [killTarget, setKillTarget] = useState('')
  const fmt = (n: number, unit: string) => `${n.toLocaleString()}${unit}`
  const fmtMem = (mb: number) => mb >= 1024 ? `${(mb / 1024).toFixed(1)}GB` : `${mb}MB`

  return (
    <div className="health-pane">
      <div className="strip-pane-title">System Health</div>
      <div className="health-metrics">
        <div className="health-metric">
          <div className="hm-label">Tasks</div>
          <div className="hm-value" style={{ color: health && health.running_tasks > 0 ? 'var(--yellow)' : 'var(--green)' }}>
            {health?.running_tasks ?? 0}
          </div>
          <div className="hm-sub">running</div>
        </div>
        <div className="health-metric">
          <div className="hm-label">CPU</div>
          <div className="hm-value" style={{ color: (health?.cpu_pct ?? 0) > 80 ? 'var(--red)' : 'var(--text)' }}>
            {health?.cpu_pct != null ? fmt(health.cpu_pct, '%') : '—'}
          </div>
          <div className="hm-sub">usage</div>
        </div>
        <div className="health-metric">
          <div className="hm-label">RAM</div>
          <div className="hm-value" style={{ color: (health?.memory_pct ?? 0) > 85 ? 'var(--red)' : 'var(--text)' }}>
            {health?.memory_mb ? fmtMem(health.memory_mb) : '—'}
          </div>
          <div className="hm-sub">{health?.memory_pct != null ? `${health.memory_pct}%` : 'used'}</div>
        </div>
        {health?.gpu_pct != null && (
          <div className="health-metric">
            <div className="hm-label">GPU</div>
            <div className="hm-value">{fmt(health.gpu_pct, '%')}</div>
            <div className="hm-sub">{health.gpu_mem_mb ? fmtMem(health.gpu_mem_mb) : 'vram'}</div>
          </div>
        )}
      </div>

      {/* Kill dropdown */}
      <div className="kill-row">
        <select
          className="select-input kill-select"
          value={killTarget}
          onChange={e => setKillTarget(e.target.value)}
        >
          <option value="">— select task to kill —</option>
          {health?.tasks.map(t => (
            <option key={t.id} value={t.id}>
              [{t.id}] {t.cmd.slice(0, 40)}
            </option>
          ))}
        </select>
        <button
          className="btn btn-danger"
          disabled={!killTarget}
          onClick={async () => {
            if (!killTarget) return
            await onKill(killTarget)
            setKillTarget('')
          }}
        >
          ✕ Kill
        </button>
      </div>
    </div>
  )
}

// ── Task list pane ────────────────────────────────────────────────────────

function TaskListPane({
  health,
  activeTaskId,
  onSelect,
}: {
  health: SystemHealth | null
  activeTaskId: string | null
  onSelect: (id: string) => void
}) {
  const [recentTasks, setRecentTasks] = useState<Array<{ id: string; cmd: string; status: string }>>([])

  useEffect(() => {
    api.tasks().then(r => setRecentTasks(r.tasks.slice(0, 8) as never[]))
  }, [health])  // refresh when health updates

  const statusColor = (s: string) =>
    s === 'running' ? 'var(--yellow)' : s === 'done' ? 'var(--green)' : 'var(--red)'

  return (
    <div className="taskpane">
      <div className="strip-pane-title">Recent Tasks</div>
      <div className="task-list-items">
        {recentTasks.length === 0 && (
          <div style={{ color: 'var(--muted)', fontSize: '0.75rem', padding: '0.5rem 0' }}>No tasks yet</div>
        )}
        {recentTasks.map(t => (
          <button
            key={t.id}
            className={`task-list-item ${t.id === activeTaskId ? 'active' : ''}`}
            onClick={() => onSelect(t.id)}
          >
            <span
              className="task-status-dot"
              style={{ background: statusColor(t.status) }}
            />
            <span className="task-list-cmd">{t.cmd.slice(0, 28)}</span>
            <span className="task-list-id" style={{ color: statusColor(t.status) }}>{t.id}</span>
          </button>
        ))}
      </div>
    </div>
  )
}

// ── Terminal pane ─────────────────────────────────────────────────────────

function TerminalPane({ taskId }: { taskId: string | null }) {
  const [logs, setLogs] = useState<string[]>([])
  const [status, setStatus] = useState<'running' | 'done' | 'failed' | 'idle'>('idle')
  const [cmd, setCmd] = useState('')
  const bodyRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    if (!taskId) { setLogs([]); setStatus('idle'); setCmd(''); return }
    esRef.current?.close()

    api.task(taskId).then(t => {
      setCmd(t.cmd)
      setLogs(t.logs)
      setStatus(t.status)
      if (t.status !== 'running') return
      const es = new EventSource(api.taskStreamUrl(taskId))
      esRef.current = es
      es.onmessage = e => {
        if (e.data.startsWith('__DONE__:')) {
          setStatus(e.data.split(':')[1] as 'done' | 'failed')
          es.close()
        } else {
          setLogs(prev => [...prev, e.data])
        }
      }
      es.onerror = () => es.close()
    })
    return () => esRef.current?.close()
  }, [taskId])

  useEffect(() => {
    if (bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight
  }, [logs])

  const lineColor = (l: string) => {
    const ll = l.toLowerCase()
    if (ll.includes('error') || ll.includes('fail') || ll.includes('traceback')) return '#f87171'
    if (ll.includes('warn')) return '#fbbf24'
    if (ll.includes('ok') || ll.includes('done') || ll.includes('success') || ll.includes('insert')) return '#34d399'
    return '#94a3b8'
  }

  return (
    <div className="terminal-pane">
      <div className="strip-pane-title">
        Terminal
        {taskId && (
          <span style={{ marginLeft: '0.5rem', color: 'var(--muted)', fontWeight: 400, fontSize: '0.7rem' }}>
            {status === 'running' && <><span className="pulse" /> {cmd}</>}
            {status === 'done' && <span style={{ color: 'var(--green)' }}>✓ {cmd}</span>}
            {status === 'failed' && <span style={{ color: 'var(--red)' }}>✗ {cmd}</span>}
          </span>
        )}
      </div>
      <div className="terminal-body" ref={bodyRef}>
        {!taskId && <span className="terminal-placeholder">Run a command to see output here…</span>}
        {logs.map((l, i) => (
          <span key={i} style={{ display: 'block', color: lineColor(l) }}>{l}{'\n'}</span>
        ))}
        {status === 'done' && <span style={{ color: 'var(--green)', fontWeight: 600 }}>✓ Completed{'\n'}</span>}
        {status === 'failed' && <span style={{ color: 'var(--red)', fontWeight: 600 }}>✗ Failed{'\n'}</span>}
      </div>
    </div>
  )
}

// ── Main Cockpit ──────────────────────────────────────────────────────────

export default function Cockpit() {
  const [selected, setSelected] = useState<{ stageId: string; cmdId: string } | null>(null)
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null)
  const [health, setHealth] = useState<SystemHealth | null>(null)

  // Poll system health every 3s
  useEffect(() => {
    const tick = () => api.systemHealth().then(setHealth).catch(() => null)
    tick()
    const id = setInterval(tick, 3000)
    return () => clearInterval(id)
  }, [])

  const handleTask = (id: string) => setActiveTaskId(id)

  const handleKill = async (id: string) => {
    await api.killTask(id)
    api.systemHealth().then(setHealth)
  }

  // Find selected cmd + stage
  const selectedStage = selected ? STAGES.find(s => s.id === selected.stageId) : null
  const selectedCmd = selectedStage?.commands.find(c => c.id === selected?.cmdId) ?? null

  return (
    <div className="cockpit">
      <div className="cockpit-upper">
        {/* Left — stage/command list */}
        <StageSidebar selected={selected} onSelect={setSelected} />

        {/* Right — command config panel */}
        <div className="cockpit-panel">
          {selectedCmd && selectedStage ? (
            <CommandPanel cmd={selectedCmd} stage={selectedStage} onTask={handleTask} />
          ) : (
            <div className="cockpit-welcome">
              <div className="cockpit-welcome-icon">🚀</div>
              <div className="cockpit-welcome-title">ApplyPilot Cockpit</div>
              <div className="cockpit-welcome-sub">
                Select a stage command on the left to configure and run it.
              </div>
              <div className="cockpit-stage-overview">
                {STAGES.map(s => (
                  <div key={s.id} className="overview-stage" style={{ borderLeftColor: s.color }}>
                    <span className="overview-num" style={{ color: s.color }}>{s.number}</span>
                    <span className="overview-title">{s.title}</span>
                    <span className="overview-count" style={{ color: 'var(--muted)' }}>
                      {s.commands.length} cmd{s.commands.length !== 1 ? 's' : ''}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Bottom strip — 3 panes */}
      <div className="cockpit-strip">
        <HealthPane health={health} onKill={handleKill} />
        <TaskListPane health={health} activeTaskId={activeTaskId} onSelect={setActiveTaskId} />
        <TerminalPane taskId={activeTaskId} />
      </div>
    </div>
  )
}
