const BASE = '/api'

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`${res.status}: ${text}`)
  }
  return res.json()
}

const get = <T>(p: string) => req<T>('GET', p)
const post = <T>(p: string, b?: unknown) => req<T>('POST', p, b)
const patch = <T>(p: string, b?: unknown) => req<T>('PATCH', p, b)

// ── Types ──────────────────────────────────────────────────────────────────

export interface Stats {
  total: number
  with_description: number
  pending_detail: number
  detail_errors: number
  scored: number
  unscored: number
  tailored: number
  untailored_eligible: number
  with_cover_letter: number
  applied: number
  apply_errors: number
  ready_to_apply: number
  score_distribution: [number, number][]
  by_site: [string, number][]
  tailor_exhausted?: number
  cover_exhausted?: number
}

export interface Job {
  url: string
  title: string | null
  company: string | null
  salary: string | null
  location: string | null
  site: string | null
  fit_score: number | null
  score_reasoning: string | null
  application_url: string | null
  applied_at: string | null
  apply_status: string | null
  apply_error: string | null
  tailored_resume_path: string | null
  cover_letter_path: string | null
  discovered_at: string | null
}

export interface JobsResponse {
  total: number
  jobs: Job[]
  offset: number
  limit: number
}

export interface Task {
  id: string
  cmd: string
  status: 'running' | 'done' | 'failed'
  logs: string[]
  started_at: number
  finished_at: number | null
  returncode: number | null
}

export interface TaskRef {
  task_id: string
}

export interface Signal {
  company_name: string
  tier: string | null
  industry: string | null
  size_tier: string | null
  responded: number
  notes: string | null
  updated_at: string | null
}

export interface SignalsResponse {
  signals: Signal[]
  summary: {
    total: number
    responded: number
    no_response: number
    response_rate: number
  }
}

export interface DoctorCheck {
  name: string
  ok: boolean
  note: string
}

export interface DoctorResponse {
  checks: DoctorCheck[]
  tier: number
  tier_label: string
}

// ── API calls ──────────────────────────────────────────────────────────────

export const api = {
  // Stats
  stats: () => get<Stats>('/stats'),
  sites: () => get<string[]>('/sites'),

  // Jobs
  jobs: (params: Record<string, string | number>) => {
    const q = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]))
    return get<JobsResponse>(`/jobs?${q}`)
  },
  markApplied: (url: string) => post('/jobs/mark-applied', { url }),
  markFailed: (url: string, reason?: string) => post('/jobs/mark-failed', { url, reason }),
  resetFailed: () => post('/jobs/reset-failed'),
  releaseLocked: () => post('/jobs/release-locked'),
  dedup: () => post('/jobs/dedup'),

  // Pipeline
  pipelineRun: (b: object) => post<TaskRef>('/pipeline/run', b),
  pipelineApply: (b: object) => post<TaskRef>('/pipeline/apply', b),
  pipelineEnrich: (b: object) => post<TaskRef>('/pipeline/enrich', b),
  pipelineEnrichLinkedin: () => post<TaskRef>('/pipeline/enrich-linkedin'),
  pipelinePrioritize: (b: object) => post<TaskRef>('/pipeline/prioritize', b),

  // Explore
  exploreWorkday: (b: object) => post<TaskRef>('/explore/workday', b),
  exploreGreenhouse: (b: object) => post<TaskRef>('/explore/greenhouse', b),
  exploreAshby: (b: object) => post<TaskRef>('/explore/ashby', b),
  exploreGenie: (b: object) => post<TaskRef>('/explore/genie', b),
  exploreSerper: (b: object) => post<TaskRef>('/explore/serper', b),
  exploreEmail: (b: object) => post<TaskRef>('/explore/email', b),

  // Optimize
  optimizeQueue: (b: object) => post<TaskRef>('/optimize/queue', b),
  classifyCompanies: () => post<TaskRef>('/optimize/classify'),

  // Signals
  signals: (params?: { responded_only?: boolean }) => {
    const q = params?.responded_only ? '?responded_only=true' : ''
    return get<SignalsResponse>(`/signals${q}`)
  },
  logOutcome: (b: object) => post('/signals/log', b),
  syncOutcomes: () => post<TaskRef>('/signals/sync'),
  buildSignals: () => post<TaskRef>('/signals/build'),

  // Doctor
  doctor: () => get<DoctorResponse>('/doctor'),

  // Tasks
  tasks: () => get<{ tasks: Omit<Task, 'logs'>[] }>('/tasks'),
  task: (id: string) => get<Task>(`/tasks/${id}`),
  taskStreamUrl: (id: string) => `${BASE}/tasks/${id}/stream`,
}
