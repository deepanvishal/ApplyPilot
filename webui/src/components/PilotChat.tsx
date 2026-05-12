import { useState, useRef, useEffect, useCallback } from 'react'

type Message = {
  role: 'user' | 'assistant'
  content: string
  streaming?: boolean
}

const KEYFRAMES = `
@keyframes pilotSlideUp {
  from { opacity: 0; transform: translateY(12px); }
  to   { opacity: 1; transform: translateY(0); }
}
@keyframes pilotBlink {
  0%, 100% { opacity: 1; }
  50%       { opacity: 0; }
}
`

export default function PilotChat() {
  const [open, setOpen] = useState(false)
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [streaming, setStreaming] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const abortRef = useRef<AbortController | null>(null)
  const tokenStreamedRef = useRef(false)

  // Fire-and-forget warm-up on mount so BGE-M3 + reranker load before first question
  useEffect(() => {
    fetch('/api/pilot-intel/warmup').catch(() => {})
  }, [])

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages])

  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 50)
  }, [open])

  const send = useCallback(async () => {
    const q = input.trim()
    if (!q || streaming) return

    setInput('')
    const history = messages.map(m => ({ role: m.role, content: m.content }))

    setMessages(prev => [
      ...prev,
      { role: 'user', content: q },
      { role: 'assistant', content: '', streaming: true },
    ])
    setStreaming(true)
    tokenStreamedRef.current = false

    const ctrl = new AbortController()
    abortRef.current = ctrl

    try {
      const response = await fetch('/api/pilot-intel/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q, history }),
        signal: ctrl.signal,
      })

      if (!response.ok) throw new Error(`HTTP ${response.status}`)

      const reader = response.body!.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let done_seen = false

      while (!done_seen) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split('\n\n')
        buffer = parts.pop() ?? ''

        for (const part of parts) {
          const line = part.trim()
          if (!line.startsWith('data:')) continue
          const data = line.slice(5).trimStart()

          if (data === '__DONE__') {
            done_seen = true
            setMessages(prev =>
              prev.map((m, i) =>
                i === prev.length - 1 ? { ...m, streaming: false } : m
              )
            )
            break
          } else if (data.startsWith('__ERROR__:')) {
            const err = data.slice(10)
            setMessages(prev =>
              prev.map((m, i) =>
                i === prev.length - 1
                  ? { role: 'assistant', content: `⚠ ${err}`, streaming: false }
                  : m
              )
            )
            done_seen = true
            break
          } else if (data.startsWith('__FINAL__:')) {
            const final = data.slice(10).replace(/\\n/g, '\n')
            setMessages(prev =>
              prev.map((m, i) =>
                i === prev.length - 1
                  ? { role: 'assistant', content: final, streaming: false }
                  : m
              )
            )
          } else {
            tokenStreamedRef.current = true
            const token = data.replace(/\\n/g, '\n')
            setMessages(prev =>
              prev.map((m, i) =>
                i === prev.length - 1
                  ? { ...m, content: m.content + token }
                  : m
              )
            )
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        setMessages(prev =>
          prev.map((m, i) =>
            i === prev.length - 1
              ? {
                  role: 'assistant',
                  content: 'Connection error. Is the server running?',
                  streaming: false,
                }
              : m
          )
        )
      }
    } finally {
      setStreaming(false)
      abortRef.current = null
    }
  }, [input, messages, streaming])

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      void send()
    }
  }

  const clearChat = () => {
    if (streaming && abortRef.current) abortRef.current.abort()
    setMessages([])
    setStreaming(false)
  }

  return (
    <>
      <style>{KEYFRAMES}</style>

      {/* Floating trigger button */}
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          position: 'fixed',
          bottom: '1.5rem',
          right: '1.5rem',
          background: open ? '#3730a3' : '#4f46e5',
          color: '#fff',
          border: 'none',
          borderRadius: '2rem',
          padding: '0.55rem 1.1rem',
          fontSize: '0.83rem',
          fontWeight: 700,
          cursor: 'pointer',
          zIndex: 9999,
          boxShadow: '0 4px 20px rgba(79,70,229,0.45)',
          display: 'flex',
          alignItems: 'center',
          gap: '0.4rem',
          letterSpacing: '0.01em',
          transition: 'background 0.15s',
        }}
      >
        <span style={{ fontSize: '0.9rem' }}>✦</span>
        <span>Ask pilot-intel</span>
      </button>

      {/* Chat panel */}
      {open && (
        <div
          style={{
            position: 'fixed',
            bottom: '4.5rem',
            right: '1.5rem',
            width: '350px',
            height: '500px',
            background: '#1a1a2e',
            border: '1px solid #334155',
            borderRadius: '12px',
            display: 'flex',
            flexDirection: 'column',
            zIndex: 9998,
            boxShadow: '0 8px 40px rgba(0,0,0,0.65)',
            overflow: 'hidden',
            animation: 'pilotSlideUp 0.18s ease',
            fontFamily: 'inherit',
          }}
        >
          {/* Header */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              padding: '0.65rem 0.875rem',
              borderBottom: '1px solid #2d3748',
              background: '#16213e',
              flexShrink: 0,
            }}
          >
            <span
              style={{
                fontSize: '0.83rem',
                fontWeight: 700,
                color: '#e2e8f0',
                display: 'flex',
                alignItems: 'center',
                gap: '0.4rem',
              }}
            >
              <span style={{ color: '#818cf8' }}>✦</span>
              pilot-intel
              {streaming && (
                <span
                  style={{
                    display: 'inline-block',
                    width: 6,
                    height: 6,
                    borderRadius: '50%',
                    background: '#f59e0b',
                    animation: 'pilotBlink 1s infinite',
                    marginLeft: 4,
                  }}
                />
              )}
            </span>
            <div style={{ display: 'flex', gap: '0.25rem' }}>
              <button
                onClick={clearChat}
                title="Clear chat"
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: '#64748b',
                  cursor: 'pointer',
                  fontSize: '1rem',
                  lineHeight: 1,
                  padding: '0.2rem 0.35rem',
                  borderRadius: '4px',
                }}
                onMouseEnter={e => (e.currentTarget.style.color = '#94a3b8')}
                onMouseLeave={e => (e.currentTarget.style.color = '#64748b')}
              >
                ↺
              </button>
              <button
                onClick={() => setOpen(false)}
                title="Close"
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: '#64748b',
                  cursor: 'pointer',
                  fontSize: '1.1rem',
                  lineHeight: 1,
                  padding: '0.2rem 0.35rem',
                  borderRadius: '4px',
                }}
                onMouseEnter={e => (e.currentTarget.style.color = '#94a3b8')}
                onMouseLeave={e => (e.currentTarget.style.color = '#64748b')}
              >
                ×
              </button>
            </div>
          </div>

          {/* Messages */}
          <div
            ref={scrollRef}
            style={{
              flex: 1,
              overflowY: 'auto',
              padding: '0.75rem',
              display: 'flex',
              flexDirection: 'column',
              gap: '0.6rem',
            }}
          >
            {messages.length === 0 && (
              <div
                style={{
                  color: '#475569',
                  fontSize: '0.78rem',
                  textAlign: 'center',
                  marginTop: '2.5rem',
                  lineHeight: 1.6,
                }}
              >
                Ask anything about your job search pipeline.<br />
                <span style={{ color: '#374151' }}>
                  errors · coverage · fit scores · site performance
                </span>
              </div>
            )}

            {messages.map((msg, i) => (
              <div
                key={i}
                style={{
                  alignSelf: msg.role === 'user' ? 'flex-end' : 'flex-start',
                  background: msg.role === 'user' ? '#4f46e5' : '#2d2d3d',
                  color: '#e2e8f0',
                  padding: '0.5rem 0.75rem',
                  borderRadius:
                    msg.role === 'user'
                      ? '12px 12px 3px 12px'
                      : '12px 12px 12px 3px',
                  maxWidth: '88%',
                  fontSize: '0.81rem',
                  lineHeight: 1.55,
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word',
                }}
              >
                {msg.streaming && !msg.content ? (
                  <span style={{ color: '#64748b', fontStyle: 'italic' }}>
                    Thinking...
                  </span>
                ) : (
                  <>
                    {msg.content}
                    {msg.streaming && (
                      <span
                        style={{
                          display: 'inline-block',
                          width: '0.5rem',
                          animation: 'pilotBlink 0.7s infinite',
                          marginLeft: '1px',
                        }}
                      >
                        ▊
                      </span>
                    )}
                  </>
                )}
              </div>
            ))}
          </div>

          {/* Input */}
          <div
            style={{
              padding: '0.65rem 0.75rem',
              borderTop: '1px solid #2d3748',
              background: '#16213e',
              display: 'flex',
              gap: '0.5rem',
              flexShrink: 0,
            }}
          >
            <input
              ref={inputRef}
              type="text"
              placeholder={streaming ? 'Waiting for response…' : 'Ask a question…'}
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={handleKey}
              disabled={streaming}
              style={{
                flex: 1,
                background: '#0f172a',
                border: '1px solid #334155',
                borderRadius: '8px',
                padding: '0.42rem 0.7rem',
                color: '#e2e8f0',
                fontSize: '0.81rem',
                outline: 'none',
                opacity: streaming ? 0.6 : 1,
              }}
            />
            <button
              onClick={() => void send()}
              disabled={streaming || !input.trim()}
              style={{
                background: '#4f46e5',
                color: '#fff',
                border: 'none',
                borderRadius: '8px',
                padding: '0.42rem 0.85rem',
                fontSize: '0.85rem',
                fontWeight: 700,
                cursor: streaming || !input.trim() ? 'not-allowed' : 'pointer',
                opacity: streaming || !input.trim() ? 0.45 : 1,
                transition: 'opacity 0.15s',
              }}
            >
              {streaming ? '…' : '→'}
            </button>
          </div>
        </div>
      )}
    </>
  )
}
