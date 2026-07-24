import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Zap, Settings, FileCode, CheckSquare, ChevronRight,
  Key, MessageSquare, Brain,
} from 'lucide-react'
import FileDropzone from '../components/FileDropzone.jsx'
import { api } from '../lib/api.js'

// ── Provider definitions ──────────────────────────────────────────────────────
const PROVIDERS = {
  apikey: {
    id:    'apikey',
    label: 'API Key',
    icon:  Key,
    desc:  'Use your own LLM API key (OpenAI, Gemini, or Anthropic)',
    color: 'var(--primary)',
  },
  ag_chat: {
    id:    'ag_chat',
    label: 'AG Chat',
    icon:  MessageSquare,
    desc:  'Route all agent prompts to the Antigravity IDE agent in your active chat',
    color: 'var(--purple)',
  },
}

const LLM_OPTIONS = [
  { provider: 'gemini',    label: 'Google Gemini', color: '#4285F4' },
  { provider: 'openai',    label: 'OpenAI',        color: '#10A37F' },
  { provider: 'anthropic', label: 'Anthropic',     color: '#D97706' },
]

const OUTPUT_OPTIONS = [
  { id: 'pyspark',    label: 'PySpark',          sub: 'Apache Spark Python script',      color: 'var(--primary)' },
  { id: 'datafusion', label: 'Cloud DataFusion', sub: 'GCP DataFusion pipeline JSON',    color: 'var(--accent)' },
  { id: 'tests',      label: 'Unit Tests',       sub: 'Auto-generated pytest suite',     color: 'var(--success)' },
  { id: 'report',     label: 'Migration Report', sub: 'Confidence & validation summary', color: 'var(--purple)' },
]

export default function Upload() {
  const navigate  = useNavigate()
  const [file,    setFile]    = useState(null)
  const [outputs, setOutputs] = useState({ pyspark: true, datafusion: true, tests: true, report: true })
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  // LLM provider state
  const [providerMode, setProviderMode] = useState('apikey') // 'apikey' | 'ag_chat'
  const [llmProvider,  setLlmProvider]  = useState('gemini')

  const toggleOutput = (id) => setOutputs(o => ({ ...o, [id]: !o[id] }))

  const handleStart = async () => {
    if (!file) { setError('Please select a file to migrate'); return }
    setError(null)
    setLoading(true)
    try {
      const providerConfig = providerMode === 'ag_chat'
        ? { provider: 'ag_chat', apiKey: '', model: '' }
        : { provider: llmProvider, apiKey: 'env', model: 'gemini-2.5-pro' }

      const { jobId } = await api.uploadFile(file, providerConfig)
      navigate(`/pipeline/${jobId}`)
    } catch (e) {
      setError(e.message)
      setLoading(false)
    }
  }

  const canStart = !!file

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.35 }}
      className="page"
    >
      <div className="container" style={{ padding: '48px 24px', maxWidth: 1020 }}>
        {/* Header */}
        <div style={{ marginBottom: 40 }}>
          <div className="section-label">New Migration</div>
          <h1 style={{ fontSize: 36, fontWeight: 700 }}>Upload DataStage File</h1>
          <p style={{ color: 'var(--text-muted)', marginTop: 8, fontSize: 16 }}>
            Upload a <code style={{ color: 'var(--primary-bright)', fontFamily: 'var(--font-mono)' }}>.dsx</code> or{' '}
            <code style={{ color: 'var(--purple)', fontFamily: 'var(--font-mono)' }}>.isx</code> file and choose your AI provider.
          </p>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 380px', gap: 24, alignItems: 'start' }}>
          {/* ── Left column ───────────────────────────────────────────── */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            {/* File upload */}
            <div className="glass-card" style={{ padding: 28 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
                <FileCode size={18} color="var(--primary)" />
                <h2 style={{ fontSize: 16, fontWeight: 700 }}>Upload File</h2>
              </div>
              <FileDropzone onFile={setFile} />
            </div>

            {/* ── LLM Provider ─────────────────────────────────────────── */}
            <div className="glass-card" style={{ padding: 28 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
                <Brain size={18} color="var(--purple)" />
                <h2 style={{ fontSize: 16, fontWeight: 700 }}>AI Provider</h2>
              </div>

              {/* Provider mode toggle */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 20 }}>
                {Object.values(PROVIDERS).map(p => {
                  const active = providerMode === p.id
                  return (
                    <button
                      key={p.id}
                      onClick={() => setProviderMode(p.id)}
                      style={{
                        display: 'flex', flexDirection: 'column', alignItems: 'flex-start',
                        gap: 8, padding: '16px 18px', borderRadius: 'var(--radius)',
                        border: `1.5px solid ${active ? p.color : 'var(--border)'}`,
                        background: active ? `${p.color}12` : 'rgba(255,255,255,0.02)',
                        cursor: 'pointer', transition: 'all 0.2s', textAlign: 'left',
                        boxShadow: active ? `0 0 20px ${p.color}18` : 'none',
                      }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <p.icon size={16} color={active ? p.color : 'var(--text-dim)'} />
                        <span style={{ fontSize: 14, fontWeight: 700, color: active ? 'var(--text)' : 'var(--text-muted)' }}>
                          {p.label}
                        </span>
                      </div>
                      <span style={{ fontSize: 11, color: 'var(--text-dim)', lineHeight: 1.4 }}>{p.desc}</span>
                    </button>
                  )
                })}
              </div>

              <AnimatePresence mode="wait">
                {/* API Key Mode */}
                {providerMode === 'apikey' && (
                  <motion.div
                    key="apikey"
                    initial={{ opacity: 0, y: 8 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -8 }}
                    style={{ display: 'flex', flexDirection: 'column', gap: 14 }}
                  >
                    {/* LLM selector */}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                      <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-dim)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                        LLM Provider
                      </label>
                      <div style={{ display: 'flex', gap: 8 }}>
                        {LLM_OPTIONS.map(opt => (
                          <button
                            key={opt.provider}
                            onClick={() => setLlmProvider(opt.provider)}
                            style={{
                              flex: 1, padding: '9px 12px', borderRadius: 'var(--radius-sm)',
                              border: `1.5px solid ${llmProvider === opt.provider ? opt.color : 'var(--border)'}`,
                              background: llmProvider === opt.provider ? `${opt.color}15` : 'transparent',
                              color: llmProvider === opt.provider ? 'var(--text)' : 'var(--text-muted)',
                              fontSize: 12, fontWeight: 600, cursor: 'pointer', transition: 'all 0.15s',
                            }}
                          >
                            {opt.label}
                          </button>
                        ))}
                      </div>
                    </div>

                    {/* .env key notice */}
                    <div style={{
                      display: 'flex', alignItems: 'center', gap: 10,
                      padding: '10px 14px', borderRadius: 'var(--radius-sm)',
                      background: 'rgba(59,130,246,0.06)', border: '1px solid rgba(59,130,246,0.18)',
                    }}>
                      <Key size={14} color="var(--primary)" style={{ flexShrink: 0 }} />
                      <p style={{ fontSize: 12, color: 'var(--text-muted)', margin: 0, lineHeight: 1.5 }}>
                        API key is read from <code style={{ fontFamily: 'var(--font-mono)', color: 'var(--primary-bright)', fontSize: 11 }}>.env</code> on the server. No key entry needed.
                      </p>
                    </div>
                  </motion.div>
                )}

                {/* AG Chat Mode */}
                {providerMode === 'ag_chat' && (
                  <motion.div
                    key="ag_chat"
                    initial={{ opacity: 0, y: 8 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -8 }}
                    style={{
                      padding: '18px 20px',
                      borderRadius: 'var(--radius)',
                      background: 'rgba(139,92,246,0.06)',
                      border: '1px solid rgba(139,92,246,0.2)',
                    }}
                  >
                    <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
                      <div style={{
                        width: 36, height: 36, borderRadius: 10, flexShrink: 0,
                        background: 'rgba(139,92,246,0.15)', border: '1px solid rgba(139,92,246,0.3)',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                      }}>
                        <MessageSquare size={16} color="var(--purple)" />
                      </div>
                      <div>
                        <div style={{ fontWeight: 700, fontSize: 14, color: 'var(--text)', marginBottom: 6 }}>
                          Antigravity IDE Agent
                        </div>
                        <p style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6, margin: 0 }}>
                          Each agent prompt is automatically processed by the Antigravity IDE agent
                          in the background. A draggable floating brain widget in the corner of your screen
                          will show you which agent is currently executing.
                        </p>
                        <div style={{ display: 'flex', gap: 6, marginTop: 10 }}>
                          <span className="badge badge-purple" style={{ fontSize: 11 }}>No API key needed</span>
                          <span className="badge badge-purple" style={{ fontSize: 11 }}>Fully Automated</span>
                          <span className="badge badge-purple" style={{ fontSize: 11 }}>Active Chat Model</span>
                        </div>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>

            {/* Error */}
            {error && (
              <div style={{ padding: '12px 16px', background: 'var(--error-bg)', border: '1px solid rgba(239,68,68,0.2)', borderRadius: 'var(--radius)', fontSize: 13, color: 'var(--error)' }}>
                {error}
              </div>
            )}
          </div>

          {/* ── Right column — config ─────────────────────────────────── */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            {/* Output Targets */}
            <div className="glass-card" style={{ padding: 24 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 18 }}>
                <CheckSquare size={16} color="var(--primary)" />
                <h3 style={{ fontSize: 15, fontWeight: 700 }}>Output Targets</h3>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                {OUTPUT_OPTIONS.map(opt => (
                  <label key={opt.id} style={{ display: 'flex', alignItems: 'center', gap: 12, cursor: 'pointer' }}>
                    <div
                      onClick={() => toggleOutput(opt.id)}
                      style={{
                        width: 20, height: 20, borderRadius: 5,
                        background: outputs[opt.id] ? `${opt.color}22` : 'transparent',
                        border: `2px solid ${outputs[opt.id] ? opt.color : 'var(--border)'}`,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        flexShrink: 0, cursor: 'pointer', transition: 'all 0.15s',
                      }}
                    >
                      {outputs[opt.id] && <div style={{ width: 8, height: 8, borderRadius: 2, background: opt.color }} />}
                    </div>
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 600, color: outputs[opt.id] ? 'var(--text)' : 'var(--text-muted)' }}>{opt.label}</div>
                      <div style={{ fontSize: 11, color: 'var(--text-dim)' }}>{opt.sub}</div>
                    </div>
                  </label>
                ))}
              </div>
            </div>

            {/* Pipeline stages summary */}
            <div className="glass-card" style={{ padding: 24 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
                <Settings size={16} color="var(--accent)" />
                <h3 style={{ fontSize: 15, fontWeight: 700 }}>Pipeline Agents</h3>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {[
                  { name: 'Parser Agent',        desc: 'XML parse + normalize',        color: '#ef4444' },
                  { name: 'Classifier Agent',    desc: 'Complexity & confidence',      color: '#f59e0b' },
                  { name: 'Translator Agent',    desc: 'PySpark + DataFusion code',    color: '#3b82f6' },
                  { name: 'Reviewer Agent',      desc: 'Code review & best practices', color: '#8b5cf6' },
                  { name: 'Tester Agent',        desc: 'pytest suite generation',      color: '#10b981' },
                  { name: 'Documentation Agent', desc: 'Migration docs + report',      color: '#06b6d4' },
                ].map((a, i) => (
                  <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <div style={{ width: 6, height: 6, borderRadius: '50%', background: a.color, flexShrink: 0 }} />
                    <div style={{ flex: 1 }}>
                      <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)' }}>{a.name}</span>
                      <span style={{ fontSize: 11, color: 'var(--text-dim)', marginLeft: 6 }}>{a.desc}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Start button */}
            <button
              className="btn btn-primary w-full"
              onClick={handleStart}
              disabled={!canStart || loading}
              style={{ padding: '14px', fontSize: 15, justifyContent: 'center', gap: 8 }}
            >
              {loading ? (
                <><div style={{ width: 16, height: 16, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.8s linear infinite' }} /> Starting...</>
              ) : (
                <><Zap size={16} /> Start Migration <ChevronRight size={14} /></>
              )}
            </button>

            {!canStart && !loading && (
              <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -8 }}>
                Upload a .dsx or .isx file to continue
              </p>
            )}
          </div>
        </div>
      </div>
    </motion.div>
  )
}
