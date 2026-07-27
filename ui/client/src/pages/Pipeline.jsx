import { useEffect, useRef, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Clock, FileCode, BarChart2, Activity, AlertTriangle, CheckCircle, ArrowLeft, ArrowRight } from 'lucide-react'
import PipelineGraph from '../components/PipelineGraph.jsx'
import socket, { subscribeToJob, unsubscribeFromJob } from '../lib/socket.js'
import { api } from '../lib/api.js'

export default function Pipeline() {
  const { jobId } = useParams()
  const navigate = useNavigate()
  const [job, setJob] = useState(null)
  const [logs, setLogs] = useState([])
  const [stages, setStages] = useState([])
  const [classification, setClassification] = useState(null)
  const [elapsed, setElapsed] = useState(0)
  const [status, setStatus] = useState('processing')
  const logEndRef = useRef(null)
  const timerRef = useRef(null)

  useEffect(() => {
    // Load initial job state
    api.getJob(jobId).then(j => {
      setJob(j)
      setStages(j.stages || [])
      setLogs(j.logs || [])
      setStatus(j.status)
      setClassification(j.classification)
    }).catch(() => navigate('/upload'))

    // Subscribe to Socket.io events
    subscribeToJob(jobId)

    socket.on('pipeline:started', () => {
      setStatus('processing')
      timerRef.current = setInterval(() => setElapsed(e => e + 0.1), 100)
    })

    socket.on('pipeline:stage', ({ stageId, status: s }) => {
      setStages(prev => prev.map(st => st.id === stageId ? { ...st, status: s } : st))
    })

    socket.on('pipeline:log', (entry) => {
      setLogs(prev => [...prev, entry])
    })

    socket.on('pipeline:classification', (cls) => {
      setClassification(cls)
    })

    socket.on('pipeline:human_review', ({ humanReview }) => {
      clearInterval(timerRef.current)
      setStatus('human_review')
      setTimeout(() => navigate(`/review/${jobId}`), 1500)
    })

    socket.on('pipeline:validation_review', (payload) => {
      clearInterval(timerRef.current)
      setStatus('validation_review')
      setTimeout(() => navigate(`/validation/${jobId}`), 1500)
    })

    socket.on('pipeline:completed', ({ report }) => {
      clearInterval(timerRef.current)
      setStatus('completed')
      setTimeout(() => navigate(`/results/${jobId}`), 1800)
    })

    socket.on('pipeline:rejected', () => {
      clearInterval(timerRef.current)
      setStatus('failed')
    })

    // Start timer for fresh jobs
    timerRef.current = setInterval(() => setElapsed(e => e + 0.1), 100)

    return () => {
      unsubscribeFromJob(jobId)
      clearInterval(timerRef.current)
      socket.off('pipeline:started')
      socket.off('pipeline:stage')
      socket.off('pipeline:log')
      socket.off('pipeline:classification')
      socket.off('pipeline:human_review')
      socket.off('pipeline:validation_review')
      socket.off('pipeline:completed')
      socket.off('pipeline:rejected')
    }
  }, [jobId])

  // Auto-scroll logs
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  const completedCount = stages.filter(s => s.status === 'done').length
  const progress = stages.length ? (completedCount / stages.length) * 100 : 0

  const statusConfig = {
    processing:       { label: 'Processing', color: 'var(--primary)',  dot: 'dot-blue', badge: 'badge-info' },
    human_review:     { label: 'Review Required', color: 'var(--warning)', dot: 'dot-warning', badge: 'badge-warning' },
    validation_review:{ label: 'Validation Review', color: 'var(--error)', dot: 'dot-error', badge: 'badge-error' },
    completed:        { label: 'Completed', color: 'var(--success)', dot: 'dot-success', badge: 'badge-success' },
    failed:           { label: 'Failed', color: 'var(--error)', dot: 'dot-error', badge: 'badge-error' },
  }
  const sc = statusConfig[status] || statusConfig.processing

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="page"
    >
      <div className="container" style={{ padding: '40px 24px' }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 40 }}>
          <div>
            <div style={{ display: 'flex', gap: 10, marginBottom: 12 }}>
              <button className="btn btn-ghost btn-sm" onClick={() => navigate('/dashboard')} style={{ gap: 6, padding: '6px 12px' }}>
                <ArrowLeft size={13} /> Back to Dashboard
              </button>
              {status === 'completed' && (
                <button className="btn btn-primary btn-sm" onClick={() => navigate(`/results/${jobId}`)} style={{ gap: 6, padding: '6px 12px' }}>
                  View Results & Artifacts <ArrowRight size={13} />
                </button>
              )}
            </div>
            <div className="section-label">Pipeline Execution</div>
            <h1 style={{ fontSize: 28, fontWeight: 800, display: 'flex', alignItems: 'center', gap: 12 }}>
              <FileCode size={24} color="var(--primary)" />
              {job?.originalname || 'Processing...'}
            </h1>
            <div style={{ display: 'flex', gap: 12, marginTop: 10, alignItems: 'center' }}>
              <span className={`badge ${sc.badge}`}>
                <span className={`dot ${sc.dot} dot-pulse`} />
                {sc.label}
              </span>
              <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 13, color: 'var(--text-muted)' }}>
                <Clock size={13} />
                {elapsed.toFixed(1)}s
              </span>
              {classification && (
                <span className={`badge ${classification.complexity === 'Complex' ? 'badge-warning' : classification.complexity === 'Simple' ? 'badge-success' : 'badge-info'}`}>
                  {classification.complexity}
                </span>
              )}
            </div>
          </div>

          {/* Confidence gauge */}
          {classification && (
            <motion.div
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              className="glass-card"
              style={{ padding: '20px 28px', textAlign: 'center', minWidth: 140 }}
            >
              <div style={{ fontSize: 11, color: 'var(--text-dim)', marginBottom: 8, letterSpacing: '0.08em', textTransform: 'uppercase' }}>Confidence</div>
              <div style={{ fontFamily: 'var(--font-head)', fontSize: 36, fontWeight: 800, color: classification.confidenceScore >= 85 ? 'var(--success)' : classification.confidenceScore >= 70 ? 'var(--warning)' : 'var(--error)' }}>
                {classification.confidenceScore.toFixed(1)}<span style={{ fontSize: 16 }}>%</span>
              </div>
              <div style={{ marginTop: 10, height: 4, background: 'var(--border)', borderRadius: 99 }}>
                <div style={{ height: '100%', width: `${classification.confidenceScore}%`, borderRadius: 99, background: classification.confidenceScore >= 85 ? 'linear-gradient(90deg, var(--success), #34d399)' : 'linear-gradient(90deg, var(--warning), #fbbf24)', transition: 'width 0.8s var(--ease-out)' }} />
              </div>
            </motion.div>
          )}
        </div>

        {/* Overall progress */}
        <div style={{ marginBottom: 32 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>Overall Progress</span>
            <span style={{ fontSize: 13, color: 'var(--text-muted)' }}>{completedCount} / {stages.length} stages</span>
          </div>
          <div className="progress-bar" style={{ height: 6 }}>
            <div className={`progress-fill ${status === 'completed' ? 'success' : ''}`} style={{ width: `${progress}%` }} />
          </div>
        </div>

        {/* Pipeline node graph */}
        <div className="glass-card" style={{ padding: '24px 32px', marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <Activity size={15} color="var(--primary)" />
            <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text)' }}>Pipeline Stages</span>
          </div>
          <PipelineGraph stages={stages} />
        </div>

        {/* Log terminal + metrics */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 280px', gap: 20, height: 360 }}>
          {/* Log terminal */}
          <div className="glass-card" style={{ overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
            <div style={{
              padding: '12px 16px',
              borderBottom: '1px solid var(--border)',
              display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0,
            }}>
              <div style={{ display: 'flex', gap: 5 }}>
                {['#EF4444','#F59E0B','#10B981'].map((c, i) => <div key={i} style={{ width: 8, height: 8, borderRadius: '50%', background: c, opacity: 0.6 }} />)}
              </div>
              <span style={{ fontSize: 12, color: 'var(--text-dim)', fontFamily: 'var(--font-mono)' }}>pipeline.log</span>
            </div>
            <div className="log-terminal" style={{ flex: 1 }}>
              {logs.length === 0 && (
                <div style={{ color: 'var(--text-dim)', fontStyle: 'italic' }}>Initializing pipeline...</div>
              )}
              {logs.map((log, i) => (
                <div key={i} className="log-entry">
                  <span className="log-ts">{new Date(log.ts).toLocaleTimeString()}</span>
                  <span className={`log-msg ${log.level}`}>
                    {log.level === 'success' && '✓ '}
                    {log.level === 'warning' && '⚠ '}
                    {log.level === 'error' && '✗ '}
                    {log.message}
                  </span>
                </div>
              ))}
              <div ref={logEndRef} />
            </div>
          </div>

          {/* Stage status cards */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8, overflowY: 'auto' }}>
            {stages.map(stage => {
              const colors = { done: 'var(--success)', active: 'var(--primary)', error: 'var(--error)', pending: 'var(--text-dim)', human_review: 'var(--warning)' }
              const c = colors[stage.status] || colors.pending
              return (
                <div
                  key={stage.id}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 12,
                    padding: '10px 14px',
                    background: stage.status === 'active' ? 'rgba(59,130,246,0.06)' : stage.status === 'done' ? 'rgba(16,185,129,0.04)' : 'rgba(255,255,255,0.02)',
                    border: `1px solid ${stage.status === 'active' ? 'rgba(59,130,246,0.2)' : stage.status === 'done' ? 'rgba(16,185,129,0.15)' : 'var(--border)'}`,
                    borderRadius: 'var(--radius)',
                    transition: 'all 0.3s',
                  }}
                >
                  <div style={{ width: 8, height: 8, borderRadius: '50%', background: c, flexShrink: 0, ...(stage.status === 'active' ? { animation: 'pulse-dot 1s infinite' } : {}) }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: stage.status === 'pending' ? 'var(--text-dim)' : 'var(--text)' }}>{stage.label}</div>
                    {stage.duration && <div style={{ fontSize: 10, color: 'var(--success)', marginTop: 1 }}>{stage.duration}s</div>}
                  </div>
                  <div style={{ fontSize: 10, color: c, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{stage.status}</div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Human review banner */}
        {status === 'human_review' && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            style={{
              marginTop: 24,
              padding: '20px 24px',
              background: 'rgba(245,158,11,0.08)',
              border: '1px solid rgba(245,158,11,0.3)',
              borderRadius: 'var(--radius-lg)',
              display: 'flex', alignItems: 'center', gap: 16,
            }}
          >
            <AlertTriangle size={24} color="var(--warning)" style={{ flexShrink: 0 }} />
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 700, color: 'var(--warning)', marginBottom: 4 }}>Human Review Required</div>
              <div style={{ fontSize: 14, color: 'var(--text-muted)' }}>This job was classified as <strong>Complex</strong>. Redirecting you to the human review screen...</div>
            </div>
            <div style={{ width: 20, height: 20, border: '2px solid rgba(245,158,11,0.3)', borderTopColor: 'var(--warning)', borderRadius: '50%', animation: 'spin 0.8s linear infinite', flexShrink: 0 }} />
          </motion.div>
        )}

        {/* Completed banner */}
        {status === 'completed' && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            style={{
              marginTop: 24,
              padding: '20px 24px',
              background: 'rgba(16,185,129,0.08)',
              border: '1px solid rgba(16,185,129,0.3)',
              borderRadius: 'var(--radius-lg)',
              display: 'flex', alignItems: 'center', gap: 16,
            }}
          >
            <CheckCircle size={24} color="var(--success)" style={{ flexShrink: 0 }} />
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 700, color: 'var(--success)', marginBottom: 4 }}>Migration Complete!</div>
              <div style={{ fontSize: 14, color: 'var(--text-muted)' }}>All outputs generated. Loading results...</div>
            </div>
            <div style={{ width: 20, height: 20, border: '2px solid rgba(16,185,129,0.3)', borderTopColor: 'var(--success)', borderRadius: '50%', animation: 'spin 0.8s linear infinite', flexShrink: 0 }} />
          </motion.div>
        )}
      </div>
    </motion.div>
  )
}
