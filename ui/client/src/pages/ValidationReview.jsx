import { useEffect, useState, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Brain, CheckCircle, Loader2, Send, MessageSquare,
  AlertTriangle, ShieldCheck, BarChart2, Zap, ChevronRight,
} from 'lucide-react'
import { api } from '../lib/api.js'
import socket, { subscribeToJob, unsubscribeFromJob } from '../lib/socket.js'

export default function ValidationReview() {
  const { jobId }   = useParams()
  const navigate    = useNavigate()
  const [job, setJob]               = useState(null)
  const [vr, setVr]                 = useState(null)   // validation_review payload
  const [loading, setLoading]       = useState(false)
  const [submitted, setSubmitted]   = useState(null)
  const [showComment, setShowComment] = useState(false)
  const [comment, setComment]       = useState('')
  const commentRef                  = useRef(null)

  useEffect(() => {
    api.getJob(jobId)
      .then(j => {
        setJob(j)
        // If job already has a validationReview payload saved, use it
        if (j.validationReview) setVr(j.validationReview)
      })
      .catch(() => navigate('/upload'))

    subscribeToJob(jobId)

    socket.on('pipeline:validation_review', (payload) => {
      setVr(payload)
      setSubmitted(null)
      setLoading(false)
      setShowComment(false)
      setComment('')
    })

    socket.on('pipeline:validation_decision', ({ decision }) => {
      // Pipeline received the decision — show processing state
      if (decision !== 'proceed') {
        setSubmitted('processing')
      }
    })

    socket.on('pipeline:completed', () => navigate(`/results/${jobId}`))
    socket.on('pipeline:rejected',  () => navigate('/dashboard'))

    return () => {
      unsubscribeFromJob(jobId)
      socket.off('pipeline:validation_review')
      socket.off('pipeline:validation_decision')
      socket.off('pipeline:completed')
      socket.off('pipeline:rejected')
    }
  }, [jobId])

  useEffect(() => {
    if (showComment && commentRef.current) commentRef.current.focus()
  }, [showComment])

  const submit = async (decision, userComment = '') => {
    setLoading(true)
    setSubmitted(decision)
    try {
      await api.submitValidationDecision(jobId, decision, userComment)
      if (decision === 'proceed') {
        // Redirect back to pipeline view to watch remainder
        setTimeout(() => navigate(`/pipeline/${jobId}`), 1000)
      }
    } catch {
      setLoading(false)
      setSubmitted(null)
    }
  }

  if (!job) {
    return (
      <div className="page" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
          <div style={{ width: 32, height: 32, border: '2px solid var(--border)', borderTopColor: 'var(--primary)', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 16px' }} />
          Loading validation review...
        </div>
      </div>
    )
  }

  if (!vr) {
    return (
      <div className="page" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
          <Loader2 size={28} style={{ animation: 'spin 1.2s linear infinite', margin: '0 auto 16px' }} />
          Waiting for validation results...
        </div>
      </div>
    )
  }

  const score          = vr.score ?? 0
  const summary        = vr.summary ?? ''
  const issues         = vr.issues ?? []
  const strengths      = vr.strengths ?? []
  const loopCount      = vr.loopCount ?? 0
  const maxLoops       = vr.maxLoops ?? 3
  const canRetry       = vr.canRetry ?? (loopCount < maxLoops)
  const criticalIssues = vr.critical_issues ?? false

  const scoreColor = score >= 90 ? 'var(--success)' : score >= 70 ? 'var(--warning)' : 'var(--error)'
  const scoreGradient = score >= 90
    ? 'linear-gradient(90deg, var(--success), #34d399)'
    : score >= 70
    ? 'linear-gradient(90deg, var(--warning), #fbbf24)'
    : 'linear-gradient(90deg, var(--error), #f87171)'

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="page"
    >
      {/* Top alert banner */}
      <div style={{
        background:   'linear-gradient(135deg, rgba(239,68,68,0.10) 0%, rgba(245,158,11,0.05) 100%)',
        borderBottom: '1px solid rgba(239,68,68,0.25)',
        padding:      '14px 0',
      }}>
        <div className="container" style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{
            width: 40, height: 40, borderRadius: '50%', flexShrink: 0,
            background: 'rgba(239,68,68,0.12)', border: '1px solid rgba(239,68,68,0.3)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <BarChart2 size={20} color="var(--error)" />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 700, color: 'var(--error)', fontSize: 15 }}>
              Validation Review Required
              {loopCount > 0 && (
                <span style={{ marginLeft: 8, fontSize: 12, fontWeight: 500, color: 'var(--text-muted)' }}>
                  — Retry {loopCount}/{maxLoops}
                </span>
              )}
            </div>
            <div style={{ fontSize: 13, color: 'var(--text-muted)', marginTop: 2 }}>
              <strong style={{ color: 'var(--text)' }}>{job.originalname}</strong> — AI validation score is below 90%. Your decision is needed.
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <span className="badge" style={{
              background: `${scoreColor}22`, color: scoreColor,
              border: `1px solid ${scoreColor}55`, fontSize: 13, fontWeight: 800, padding: '4px 12px',
            }}>
              {score}%
            </span>
            {criticalIssues && (
              <span className="badge badge-error" style={{ fontSize: 11 }}>Critical Issues</span>
            )}
          </div>
        </div>
      </div>

      <div className="container" style={{ padding: '28px 24px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 360px', gap: 24, alignItems: 'start' }}>

          {/* Left: Validation analysis */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

            {/* Score card */}
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              className="glass-card"
              style={{ padding: 24, borderColor: `${scoreColor}44` }}
            >
              <div style={{ display: 'flex', gap: 12, marginBottom: 20, alignItems: 'center' }}>
                <div style={{
                  width: 38, height: 38, borderRadius: 12, flexShrink: 0,
                  background: `${scoreColor}18`, border: `1px solid ${scoreColor}44`,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  <ShieldCheck size={18} color={scoreColor} />
                </div>
                <div>
                  <div style={{ fontWeight: 700, fontSize: 15 }}>Validation Score</div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Structural match between source job and generated output</div>
                </div>
                <div style={{
                  marginLeft: 'auto',
                  fontFamily: 'var(--font-head)', fontSize: 42, fontWeight: 900,
                  color: scoreColor, lineHeight: 1,
                }}>
                  {score}<span style={{ fontSize: 18 }}>%</span>
                </div>
              </div>

              {/* Score bar */}
              <div style={{ height: 8, background: 'var(--border)', borderRadius: 99, marginBottom: 12, overflow: 'hidden' }}>
                <motion.div
                  initial={{ width: 0 }}
                  animate={{ width: `${score}%` }}
                  transition={{ duration: 0.8, ease: 'easeOut' }}
                  style={{ height: '100%', borderRadius: 99, background: scoreGradient }}
                />
              </div>

              <div style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.6, padding: '12px 16px', background: 'rgba(255,255,255,0.03)', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                {summary}
              </div>
            </motion.div>

            {/* Issues */}
            {issues.length > 0 && (
              <motion.div
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 }}
                className="glass-card"
                style={{ padding: 24, borderColor: 'rgba(239,68,68,0.2)' }}
              >
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
                  <AlertTriangle size={14} color="var(--error)" />
                  Issues Found ({issues.length})
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {issues.map((issue, i) => (
                    <div key={i} style={{
                      display: 'flex', alignItems: 'flex-start', gap: 10, padding: '8px 12px',
                      background: 'rgba(239,68,68,0.05)', border: '1px solid rgba(239,68,68,0.18)', borderRadius: 8,
                    }}>
                      <div style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--error)', flexShrink: 0, marginTop: 5 }} />
                      <span style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.55 }}>{issue}</span>
                    </div>
                  ))}
                </div>
              </motion.div>
            )}

            {/* Strengths */}
            {strengths.length > 0 && (
              <motion.div
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 }}
                className="glass-card"
                style={{ padding: 24 }}
              >
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
                  <CheckCircle size={14} color="var(--success)" />
                  What Was Correct ({strengths.length})
                </div>
                {strengths.map((s, i) => (
                  <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'flex-start', marginBottom: 8 }}>
                    <ChevronRight size={12} color="var(--success)" style={{ flexShrink: 0, marginTop: 2 }} />
                    <span style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.55 }}>{s}</span>
                  </div>
                ))}
              </motion.div>
            )}
          </div>

          {/* Right: Decision panel */}
          <div style={{ position: 'sticky', top: 80 }}>
            <div className="glass-card" style={{ padding: 24 }}>
              <h3 style={{ fontSize: 16, fontWeight: 700, marginBottom: 6 }}>Your Decision</h3>
              <p style={{ fontSize: 12, color: 'var(--text-dim)', marginBottom: 24, lineHeight: 1.5 }}>
                Choose how to handle the validation score below 90%.
              </p>

              <AnimatePresence mode="wait">
                {submitted === 'processing' ? (
                  <motion.div
                    key="processing"
                    initial={{ opacity: 0, scale: 0.92 }}
                    animate={{ opacity: 1, scale: 1 }}
                    style={{
                      textAlign: 'center', padding: '28px 16px', borderRadius: 'var(--radius)',
                      background: 'rgba(139,92,246,0.06)',
                      border: '1px solid rgba(139,92,246,0.2)',
                    }}
                  >
                    <Brain size={36} color="var(--purple)" style={{ margin: '0 auto 12px' }} />
                    <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 6 }}>LLM proposing fix...</div>
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
                      <Loader2 size={12} style={{ animation: 'spin 1.2s linear infinite' }} />
                      Re-translating with AI fix
                    </div>
                  </motion.div>
                ) : submitted === 'proceed' ? (
                  <motion.div
                    key="accepted"
                    initial={{ opacity: 0, scale: 0.92 }}
                    animate={{ opacity: 1, scale: 1 }}
                    style={{
                      textAlign: 'center', padding: '28px 16px', borderRadius: 'var(--radius)',
                      background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.2)',
                    }}
                  >
                    <CheckCircle size={36} color="var(--success)" style={{ margin: '0 auto 12px' }} />
                    <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 6 }}>Accepted — continuing pipeline</div>
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
                      <Loader2 size={12} style={{ animation: 'spin 1.2s linear infinite' }} />
                      Redirecting...
                    </div>
                  </motion.div>
                ) : (
                  <motion.div
                    key="buttons"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    style={{ display: 'flex', flexDirection: 'column', gap: 12 }}
                  >
                    {/* Proceed as-is */}
                    <button
                      className="btn btn-success w-full"
                      onClick={() => submit('proceed')}
                      disabled={loading}
                      style={{ justifyContent: 'center', gap: 8, padding: '14px', fontSize: 14 }}
                    >
                      <CheckCircle size={15} />
                      Proceed Anyway
                    </button>
                    <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -6 }}>
                      Accept current score and continue to final report
                    </p>

                    <div style={{ borderTop: '1px solid var(--border)', margin: '4px 0' }} />

                    {/* AI Fix */}
                    {canRetry && (
                      <>
                        <button
                          className="btn btn-warning w-full"
                          onClick={() => submit('ai_fix')}
                          disabled={loading}
                          style={{ justifyContent: 'center', gap: 8, padding: '13px', fontSize: 14 }}
                        >
                          <Zap size={15} />
                          Sort Issue (AI Propose)
                        </button>
                        <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -6 }}>
                          AI proposes a fix, re-translates, and re-validates automatically
                        </p>

                        <div style={{ borderTop: '1px solid var(--border)', margin: '4px 0' }} />
                      </>
                    )}

                    {/* Add Comment */}
                    {canRetry && (
                      <>
                        <button
                          className="btn w-full"
                          onClick={() => setShowComment(v => !v)}
                          disabled={loading}
                          style={{
                            justifyContent: 'center', gap: 8, padding: '13px', fontSize: 14,
                            background: 'rgba(139,92,246,0.12)', border: '1px solid rgba(139,92,246,0.3)',
                            color: 'var(--purple)',
                          }}
                        >
                          <MessageSquare size={15} />
                          Add Comment
                        </button>
                        <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -6 }}>
                          Guide the AI fix — your comment is merged with its solution
                        </p>

                        <AnimatePresence>
                          {showComment && (
                            <motion.div
                              initial={{ opacity: 0, height: 0 }}
                              animate={{ opacity: 1, height: 'auto' }}
                              exit={{ opacity: 0, height: 0 }}
                              style={{ overflow: 'hidden' }}
                            >
                              <textarea
                                ref={commentRef}
                                className="input"
                                rows={4}
                                placeholder={`Tell the AI what to fix...\n\nExamples:\n• "The join key should be CUSTOMER_ID not ID"\n• "Handle nulls in the AGE column with 0 default"`}
                                value={comment}
                                onChange={e => setComment(e.target.value)}
                                style={{ fontSize: 12.5, resize: 'vertical', marginBottom: 8, lineHeight: 1.6 }}
                              />
                              <button
                                className="btn w-full btn-sm"
                                onClick={() => submit('comment', comment.trim())}
                                disabled={!comment.trim() || loading}
                                style={{
                                  justifyContent: 'center', gap: 6,
                                  background: 'rgba(139,92,246,0.15)', border: '1px solid rgba(139,92,246,0.3)',
                                  color: 'var(--purple)',
                                }}
                              >
                                <Send size={13} />
                                Submit &amp; Re-validate with AI
                              </button>
                            </motion.div>
                          )}
                        </AnimatePresence>
                      </>
                    )}

                    {/* Max loops reached notice */}
                    {!canRetry && (
                      <div style={{
                        padding: '12px 14px', borderRadius: 'var(--radius)',
                        background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.2)',
                        fontSize: 12, color: 'var(--warning)', lineHeight: 1.5,
                      }}>
                        <strong>Max retries reached ({maxLoops}/{maxLoops}).</strong> You can only proceed at this point.
                      </div>
                    )}
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  )
}
