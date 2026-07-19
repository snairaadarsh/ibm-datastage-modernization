import { useEffect, useState, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Brain, ShieldAlert, ChevronRight, X, MessageSquare,
  Flag, AlertTriangle, CheckCircle, Loader2, Send
} from 'lucide-react'
import { api } from '../lib/api.js'
import socket, { subscribeToJob, unsubscribeFromJob } from '../lib/socket.js'

export default function HumanReview() {
  const { jobId }  = useParams()
  const navigate   = useNavigate()
  const [job, setJob]               = useState(null)
  const [loading, setLoading]       = useState(false)
  const [submitted, setSubmitted]   = useState(null)   // 'proceed' | 'stop' | 'comment'
  const [showComment, setShowComment] = useState(false)
  const [comment, setComment]       = useState('')
  const commentRef                  = useRef(null)

  // ── Load job and subscribe to socket ────────────────────────────────────────
  useEffect(() => {
    api.getJob(jobId)
      .then(j => setJob(j))
      .catch(() => navigate('/upload'))

    subscribeToJob(jobId)

    // If user submitted Proceed/Comment, pipeline resumes and we redirect to pipeline view
    socket.on('pipeline:review_decision', ({ decision }) => {
      if (decision === 'stop') {
        setTimeout(() => navigate('/dashboard'), 1500)
      } else {
        setTimeout(() => navigate(`/pipeline/${jobId}`), 1200)
      }
    })

    socket.on('pipeline:completed', () => navigate(`/results/${jobId}`))
    socket.on('pipeline:rejected',  () => navigate('/dashboard'))

    return () => {
      unsubscribeFromJob(jobId)
      socket.off('pipeline:review_decision')
      socket.off('pipeline:completed')
      socket.off('pipeline:rejected')
    }
  }, [jobId])

  useEffect(() => {
    if (showComment && commentRef.current) {
      commentRef.current.focus()
    }
  }, [showComment])

  const hr = job?.humanReview

  // ── Decision handlers ───────────────────────────────────────────────────────

  const handleProceed = async () => {
    setLoading(true)
    setSubmitted('proceed')
    try {
      await api.submitHumanReviewDecision(jobId, 'proceed')
    } catch { setLoading(false); setSubmitted(null) }
  }

  const handleStop = async () => {
    setLoading(true)
    setSubmitted('stop')
    try {
      await api.submitHumanReviewDecision(jobId, 'stop')
    } catch { setLoading(false); setSubmitted(null) }
  }

  const handleComment = async () => {
    if (!comment.trim()) {
      setShowComment(true)
      return
    }
    setLoading(true)
    setSubmitted('comment')
    try {
      await api.submitHumanReviewDecision(jobId, 'comment', comment.trim())
    } catch { setLoading(false); setSubmitted(null) }
  }

  if (!job) {
    return (
      <div className="page" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
          <div style={{ width: 32, height: 32, border: '2px solid var(--border)', borderTopColor: 'var(--primary)', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 16px' }} />
          Loading review...
        </div>
      </div>
    )
  }

  const complexity  = hr?.complexity || job.classification?.complexity || 'medium'
  const confidence  = hr?.confidence ?? (job.classification?.confidence_score * 100) ?? 0
  const explanation = hr?.llmExplanation || hr?.aiReasoning || ''
  const flagged     = hr?.flaggedStages || hr?.riskAreas || []
  const ambiguity   = hr?.ambiguityFlags || []
  const recommendations = hr?.recommendations || []

  const complexityColor = complexity === 'complex' ? 'var(--error)' : 'var(--warning)'

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="page"
    >
      {/* ── Top alert banner ─────────────────────────────────────────────── */}
      <div style={{
        background:   `linear-gradient(135deg, rgba(245,158,11,0.10) 0%, rgba(239,68,68,0.05) 100%)`,
        borderBottom: '1px solid rgba(245,158,11,0.25)',
        padding:      '14px 0',
      }}>
        <div className="container" style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{
            width: 40, height: 40, borderRadius: '50%', flexShrink: 0,
            background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.3)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <ShieldAlert size={20} color="var(--warning)" />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 700, color: 'var(--warning)', fontSize: 15 }}>
              Human Review Required — {complexity.charAt(0).toUpperCase() + complexity.slice(1)} Complexity
            </div>
            <div style={{ fontSize: 13, color: 'var(--text-muted)', marginTop: 2 }}>
              <strong style={{ color: 'var(--text)' }}>{job.originalname}</strong> — LLM flagged issues that need your decision before generation proceeds.
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <span className="badge badge-warning" style={{ fontSize: 11 }}>
              {complexity.toUpperCase()}
            </span>
            <span className="badge badge-warning" style={{ fontSize: 11 }}>
              {confidence.toFixed(1)}% confidence
            </span>
          </div>
        </div>
      </div>

      <div className="container" style={{ padding: '28px 24px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 360px', gap: 24, alignItems: 'start' }}>

          {/* ── Left: LLM analysis ─────────────────────────────────────────── */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

            {/* LLM Explanation */}
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              className="glass-card"
              style={{ padding: 24, borderColor: 'rgba(139,92,246,0.25)' }}
            >
              <div style={{ display: 'flex', gap: 12, marginBottom: 18, alignItems: 'center' }}>
                <div style={{
                  width: 38, height: 38, borderRadius: 12, flexShrink: 0,
                  background: 'rgba(139,92,246,0.12)', border: '1px solid rgba(139,92,246,0.25)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  boxShadow: '0 2px 8px rgba(139,92,246,0.2)',
                }}>
                  <Brain size={18} color="var(--purple)" />
                </div>
                <div>
                  <div style={{ fontWeight: 700, fontSize: 15 }}>LLM Analysis</div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Why this job requires your decision</div>
                </div>
              </div>

              {explanation ? (
                <div style={{
                  fontSize: 14, color: 'var(--text-muted)', lineHeight: 1.75,
                  padding: '14px 18px',
                  background: 'rgba(139,92,246,0.05)',
                  borderRadius: 'var(--radius)',
                  border: '1px solid rgba(139,92,246,0.12)',
                  whiteSpace: 'pre-wrap',
                }}>
                  {explanation}
                </div>
              ) : (
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', color: 'var(--text-dim)', fontSize: 13, padding: '12px 0' }}>
                  <Loader2 size={14} style={{ animation: 'spin 1.2s linear infinite' }} />
                  Generating analysis...
                </div>
              )}
            </motion.div>

            {/* Flagged areas */}
            {(flagged.length > 0 || ambiguity.length > 0) && (
              <motion.div
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 }}
                className="glass-card"
                style={{ padding: 24, borderColor: 'rgba(245,158,11,0.2)' }}
              >
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
                  <Flag size={14} color="var(--warning)" />
                  Flagged Areas & Ambiguities
                </div>

                {flagged.length > 0 && (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: ambiguity.length > 0 ? 14 : 0 }}>
                    {flagged.map((stage, i) => (
                      <div key={i} style={{
                        display: 'flex', alignItems: 'center', gap: 10, padding: '8px 12px',
                        background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.2)', borderRadius: 8,
                      }}>
                        <div style={{ width: 6, height: 6, borderRadius: '50%', background: complexityColor, flexShrink: 0 }} />
                        <span style={{ fontSize: 12, fontFamily: 'var(--font-mono)', color: 'var(--warning)' }}>{stage}</span>
                      </div>
                    ))}
                  </div>
                )}

                {ambiguity.length > 0 && (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                    {ambiguity.map((flag, i) => (
                      <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
                        <AlertTriangle size={11} color="var(--warning)" style={{ flexShrink: 0, marginTop: 2 }} />
                        <span style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.5 }}>{flag}</span>
                      </div>
                    ))}
                  </div>
                )}
              </motion.div>
            )}

            {/* Recommendations */}
            {recommendations.length > 0 && (
              <motion.div
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 }}
                className="glass-card"
                style={{ padding: 24 }}
              >
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
                  <CheckCircle size={14} color="var(--primary)" /> LLM Recommendations
                </div>
                {recommendations.map((rec, i) => (
                  <div key={i} style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'flex-start' }}>
                    <div style={{
                      width: 18, height: 18, borderRadius: '50%', flexShrink: 0, marginTop: 1,
                      background: 'rgba(99,179,237,0.1)', border: '1px solid rgba(99,179,237,0.2)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      fontSize: 9, color: 'var(--primary)', fontWeight: 700,
                    }}>{i + 1}</div>
                    <span style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.55 }}>{rec}</span>
                  </div>
                ))}
              </motion.div>
            )}
          </div>

          {/* ── Right: Decision panel ───────────────────────────────────────── */}
          <div style={{ position: 'sticky', top: 80 }}>
            <div className="glass-card" style={{ padding: 24 }}>
              <h3 style={{ fontSize: 16, fontWeight: 700, marginBottom: 6 }}>Your Decision</h3>
              <p style={{ fontSize: 12, color: 'var(--text-dim)', marginBottom: 24, lineHeight: 1.5 }}>
                Review the LLM analysis above, then choose how to proceed.
              </p>

              <AnimatePresence mode="wait">
                {!submitted ? (
                  <motion.div
                    key="buttons"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    style={{ display: 'flex', flexDirection: 'column', gap: 12 }}
                  >
                    {/* ── Proceed ── */}
                    <button
                      className="btn btn-success w-full"
                      onClick={handleProceed}
                      disabled={loading}
                      style={{ justifyContent: 'center', gap: 8, padding: '14px', fontSize: 14 }}
                    >
                      <CheckCircle size={15} />
                      Proceed
                    </button>
                    <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -6 }}>
                      LLM generates the code as-is, ignoring flagged issues
                    </p>

                    <div style={{ borderTop: '1px solid var(--border)', margin: '4px 0' }} />

                    {/* ── Add Comment ── */}
                    <button
                      className="btn btn-warning w-full"
                      onClick={() => setShowComment(v => !v)}
                      disabled={loading}
                      style={{ justifyContent: 'center', gap: 8, padding: '13px', fontSize: 14 }}
                    >
                      <MessageSquare size={15} />
                      Add Comment
                    </button>
                    <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -6 }}>
                      Give guidance — the LLM incorporates your instruction into all agents
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
                            rows={5}
                            placeholder={`Tell the LLM what to do differently...\n\nExamples:\n• "Handle SCD Type-2 using window functions"\n• "Use BigQuery as the target, not GCS"\n• "Skip the full outer join — use left join instead"`}
                            value={comment}
                            onChange={e => setComment(e.target.value)}
                            style={{ fontSize: 12.5, resize: 'vertical', marginBottom: 8, lineHeight: 1.6 }}
                          />
                          <button
                            className="btn btn-warning w-full btn-sm"
                            onClick={handleComment}
                            disabled={!comment.trim() || loading}
                            style={{ justifyContent: 'center', gap: 6 }}
                          >
                            <Send size={13} />
                            Submit & Continue with LLM
                          </button>
                        </motion.div>
                      )}
                    </AnimatePresence>

                    <div style={{ borderTop: '1px solid var(--border)', margin: '4px 0' }} />

                    {/* ── Stop ── */}
                    <button
                      className="btn btn-danger w-full"
                      onClick={handleStop}
                      disabled={loading}
                      style={{ justifyContent: 'center', gap: 8, padding: '13px', fontSize: 14 }}
                    >
                      <X size={15} />
                      Stop Migration
                    </button>
                    <p style={{ fontSize: 11, color: 'var(--text-dim)', textAlign: 'center', marginTop: -6 }}>
                      Cancel this job — it will be marked as failed
                    </p>
                  </motion.div>
                ) : (
                  <motion.div
                    key="submitted"
                    initial={{ opacity: 0, scale: 0.92 }}
                    animate={{ opacity: 1, scale: 1 }}
                    style={{
                      textAlign: 'center', padding: '28px 16px', borderRadius: 'var(--radius)',
                      background: submitted === 'stop'
                        ? 'rgba(239,68,68,0.06)'
                        : 'rgba(16,185,129,0.06)',
                      border: `1px solid ${submitted === 'stop' ? 'rgba(239,68,68,0.2)' : 'rgba(16,185,129,0.2)'}`,
                    }}
                  >
                    {submitted === 'stop'   && <X size={36} color="var(--error)"   style={{ margin: '0 auto 12px' }} />}
                    {submitted === 'proceed' && <CheckCircle size={36} color="var(--success)" style={{ margin: '0 auto 12px' }} />}
                    {submitted === 'comment' && <Brain size={36} color="var(--purple)" style={{ margin: '0 auto 12px' }} />}
                    <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 6 }}>
                      {submitted === 'proceed' && 'Approved — resuming pipeline...'}
                      {submitted === 'stop'    && 'Migration stopped'}
                      {submitted === 'comment' && 'Guidance sent — LLM is processing...'}
                    </div>
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
                      <Loader2 size={12} style={{ animation: 'spin 1.2s linear infinite' }} />
                      {submitted === 'stop' ? 'Redirecting to dashboard...' : 'Redirecting to pipeline view...'}
                    </div>
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
