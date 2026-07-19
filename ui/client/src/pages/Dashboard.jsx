import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { CheckCircle, Clock, AlertTriangle, X, BarChart2, Zap, FileCode, TrendingUp, Activity, Plus, ArrowRight, Trash2 } from 'lucide-react'
import KPICard from '../components/KPICard.jsx'
import { api } from '../lib/api.js'

function StatusBadge({ status }) {
  const cfg = {
    completed:    { cls: 'badge-success', icon: CheckCircle, label: 'Completed' },
    human_review: { cls: 'badge-warning', icon: AlertTriangle, label: 'In Review' },
    processing:   { cls: 'badge-info',    icon: Activity,      label: 'Processing' },
    failed:       { cls: 'badge-error',   icon: X,             label: 'Failed' },
  }[status] || { cls: 'badge-muted', icon: Clock, label: status }

  return (
    <span className={`badge ${cfg.cls}`} style={{ gap: 4 }}>
      <cfg.icon size={10} />
      {cfg.label}
    </span>
  )
}

function ComplexityBadge({ complexity }) {
  const cfg = {
    simple:  { cls: 'badge-success', label: 'Simple' },
    medium:  { cls: 'badge-info',    label: 'Medium' },
    complex: { cls: 'badge-warning', label: 'Complex' },
  }[complexity?.toLowerCase()] || { cls: 'badge-muted', label: complexity || 'Unknown' }

  return <span className={`badge ${cfg.cls}`}>{cfg.label}</span>
}

export default function Dashboard() {
  const navigate = useNavigate()
  const [allJobs, setAllJobs] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.getJobs()
      .then(jobs => { setAllJobs(jobs); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const handleDeleteJob = (jobId, e) => {
    e.stopPropagation()
    if (window.confirm('Are you sure you want to delete this migration record?')) {
      api.deleteJob(jobId)
        .then(() => {
          setAllJobs(prev => prev.filter(j => j.id !== jobId))
        })
        .catch(err => {
          alert('Failed to delete job: ' + err.message)
        })
    }
  }

  const completedJobs = allJobs.filter(j => j.status === 'completed')
  const successRate = allJobs.length ? Math.round((completedJobs.length / allJobs.length) * 100) : 0
  
  // Calculate average confidence score dynamically
  const confidenceSum = completedJobs.reduce((sum, j) => {
    const score = j.classification?.confidenceScore || j.classification?.confidence_score || 0
    // Handle both 0-1 and 0-100 ranges
    return sum + (score <= 1 ? score * 100 : score)
  }, 0)
  const avgConfidence = completedJobs.length ? (confidenceSum / completedJobs.length).toFixed(1) : '0.0'

  // Calculate complexity distribution dynamically
  const counts = { simple: 0, medium: 0, complex: 0 }
  allJobs.forEach(j => {
    const c = j.classification?.complexity?.toLowerCase()
    if (c in counts) counts[c]++
  })

  const complexityData = [
    { label: 'Simple',  count: counts.simple,  pct: allJobs.length ? Math.round((counts.simple / allJobs.length) * 100) : 0, color: 'var(--success)' },
    { label: 'Medium',  count: counts.medium,  pct: allJobs.length ? Math.round((counts.medium / allJobs.length) * 100) : 0, color: 'var(--primary)' },
    { label: 'Complex', count: counts.complex, pct: allJobs.length ? Math.round((counts.complex / allJobs.length) * 100) : 0, color: 'var(--warning)' },
  ]

  const timeAgo = (iso) => {
    const diff = Date.now() - new Date(iso)
    const h = Math.floor(diff / 3600000)
    if (h < 1) return `${Math.max(1, Math.floor(diff / 60000))}m ago`
    if (h < 24) return `${h}h ago`
    return `${Math.floor(h / 24)}d ago`
  }

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
            <div className="section-label">Analytics</div>
            <h1 style={{ fontSize: 32, fontWeight: 800 }}>Migration Dashboard</h1>
            <p style={{ color: 'var(--text-muted)', marginTop: 6 }}>Track all DataStage ETL modernization jobs and their outcomes.</p>
          </div>
          <button className="btn btn-primary" onClick={() => navigate('/upload')} style={{ gap: 7 }}>
            <Plus size={15} /> New Migration
          </button>
        </div>

        {/* KPI Cards */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 32 }}>
          <KPICard label="Total Jobs" value={allJobs.length} icon={FileCode} color="var(--primary)" delay={0} sub="All-time migrations" />
          <KPICard label="Success Rate" value={successRate} unit="%" icon={TrendingUp} color="var(--success)" delay={80} sub={`${completedJobs.length} completed`} />
          <KPICard label="Avg Confidence" value={avgConfidence} unit="%" icon={BarChart2} color="var(--accent)" delay={160} sub="Across completed jobs" />
          <KPICard label="Formats Output" value="2" icon={Zap} color="var(--purple)" delay={240} sub="PySpark + DataFusion" />
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 280px', gap: 24, alignItems: 'start' }}>
          {/* Job history table */}
          <div className="glass-card" style={{ overflow: 'hidden' }}>
            <div style={{ padding: '20px 24px', borderBottom: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h2 style={{ fontSize: 16, fontWeight: 700, fontFamily: 'var(--font)' }}>Migration History</h2>
              <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{allJobs.length} total jobs</span>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Job File</th>
                    <th>Status</th>
                    <th>Complexity</th>
                    <th>Confidence</th>
                    <th>Time</th>
                    <th>Created</th>
                    <th style={{ width: 80 }}></th>
                  </tr>
                </thead>
                <tbody>
                  {allJobs.map((job) => {
                    const score = job.classification?.confidenceScore || job.classification?.confidence_score
                    const displayScore = score != null ? (score <= 1 ? score * 100 : score) : null

                    return (
                      <tr key={job.id} style={{ cursor: job.status === 'completed' ? 'pointer' : 'default' }}>
                        <td>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                            <div style={{ width: 32, height: 32, borderRadius: 8, background: 'rgba(59,130,246,0.08)', border: '1px solid rgba(59,130,246,0.15)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                              <FileCode size={14} color="var(--primary)" />
                            </div>
                            <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 200 }}>
                              {job.originalname}
                            </span>
                          </div>
                        </td>
                        <td><StatusBadge status={job.status} /></td>
                        <td><ComplexityBadge complexity={job.classification?.complexity} /></td>
                        <td>
                          {displayScore != null ? (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                              <div style={{ width: 60, height: 4, background: 'var(--border)', borderRadius: 99, overflow: 'hidden' }}>
                                <div style={{
                                  height: '100%', borderRadius: 99,
                                  width: `${displayScore}%`,
                                  background: displayScore >= 85 ? 'var(--success)' : displayScore >= 70 ? 'var(--warning)' : 'var(--error)',
                                }} />
                              </div>
                              <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{displayScore.toFixed(1)}%</span>
                            </div>
                          ) : <span style={{ color: 'var(--text-dim)', fontSize: 12 }}>—</span>}
                        </td>
                        <td>
                          <span style={{ fontSize: 12, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 4 }}>
                            <Clock size={11} />
                            {job.elapsedSeconds ? `${job.elapsedSeconds}s` : '—'}
                          </span>
                        </td>
                        <td><span style={{ fontSize: 12, color: 'var(--text-dim)' }}>{timeAgo(job.createdAt)}</span></td>
                        <td>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'flex-end' }}>
                            {job.status === 'completed' && (
                              <button
                                className="btn btn-ghost btn-icon"
                                onClick={() => navigate(`/results/${job.id}`)}
                                title="View results"
                              >
                                <ArrowRight size={14} />
                              </button>
                            )}
                            <button
                              className="btn btn-ghost btn-icon"
                              onClick={(e) => handleDeleteJob(job.id, e)}
                              title="Delete migration record"
                              style={{ color: 'var(--error)' }}
                            >
                              <Trash2 size={14} />
                            </button>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
              {allJobs.length === 0 && (
                <div style={{ textAlign: 'center', padding: '48px 24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14 }}>
                  <div style={{ width: 44, height: 44, borderRadius: '50%', background: 'rgba(255,255,255,0.02)', border: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-dim)' }}>
                    <FileCode size={20} />
                  </div>
                  <div>
                    <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>No migrations yet</div>
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>Upload your legacy DataStage file to start.</div>
                  </div>
                  <button className="btn btn-primary btn-sm" onClick={() => navigate('/upload')} style={{ gap: 6 }}>
                    <Plus size={13} /> New Migration
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Sidebar */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            {/* Complexity distribution */}
            <div className="glass-card" style={{ padding: 24 }}>
              <h3 style={{ fontSize: 15, fontWeight: 700, fontFamily: 'var(--font)', marginBottom: 20 }}>Complexity Distribution</h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                {complexityData.map(d => (
                  <div key={d.label}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                      <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{d.label}</span>
                      <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{d.count} jobs ({d.pct}%)</span>
                    </div>
                    <div style={{ height: 6, background: 'var(--border)', borderRadius: 99, overflow: 'hidden' }}>
                      <motion.div
                        initial={{ width: 0 }}
                        animate={{ width: `${d.pct}%` }}
                        transition={{ duration: 0.8 }}
                        style={{ height: '100%', borderRadius: 99, background: d.color }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Recent activity */}
            {allJobs.length > 0 && (
              <div className="glass-card" style={{ padding: 24 }}>
                <h3 style={{ fontSize: 15, fontWeight: 700, fontFamily: 'var(--font)', marginBottom: 20 }}>Recent Activity</h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                  {allJobs.slice(0, 5).map(job => (
                    <div key={job.id} style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
                      <div style={{
                        width: 8, height: 8, borderRadius: '50%', flexShrink: 0, marginTop: 5,
                        background: job.status === 'completed' ? 'var(--success)' : job.status === 'failed' ? 'var(--error)' : job.status === 'human_review' ? 'var(--warning)' : 'var(--primary)',
                      }} />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{job.originalname}</div>
                        <div style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 2 }}>{timeAgo(job.createdAt)}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Quick start */}
            <div style={{
              padding: '20px',
              background: 'linear-gradient(135deg, rgba(59,130,246,0.08) 0%, rgba(6,182,212,0.05) 100%)',
              border: '1px solid rgba(59,130,246,0.2)',
              borderRadius: 'var(--radius-lg)',
              textAlign: 'center',
            }}>
              <Zap size={24} color="var(--primary)" style={{ margin: '0 auto 10px' }} />
              <div style={{ fontWeight: 700, marginBottom: 6, fontSize: 15 }}>Ready to Migrate?</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 16 }}>Upload your next .dsx or .isx file</div>
              <button className="btn btn-primary w-full" onClick={() => navigate('/upload')} style={{ justifyContent: 'center', gap: 6 }}>
                <Plus size={14} /> New Migration
              </button>
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  )
}
