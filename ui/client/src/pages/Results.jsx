import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Download, Package, CheckCircle, FileCode, FileJson, TestTube, FileText, Clock, BarChart2, ArrowLeft, Copy, Check, Database, GitBranch, Table, FlaskConical, Shield, Activity } from 'lucide-react'
import CodeViewer from '../components/CodeViewer.jsx'
import { api } from '../lib/api.js'

const TABS = [
  { id: 'pyspark',           label: 'PySpark',           icon: FileCode,    lang: 'python',  color: 'var(--primary)' },
  { id: 'datafusion',        label: 'DataFusion JSON',   icon: FileJson,    lang: 'json',    color: 'var(--accent)' },
  { id: 'tests',             label: 'Unit Tests',        icon: TestTube,    lang: 'python',  color: 'var(--success)' },
  { id: 'report',            label: 'Migration Report',  icon: FileText,    lang: 'json',    color: 'var(--purple)' },
  { id: 'validation_report', label: 'Validation Report', icon: Shield,      lang: 'json',    color: '#f59e0b' },
  { id: 'metadata',          label: 'Metadata Catalog',  icon: Database,    lang: 'json',    color: '#06b6d4' },
  { id: 'lineage',           label: 'Data Lineage',      icon: GitBranch,   lang: 'json',    color: '#a78bfa' },
  { id: 'ddl',               label: 'DDL Statements',    icon: Table,       lang: 'sql',     color: '#fb7185' },
  { id: 'test_harness',      label: 'Test Harness',      icon: FlaskConical, lang: 'python', color: '#34d399' },
]

export default function Results() {
  const { jobId } = useParams()
  const navigate = useNavigate()
  const [job, setJob] = useState(null)
  const [activeTab, setActiveTab] = useState('pyspark')
  const [copied, setCopied] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const load = () => {
      api.getJob(jobId).then(j => {
        setJob(j)
        setLoading(false)
        if (j.status !== 'completed') {
          navigate(`/pipeline/${jobId}`)
        }
      }).catch(() => navigate('/upload'))
    }
    load()
  }, [jobId])

  if (loading || !job) {
    return (
      <div className="page" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
          <div style={{ width: 32, height: 32, border: '2px solid var(--border)', borderTopColor: 'var(--primary)', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 16px' }} />
          Loading results...
        </div>
      </div>
    )
  }

  const activeTabCfg = TABS.find(t => t.id === activeTab)
  const code = job.outputs?.[activeTab] || ''
  const report = job.outputs?.report ? JSON.parse(job.outputs.report) : null

  // Count available artifacts
  const availableCount = TABS.filter(t => job.outputs?.[t.id]).length

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const lineCount = (txt) => txt ? txt.split('\n').length : 0

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="page"
    >
      <div className="container" style={{ padding: '40px 24px' }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 32 }}>
          <div>
            <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
              <button className="btn btn-ghost btn-sm" onClick={() => navigate('/dashboard')} style={{ gap: 6, padding: '6px 12px' }}>
                <ArrowLeft size={13} /> Back to Dashboard
              </button>
            </div>
            <div className="section-label">Migration Complete</div>
            <h1 style={{ fontSize: 28, fontWeight: 800, display: 'flex', alignItems: 'center', gap: 10 }}>
              <CheckCircle size={24} color="var(--success)" />
              {job.originalname}
            </h1>
          </div>

          {/* Header Action Buttons */}
          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            <button
              className="btn btn-ghost btn-sm"
              onClick={() => navigate(`/pipeline/${jobId}`)}
              style={{ gap: 6, padding: '8px 14px', border: '1px solid var(--border)' }}
              title="View pipeline graph and agent activity logs"
            >
              <Activity size={15} color="var(--primary)" />
              View Pipeline Graph
            </button>
            <button
              className="btn btn-secondary"
              onClick={() => api.downloadFile(jobId, 'zip')}
              style={{ gap: 7 }}
            >
              <Package size={15} />
              Download All (.zip)
            </button>
          </div>
        </div>

        {/* Summary cards */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 28 }}>
          {[
            { label: 'Complexity', value: report?.summary?.complexity || job.classification?.complexity || '—', color: 'var(--primary)' },
            { label: 'Confidence', value: `${report?.summary?.confidenceScore?.toFixed(1) || job.classification?.confidenceScore?.toFixed(1) || '—'}%`, color: 'var(--success)' },
            { label: 'Time Elapsed', value: `${report?.summary?.elapsedSeconds || job.elapsedSeconds || '—'}s`, color: 'var(--accent)' },
            { label: 'Artifacts Generated', value: `${availableCount} / ${TABS.length}`, color: 'var(--purple)' },
          ].map((card, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.08 }}
              className="glass-card"
              style={{ padding: '18px 20px', textAlign: 'center' }}
            >
              <div style={{ fontFamily: 'var(--font-head)', fontSize: 24, fontWeight: 800, color: card.color }}>{card.value}</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>{card.label}</div>
            </motion.div>
          ))}
        </div>

        {/* Tab nav + actions — scrollable for all 9 tabs */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, gap: 12 }}>
          <div
            className="tabs"
            style={{
              flex: 1,
              overflowX: 'auto',
              whiteSpace: 'nowrap',
              scrollbarWidth: 'thin',
              msOverflowStyle: 'none',
              paddingBottom: 2,
            }}
          >
            {TABS.map(t => {
              const hasContent = !!job.outputs?.[t.id]
              return (
                <button
                  key={t.id}
                  className={`tab ${activeTab === t.id ? 'active' : ''}`}
                  onClick={() => setActiveTab(t.id)}
                  style={{
                    gap: 5,
                    opacity: hasContent ? 1 : 0.4,
                    fontSize: 12,
                    padding: '8px 12px',
                    flexShrink: 0,
                    position: 'relative',
                  }}
                  title={hasContent ? t.label : `${t.label} — not generated`}
                >
                  <t.icon size={12} style={{ color: activeTab === t.id ? t.color : 'inherit' }} />
                  {t.label}
                  {hasContent && (
                    <span style={{
                      width: 5, height: 5, borderRadius: '50%',
                      background: t.color, display: 'inline-block',
                      marginLeft: 2, flexShrink: 0,
                    }} />
                  )}
                </button>
              )
            })}
          </div>

          <div style={{ display: 'flex', gap: 8, flexShrink: 0 }}>
            <button className="btn btn-secondary btn-sm" onClick={handleCopy} style={{ gap: 6 }}>
              {copied ? <Check size={13} color="var(--success)" /> : <Copy size={13} />}
              {copied ? 'Copied!' : 'Copy'}
            </button>
            <button className="btn btn-primary btn-sm" onClick={() => api.downloadFile(jobId, activeTab)} style={{ gap: 6 }}>
              <Download size={13} />
              Download
            </button>
          </div>
        </div>

        {/* Code viewer — used for most tabs */}
        {activeTab !== 'report' ? (
          code ? (
            <motion.div
              key={activeTab}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              style={{ borderRadius: 'var(--radius)', overflow: 'hidden' }}
            >
              {/* Line count badge */}
              <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 16px',
                background: 'rgba(255,255,255,0.02)',
                borderBottom: '1px solid var(--border)',
                borderRadius: 'var(--radius) var(--radius) 0 0',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  {activeTabCfg && <activeTabCfg.icon size={14} style={{ color: activeTabCfg.color }} />}
                  <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{activeTabCfg?.label}</span>
                  <span style={{
                    fontSize: 11, color: 'var(--text-muted)',
                    background: 'rgba(255,255,255,0.04)', padding: '2px 8px',
                    borderRadius: 20, border: '1px solid var(--border)',
                  }}>
                    {lineCount(code)} lines
                  </span>
                </div>
                <span style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  {activeTabCfg?.lang}
                </span>
              </div>
              <div style={{ height: 560 }}>
                <CodeViewer
                  code={code}
                  language={activeTabCfg?.lang || 'text'}
                  onDownload={() => api.downloadFile(jobId, activeTab)}
                />
              </div>
            </motion.div>
          ) : (
            <motion.div
              key={activeTab}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              className="glass-card"
              style={{ padding: '48px 24px', textAlign: 'center' }}
            >
              <div style={{ fontSize: 14, color: 'var(--text-muted)' }}>
                This artifact was not generated for this job.
              </div>
            </motion.div>
          )
        ) : (
          // Report viewer — rich formatted view
          report ? (
            <motion.div
              key="report"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              style={{ display: 'flex', flexDirection: 'column', gap: 16 }}
            >
              {/* Summary */}
              <div className="glass-card" style={{ padding: 24 }}>
                <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16, color: 'var(--text)' }}>Migration Summary</div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16 }}>
                  {[
                    { label: 'PySpark Lines', value: report.summary.linesGenerated?.pyspark },
                    { label: 'DataFusion Lines', value: report.summary.linesGenerated?.datafusion },
                    { label: 'Test Lines', value: report.summary.linesGenerated?.tests },
                  ].map((s, i) => (
                    <div key={i} style={{ textAlign: 'center', padding: '12px', background: 'rgba(255,255,255,0.02)', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                      <div style={{ fontFamily: 'var(--font-head)', fontSize: 24, fontWeight: 800, color: 'var(--primary)' }}>{s.value}</div>
                      <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>{s.label}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Validation checks */}
              <div className="glass-card" style={{ padding: 24 }}>
                <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16, color: 'var(--text)' }}>Validation Results</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {report.validation?.checks?.map((check, i) => (
                    <div key={i} style={{
                      display: 'flex', alignItems: 'center', gap: 14,
                      padding: '12px 16px',
                      background: check.status === 'passed' ? 'rgba(16,185,129,0.04)' : check.status === 'warning' ? 'rgba(245,158,11,0.04)' : 'rgba(239,68,68,0.04)',
                      border: `1px solid ${check.status === 'passed' ? 'rgba(16,185,129,0.15)' : check.status === 'warning' ? 'rgba(245,158,11,0.15)' : 'rgba(239,68,68,0.15)'}`,
                      borderRadius: 'var(--radius)',
                    }}>
                      <div style={{ width: 8, height: 8, borderRadius: '50%', background: check.status === 'passed' ? 'var(--success)' : check.status === 'warning' ? 'var(--warning)' : 'var(--error)', flexShrink: 0 }} />
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{check.name}</div>
                        <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>{check.detail}</div>
                      </div>
                      <span className={`badge ${check.status === 'passed' ? 'badge-success' : check.status === 'warning' ? 'badge-warning' : 'badge-error'}`} style={{ textTransform: 'capitalize' }}>
                        {check.status}
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              {/* Recommendations */}
              <div className="glass-card" style={{ padding: 24 }}>
                <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16, color: 'var(--text)' }}>Recommendations</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  {report.recommendations?.map((rec, i) => (
                    <div key={i} style={{ display: 'flex', gap: 12, padding: '10px 14px', background: 'rgba(59,130,246,0.04)', border: '1px solid rgba(59,130,246,0.1)', borderRadius: 'var(--radius)' }}>
                      <div style={{ width: 20, height: 20, borderRadius: '50%', background: 'rgba(59,130,246,0.1)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, fontSize: 10, fontWeight: 700, color: 'var(--primary)' }}>{i+1}</div>
                      <span style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5 }}>{rec}</span>
                    </div>
                  ))}
                </div>
              </div>
            </motion.div>
          ) : null
        )}

        {/* Artifact inventory footer */}
        <div style={{
          marginTop: 24, padding: '16px 20px',
          background: 'rgba(255,255,255,0.02)',
          borderRadius: 'var(--radius)',
          border: '1px solid var(--border)',
        }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-muted)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Generated Artifacts ({availableCount}/{TABS.length})
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {TABS.map(t => {
              const has = !!job.outputs?.[t.id]
              return (
                <div
                  key={t.id}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 5,
                    padding: '4px 10px', borderRadius: 20,
                    fontSize: 11, fontWeight: 600,
                    background: has ? `${t.color}15` : 'rgba(255,255,255,0.02)',
                    color: has ? t.color : 'var(--text-muted)',
                    border: `1px solid ${has ? `${t.color}30` : 'var(--border)'}`,
                    opacity: has ? 1 : 0.5,
                    cursor: has ? 'pointer' : 'default',
                  }}
                  onClick={() => has && setActiveTab(t.id)}
                >
                  {has ? <CheckCircle size={10} /> : <span style={{ width: 10, height: 10, borderRadius: '50%', border: '1.5px solid currentColor', display: 'inline-block' }} />}
                  {t.label}
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </motion.div>
  )
}
