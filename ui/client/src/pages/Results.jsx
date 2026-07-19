import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Download, Package, CheckCircle, FileCode, FileJson, TestTube, FileText, Clock, BarChart2, ArrowLeft, Copy, Check } from 'lucide-react'
import CodeViewer from '../components/CodeViewer.jsx'
import { api } from '../lib/api.js'

const TABS = [
  { id: 'pyspark',    label: 'PySpark',          icon: FileCode,  lang: 'python',     color: 'var(--primary)' },
  { id: 'datafusion', label: 'DataFusion JSON',  icon: FileJson,  lang: 'json',       color: 'var(--accent)' },
  { id: 'tests',      label: 'Unit Tests',       icon: TestTube,  lang: 'python',     color: 'var(--success)' },
  { id: 'report',     label: 'Migration Report', icon: FileText,  lang: 'json',       color: 'var(--purple)' },
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
            <button className="btn btn-ghost btn-sm" onClick={() => navigate('/dashboard')} style={{ marginBottom: 12, gap: 6, padding: '6px 12px' }}>
              <ArrowLeft size={13} /> Back to Dashboard
            </button>
            <div className="section-label">Migration Complete</div>
            <h1 style={{ fontSize: 28, fontWeight: 800, display: 'flex', alignItems: 'center', gap: 10 }}>
              <CheckCircle size={24} color="var(--success)" />
              {job.originalname}
            </h1>
          </div>

          {/* Download all */}
          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
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
            { label: 'Lines Generated', value: Object.values(report?.summary?.linesGenerated || {}).reduce((a, b) => a + b, 0) || '376+', color: 'var(--purple)' },
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

        {/* Tab nav + Download */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <div className="tabs" style={{ flex: 'none' }}>
            {TABS.map(t => (
              <button
                key={t.id}
                className={`tab ${activeTab === t.id ? 'active' : ''}`}
                onClick={() => setActiveTab(t.id)}
                style={{ gap: 6 }}
              >
                <t.icon size={13} style={{ color: activeTab === t.id ? t.color : 'inherit' }} />
                {t.label}
              </button>
            ))}
          </div>

          <div style={{ display: 'flex', gap: 8 }}>
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

        {/* Code viewer */}
        {activeTab !== 'report' ? (
          <motion.div
            key={activeTab}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            style={{ height: 600, borderRadius: 'var(--radius)', overflow: 'hidden' }}
          >
            <CodeViewer
              code={code}
              language={activeTabCfg.lang}
              onDownload={() => api.downloadFile(jobId, activeTab)}
            />
          </motion.div>
        ) : (
          // Report viewer
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
      </div>
    </motion.div>
  )
}
