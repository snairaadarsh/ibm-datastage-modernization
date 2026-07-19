import { useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { ArrowRight, Zap, Shield, TrendingUp, Database, GitBranch, Cloud, ChevronRight, BarChart3, Brain, Code2, TestTube } from 'lucide-react'

const PIPELINE_STEPS = [
  { icon: Database,   label: 'Parser Agent',   sub: 'XML parse + normalize',     color: '#ef4444' },
  { icon: BarChart3,  label: 'Classifier',     sub: 'Complexity scoring',         color: '#f59e0b' },
  { icon: Brain,      label: 'Translator',     sub: 'PySpark + DataFusion',       color: '#3b82f6' },
  { icon: Shield,     label: 'Reviewer',       sub: 'AI code review',             color: '#8b5cf6' },
  { icon: TestTube,   label: 'Tester',         sub: 'pytest generation',          color: '#10b981' },
  { icon: Code2,      label: 'Documentation',  sub: 'Migration report',           color: '#06b6d4' },
]

const FEATURES = [
  { icon: Brain,       title: 'Fully Agentic Pipeline',   desc: '6 specialized AI agents — Parser, Classifier, Translator, Reviewer, Tester, and Documentation — run sequentially on your DSX/ISX file.' },
  { icon: Shield,      title: 'Human-in-the-Loop',        desc: 'Complex jobs route to a human review gate. Approve, reject, or request changes before code generation proceeds.' },
  { icon: Zap,         title: 'Dual LLM Providers',       desc: 'Bring your own API key (OpenAI, Gemini, Anthropic) or use the Antigravity IDE agent in your active chat — no key needed.' },
  { icon: TrendingUp,  title: 'Dual Output Formats',      desc: 'Generates both production PySpark scripts and Google Cloud DataFusion pipeline JSON simultaneously from a single upload.' },
]

// Animated particle canvas
function ParticleCanvas() {
  const ref = useRef(null)
  useEffect(() => {
    const canvas = ref.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    canvas.width = canvas.offsetWidth
    canvas.height = canvas.offsetHeight
    const particles = Array.from({ length: 50 }, () => ({
      x: Math.random() * canvas.width,
      y: Math.random() * canvas.height,
      vx: (Math.random() - 0.5) * 0.3,
      vy: (Math.random() - 0.5) * 0.3,
      size: Math.random() * 1.5 + 0.5,
      opacity: Math.random() * 0.4 + 0.1,
    }))

    let animId
    const animate = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      particles.forEach(p => {
        p.x += p.vx; p.y += p.vy
        if (p.x < 0) p.x = canvas.width
        if (p.x > canvas.width) p.x = 0
        if (p.y < 0) p.y = canvas.height
        if (p.y > canvas.height) p.y = 0
        ctx.beginPath()
        ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2)
        ctx.fillStyle = `rgba(59,130,246,${p.opacity})`
        ctx.fill()
      })
      // Draw connections
      for (let i = 0; i < particles.length; i++) {
        for (let j = i + 1; j < particles.length; j++) {
          const dx = particles[i].x - particles[j].x
          const dy = particles[i].y - particles[j].y
          const dist = Math.sqrt(dx * dx + dy * dy)
          if (dist < 100) {
            ctx.beginPath()
            ctx.moveTo(particles[i].x, particles[i].y)
            ctx.lineTo(particles[j].x, particles[j].y)
            ctx.strokeStyle = `rgba(59,130,246,${0.08 * (1 - dist / 100)})`
            ctx.lineWidth = 0.5
            ctx.stroke()
          }
        }
      }
      animId = requestAnimationFrame(animate)
    }
    animate()
    return () => cancelAnimationFrame(animId)
  }, [])
  return <canvas ref={ref} style={{ position: 'absolute', inset: 0, width: '100%', height: '100%' }} />
}

export default function Landing() {
  const navigate = useNavigate()

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.4 }}
      className="page"
    >
      {/* ── Hero Section ───────────────────────────────────────────────────── */}
      <section style={{
        position: 'relative',
        minHeight: '90vh',
        display: 'flex',
        alignItems: 'center',
        overflow: 'hidden',
      }}>
        <ParticleCanvas />

        {/* Gradient orbs */}
        <div style={{
          position: 'absolute', top: '10%', left: '5%',
          width: 500, height: 500,
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(59,130,246,0.08) 0%, transparent 70%)',
          pointerEvents: 'none',
        }} />
        <div style={{
          position: 'absolute', bottom: '10%', right: '5%',
          width: 400, height: 400,
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(6,182,212,0.06) 0%, transparent 70%)',
          pointerEvents: 'none',
        }} />

        <div className="container" style={{ position: 'relative', zIndex: 1, textAlign: 'center', padding: '80px 24px' }}>
          {/* Tag */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            style={{ display: 'flex', justifyContent: 'center', marginBottom: 32 }}
          >
            <div style={{
              display: 'inline-flex', alignItems: 'center', gap: 8,
              padding: '6px 16px',
              borderRadius: 99,
              background: 'rgba(59,130,246,0.08)',
              border: '1px solid rgba(59,130,246,0.25)',
              fontSize: 13, fontWeight: 600, color: 'var(--primary-bright)',
            }}>
              <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--primary)', animation: 'pulse-dot 1.5s infinite' }} />
              Powered by Multi-Agent AI Pipeline
            </div>
          </motion.div>

          {/* Headline */}
          <motion.h1
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2, type: 'spring', stiffness: 100 }}
            style={{ fontSize: 'clamp(36px, 6vw, 72px)', fontWeight: 800, lineHeight: 1.05, marginBottom: 24, maxWidth: 900, margin: '0 auto 24px' }}
          >
            Transform Legacy{' '}
            <span className="gradient-text">IBM DataStage ETL</span>
            <br />to Cloud-Native in Minutes
          </motion.h1>

          {/* Sub */}
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            style={{ fontSize: 18, color: 'var(--text-muted)', maxWidth: 600, margin: '0 auto 48px', lineHeight: 1.7 }}
          >
            Upload a <code style={{ color: 'var(--primary-bright)', fontFamily: 'var(--font-mono)', fontSize: 15 }}>.dsx</code> or <code style={{ color: 'var(--purple)', fontFamily: 'var(--font-mono)', fontSize: 15 }}>.isx</code> file and watch AI agents migrate your pipeline to <strong style={{ color: 'var(--text)' }}>PySpark</strong> and <strong style={{ color: 'var(--text)' }}>Google Cloud DataFusion</strong> — automatically.
          </motion.p>

          {/* CTAs */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
            style={{ display: 'flex', gap: 12, justifyContent: 'center', flexWrap: 'wrap' }}
          >
            <button className="btn btn-primary btn-lg" onClick={() => navigate('/upload')} style={{ fontSize: 16, padding: '14px 32px' }}>
              <Zap size={18} /> Start Migration
              <ArrowRight size={16} />
            </button>
            <button className="btn btn-secondary btn-lg" onClick={() => navigate('/dashboard')} style={{ fontSize: 16 }}>
              <BarChart3 size={18} /> View Dashboard
            </button>
          </motion.div>

        </div>
      </section>

      {/* ── Pipeline Visualization ─────────────────────────────────────────── */}
      <section style={{ padding: '80px 0', background: 'rgba(255,255,255,0.01)' }}>
        <div className="container">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5 }}
            style={{ textAlign: 'center', marginBottom: 56 }}
          >
            <div className="section-label">How it works</div>
            <h2 style={{ fontSize: 36, fontWeight: 800 }}>From Legacy to Cloud in One Click</h2>
            <p style={{ color: 'var(--text-muted)', marginTop: 12, fontSize: 16 }}>
              A 7-stage AI pipeline handles everything — from XML parsing to code generation.
            </p>
          </motion.div>

          {/* Pipeline flow */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 0, flexWrap: 'nowrap', overflowX: 'auto', padding: '0 20px' }}>
            {PIPELINE_STEPS.map((step, i) => (
              <div key={i} style={{ display: 'flex', alignItems: 'center' }}>
                <motion.div
                  initial={{ opacity: 0, scale: 0.8 }}
                  whileInView={{ opacity: 1, scale: 1 }}
                  viewport={{ once: true }}
                  transition={{ delay: i * 0.15, type: 'spring' }}
                  style={{ textAlign: 'center', padding: '0 20px' }}
                >
                  <div style={{
                    width: 72, height: 72, borderRadius: 20, margin: '0 auto 12px',
                    background: `${step.color}18`,
                    border: `1px solid ${step.color}40`,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    boxShadow: `0 0 24px ${step.color}20`,
                  }}>
                    <step.icon size={28} color={step.color} />
                  </div>
                  <div style={{ fontWeight: 700, color: 'var(--text)', fontSize: 15 }}>{step.label}</div>
                  <div style={{ fontSize: 12, color: 'var(--text-dim)', marginTop: 3 }}>{step.sub}</div>
                </motion.div>
                {i < PIPELINE_STEPS.length - 1 && (
                  <motion.div
                    initial={{ opacity: 0, scaleX: 0 }}
                    whileInView={{ opacity: 1, scaleX: 1 }}
                    viewport={{ once: true }}
                    transition={{ delay: i * 0.15 + 0.1 }}
                    style={{ display: 'flex', alignItems: 'center', gap: 4, color: 'var(--text-dim)', flexShrink: 0 }}
                  >
                    <div style={{ width: 40, height: 1, background: 'linear-gradient(90deg, var(--border), var(--primary-glow))' }} />
                    <ChevronRight size={14} color="var(--primary)" />
                  </motion.div>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Features ──────────────────────────────────────────────────────── */}
      <section style={{ padding: '80px 0' }}>
        <div className="container">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            style={{ textAlign: 'center', marginBottom: 56 }}
          >
            <div className="section-label">Capabilities</div>
            <h2 style={{ fontSize: 36, fontWeight: 800 }}>Everything Your Migration Needs</h2>
          </motion.div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 20 }}>
            {FEATURES.map((f, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ delay: i * 0.1 }}
                className="glass-card"
                style={{ padding: 28 }}
              >
                <div style={{
                  width: 44, height: 44, borderRadius: 12,
                  background: 'rgba(59,130,246,0.1)', border: '1px solid rgba(59,130,246,0.2)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center', marginBottom: 16,
                }}>
                  <f.icon size={20} color="var(--primary)" />
                </div>
                <h3 style={{ fontSize: 16, fontWeight: 700, color: 'var(--text)', marginBottom: 8, fontFamily: 'var(--font)' }}>{f.title}</h3>
                <p style={{ fontSize: 14, color: 'var(--text-muted)', lineHeight: 1.6 }}>{f.desc}</p>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* ── CTA Banner ────────────────────────────────────────────────────── */}
      <section style={{ padding: '60px 0 80px' }}>
        <div className="container">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            style={{
              textAlign: 'center',
              padding: '60px 40px',
              borderRadius: 'var(--radius-xl)',
              background: 'linear-gradient(135deg, rgba(59,130,246,0.08) 0%, rgba(6,182,212,0.05) 100%)',
              border: '1px solid rgba(59,130,246,0.2)',
            }}
          >
            <h2 style={{ fontSize: 36, fontWeight: 800, marginBottom: 16 }}>Ready to Modernize?</h2>
            <p style={{ color: 'var(--text-muted)', marginBottom: 32, fontSize: 16 }}>
              Upload your DataStage file and get results in under 20 seconds.
            </p>
            <button className="btn btn-primary btn-lg" onClick={() => navigate('/upload')}>
              <Zap size={18} /> Start Your Migration Now <ArrowRight size={16} />
            </button>
          </motion.div>
        </div>
      </section>
    </motion.div>
  )
}
