import { CheckCircle, Clock, AlertTriangle, X, Loader, FileCode } from 'lucide-react'

const STAGE_CONFIG = {
  pending:      { color: 'var(--text-dim)',       icon: Clock,         bg: 'rgba(255,255,255,0.04)' },
  active:       { color: 'var(--primary)',         icon: Loader,        bg: 'rgba(59,130,246,0.12)'  },
  done:         { color: 'var(--success)',         icon: CheckCircle,   bg: 'rgba(16,185,129,0.12)'  },
  error:        { color: 'var(--error)',           icon: X,             bg: 'rgba(239,68,68,0.12)'   },
  human_review: { color: 'var(--warning)',         icon: AlertTriangle, bg: 'rgba(245,158,11,0.12)'  },
}

/**
 * PipelineGraph — Horizontal animated pipeline node graph.
 * Shows 7 stages with status-aware colors, active animations, and connector lines.
 */
export default function PipelineGraph({ stages = [], compact = false }) {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: 0,
      width: '100%',
      overflowX: 'auto',
      padding: compact ? '12px 0' : '24px 0',
    }}>
      {stages.map((stage, i) => {
        const cfg = STAGE_CONFIG[stage.status] || STAGE_CONFIG.pending
        const Icon = cfg.icon
        const isLast = i === stages.length - 1
        const isActive = stage.status === 'active'
        const isDone = stage.status === 'done'

        return (
          <div key={stage.id} style={{ display: 'flex', alignItems: 'center', flex: isLast ? '0 0 auto' : '1 1 0' }}>
            {/* Node */}
            <div style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              gap: compact ? 6 : 10,
              flex: '0 0 auto',
              position: 'relative',
            }}>
              {/* Icon circle */}
              <div
                className={isActive ? 'stage-node active' : isDone ? 'stage-node done' : 'stage-node'}
                style={{
                  width: compact ? 36 : 48,
                  height: compact ? 36 : 48,
                  borderRadius: '50%',
                  background: cfg.bg,
                  border: `2px solid ${cfg.color}`,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  transition: 'all 0.3s',
                  position: 'relative',
                }}
              >
                {/* Ripple for active */}
                {isActive && (
                  <div style={{
                    position: 'absolute',
                    inset: -4,
                    borderRadius: '50%',
                    border: `2px solid ${cfg.color}`,
                    opacity: 0.4,
                    animation: 'nodeRing 1.5s ease-out infinite',
                  }} />
                )}
                <Icon
                  size={compact ? 14 : 18}
                  color={cfg.color}
                  style={isActive ? { animation: 'spin 1.2s linear infinite' } : {}}
                />
              </div>

              {/* Label */}
              {!compact && (
                <div style={{
                  textAlign: 'center',
                  maxWidth: 80,
                }}>
                  <div style={{
                    fontSize: 11,
                    fontWeight: 700,
                    color: stage.status === 'pending' ? 'var(--text-dim)' : 'var(--text)',
                    whiteSpace: 'nowrap',
                    transition: 'color 0.3s',
                  }}>{stage.label}</div>
                  {stage.duration && (
                    <div style={{ fontSize: 10, color: 'var(--success)', marginTop: 2 }}>{stage.duration}s</div>
                  )}
                  {isActive && (
                    <div style={{ fontSize: 10, color: 'var(--primary)', marginTop: 2 }}>Running...</div>
                  )}
                </div>
              )}
            </div>

            {/* Connector line */}
            {!isLast && (
              <div style={{
                flex: 1,
                height: 2,
                marginTop: compact ? 0 : compact ? 0 : -22,
                position: 'relative',
                overflow: 'hidden',
                minWidth: 20,
                maxWidth: 80,
              }}>
                <div style={{
                  position: 'absolute',
                  inset: 0,
                  background: isDone
                    ? 'linear-gradient(90deg, var(--success), rgba(16,185,129,0.4))'
                    : isActive
                    ? 'linear-gradient(90deg, var(--primary), rgba(59,130,246,0.2))'
                    : 'var(--border)',
                  transition: 'background 0.5s',
                }} />
                {/* Flow dots for active/done connectors */}
                {(isDone || (isActive && i > 0)) && (
                  <div style={{
                    position: 'absolute',
                    top: -2,
                    width: 6,
                    height: 6,
                    borderRadius: '50%',
                    background: isDone ? 'var(--success)' : 'var(--primary)',
                    animation: 'progressPulse 1.5s linear infinite',
                  }} />
                )}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
