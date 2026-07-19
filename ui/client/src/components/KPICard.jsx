import { TrendingUp, TrendingDown, Minus } from 'lucide-react'

export default function KPICard({ label, value, unit = '', sub, trend, icon: Icon, color = 'var(--primary)', delay = 0 }) {
  const trendIcon = trend > 0 ? TrendingUp : trend < 0 ? TrendingDown : Minus
  const TrendIcon = trendIcon

  return (
    <div
      className="kpi-card anim-fade-in-up"
      style={{ animationDelay: `${delay}ms` }}
    >
      {/* Background gradient accent */}
      <div style={{
        position: 'absolute',
        top: 0, right: 0,
        width: 120, height: 120,
        borderRadius: '50%',
        background: color,
        opacity: 0.04,
        transform: 'translate(30%, -30%)',
        pointerEvents: 'none',
      }} />

      {/* Icon */}
      {Icon && (
        <div className="kpi-icon" style={{ background: `${color}18`, border: `1px solid ${color}30` }}>
          <Icon size={18} color={color} />
        </div>
      )}

      {/* Value */}
      <div style={{ marginTop: Icon ? 36 : 0 }}>
        <div className="kpi-value" style={{ color }}>
          {value}<span style={{ fontSize: 18, fontWeight: 600, color: 'var(--text-muted)' }}>{unit}</span>
        </div>
        <div className="kpi-label">{label}</div>
        {sub && (
          <div style={{ fontSize: 12, color: 'var(--text-dim)', marginTop: 8, display: 'flex', alignItems: 'center', gap: 4 }}>
            {trend !== undefined && (
              <TrendIcon size={12} color={trend > 0 ? 'var(--success)' : trend < 0 ? 'var(--error)' : 'var(--text-dim)'} />
            )}
            {sub}
          </div>
        )}
      </div>
    </div>
  )
}
