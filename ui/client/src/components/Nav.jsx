import { Link, useLocation, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import { LayoutDashboard, Upload, Zap, GitBranch } from 'lucide-react'

const links = [
  { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/upload',    icon: Upload,          label: 'New Migration' },
]

export default function Nav() {
  const { pathname } = useLocation()
  const navigate = useNavigate()

  const isActive = (path) => pathname.startsWith(path)

  return (
    <header style={{
      height: 64,
      borderBottom: '1px solid var(--border)',
      background: 'rgba(3, 8, 18, 0.85)',
      backdropFilter: 'blur(20px)',
      WebkitBackdropFilter: 'blur(20px)',
      position: 'sticky',
      top: 0,
      zIndex: 100,
      display: 'flex',
      alignItems: 'center',
    }}>
      <div className="container" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
        {/* Logo */}
        <Link to="/" style={{ textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 34, height: 34,
            borderRadius: 9,
            background: 'linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            boxShadow: '0 4px 16px rgba(59,130,246,0.35)',
          }}>
            <GitBranch size={18} color="#fff" />
          </div>
          <div>
            <div style={{ fontFamily: 'var(--font-head)', fontWeight: 700, fontSize: 15, color: 'var(--text-bright)', lineHeight: 1.1 }}>
              DataStage<span style={{ color: 'var(--primary)' }}>.</span>AI
            </div>
            <div style={{ fontSize: 10, color: 'var(--text-dim)', letterSpacing: '0.05em', lineHeight: 1 }}>ETL MODERNIZATION SUITE</div>
          </div>
        </Link>

        {/* Nav links */}
        <nav style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          {links.map(({ to, icon: Icon, label }) => (
            <Link key={to} to={to} style={{ textDecoration: 'none' }}>
              <div style={{
                display: 'flex', alignItems: 'center', gap: 7,
                padding: '7px 14px',
                borderRadius: 8,
                fontSize: 13,
                fontWeight: 600,
                color: isActive(to) ? 'var(--primary-bright)' : 'var(--text-muted)',
                background: isActive(to) ? 'rgba(59,130,246,0.1)' : 'transparent',
                border: isActive(to) ? '1px solid rgba(59,130,246,0.2)' : '1px solid transparent',
                transition: 'all 0.2s',
                cursor: 'pointer',
              }}>
                <Icon size={14} />
                {label}
              </div>
            </Link>
          ))}
        </nav>

        {/* CTA */}
        <button
          className="btn btn-primary btn-sm"
          onClick={() => navigate('/upload')}
          style={{ gap: 6 }}
        >
          <Zap size={13} />
          Start Migration
        </button>
      </div>
    </header>
  )
}
