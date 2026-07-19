import { useState } from 'react'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism'
import { Copy, Check, Download } from 'lucide-react'

export default function CodeViewer({ code = '', language = 'python', onDownload }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const lineCount = code ? code.split('\n').length : 0

  return (
    <div style={{
      background: '#010409',
      border: '1px solid rgba(255,255,255,0.06)',
      borderRadius: 'var(--radius)',
      overflow: 'hidden',
      height: '100%',
      display: 'flex',
      flexDirection: 'column',
    }}>
      {/* Toolbar */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '10px 16px',
        borderBottom: '1px solid rgba(255,255,255,0.06)',
        background: 'rgba(255,255,255,0.02)',
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', gap: 6 }}>
          {['#EF4444','#F59E0B','#10B981'].map((c, i) => (
            <div key={i} style={{ width: 10, height: 10, borderRadius: '50%', background: c, opacity: 0.7 }} />
          ))}
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span style={{ fontSize: 11, color: 'var(--text-dim)', fontFamily: 'var(--font-mono)' }}>
            {lineCount} lines
          </span>
          <button
            className="btn btn-secondary btn-sm"
            onClick={handleCopy}
            style={{ gap: 5, padding: '4px 10px', fontSize: 12 }}
          >
            {copied ? <Check size={12} color="var(--success)" /> : <Copy size={12} />}
            {copied ? 'Copied!' : 'Copy'}
          </button>
          {onDownload && (
            <button
              className="btn btn-secondary btn-sm"
              onClick={onDownload}
              style={{ gap: 5, padding: '4px 10px', fontSize: 12 }}
            >
              <Download size={12} />
              Download
            </button>
          )}
        </div>
      </div>

      {/* Code */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <SyntaxHighlighter
          language={language}
          style={vscDarkPlus}
          showLineNumbers
          wrapLongLines={false}
          customStyle={{
            margin: 0,
            padding: '16px',
            background: 'transparent',
            fontSize: '12.5px',
            lineHeight: '1.7',
            fontFamily: "'JetBrains Mono', monospace",
          }}
          lineNumberStyle={{
            color: 'rgba(255,255,255,0.15)',
            minWidth: '2.5em',
            paddingRight: '1em',
            fontSize: '11px',
          }}
        >
          {code || '// No output yet'}
        </SyntaxHighlighter>
      </div>
    </div>
  )
}
