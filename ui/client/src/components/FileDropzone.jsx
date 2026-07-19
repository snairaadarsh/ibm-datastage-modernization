import { useState, useCallback } from 'react'
import { Upload, FileCode, X, CheckCircle } from 'lucide-react'

export default function FileDropzone({ onFile }) {
  const [isDragging, setIsDragging] = useState(false)
  const [file, setFile] = useState(null)
  const [error, setError] = useState(null)

  const validateFile = (f) => {
    const ext = f.name.split('.').pop().toLowerCase()
    if (!['dsx', 'isx', 'xml'].includes(ext)) {
      setError('Only .dsx, .isx, and .xml files are supported')
      return false
    }
    if (f.size > 50 * 1024 * 1024) {
      setError('File too large (max 50MB)')
      return false
    }
    return true
  }

  const handleFile = useCallback((f) => {
    setError(null)
    if (validateFile(f)) {
      setFile(f)
      onFile?.(f)
    }
  }, [onFile])

  const onDrop = useCallback((e) => {
    e.preventDefault()
    setIsDragging(false)
    const f = e.dataTransfer.files[0]
    if (f) handleFile(f)
  }, [handleFile])

  const onInputChange = (e) => {
    const f = e.target.files[0]
    if (f) handleFile(f)
  }

  const ext = file?.name.split('.').pop().toLowerCase()

  return (
    <div>
      {!file ? (
        <label
          onDragOver={(e) => { e.preventDefault(); setIsDragging(true) }}
          onDragLeave={() => setIsDragging(false)}
          onDrop={onDrop}
          htmlFor="file-input"
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 16,
            padding: '60px 40px',
            border: `2px dashed ${isDragging ? 'var(--primary)' : 'var(--border)'}`,
            borderRadius: 'var(--radius-lg)',
            background: isDragging ? 'rgba(59,130,246,0.05)' : 'rgba(255,255,255,0.02)',
            cursor: 'pointer',
            transition: 'all 0.2s',
            textAlign: 'center',
          }}
          className={isDragging ? 'dropzone-active' : ''}
        >
          <div style={{
            width: 64, height: 64,
            borderRadius: '50%',
            background: isDragging ? 'rgba(59,130,246,0.15)' : 'rgba(255,255,255,0.04)',
            border: `1px solid ${isDragging ? 'var(--primary)' : 'var(--border)'}`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            transition: 'all 0.2s',
          }}>
            <Upload size={28} color={isDragging ? 'var(--primary)' : 'var(--text-dim)'} />
          </div>
          <div>
            <div style={{ fontSize: 16, fontWeight: 600, color: 'var(--text)' }}>
              Drop your DataStage file here
            </div>
            <div style={{ fontSize: 14, color: 'var(--text-muted)', marginTop: 6 }}>
              or <span style={{ color: 'var(--primary)', fontWeight: 600 }}>browse to upload</span>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {['.dsx', '.isx', '.xml'].map(ext => (
              <span key={ext} className="badge badge-info">{ext}</span>
            ))}
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-dim)' }}>Max file size: 50MB</div>
          <input
            id="file-input"
            type="file"
            accept=".dsx,.isx,.xml"
            style={{ display: 'none' }}
            onChange={onInputChange}
          />
        </label>
      ) : (
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 16,
          padding: '20px 24px',
          background: 'rgba(16,185,129,0.06)',
          border: '1px solid rgba(16,185,129,0.25)',
          borderRadius: 'var(--radius-lg)',
        }}>
          <div style={{
            width: 48, height: 48,
            borderRadius: 12,
            background: ext === 'dsx' ? 'rgba(59,130,246,0.12)' : 'rgba(139,92,246,0.12)',
            border: `1px solid ${ext === 'dsx' ? 'rgba(59,130,246,0.3)' : 'rgba(139,92,246,0.3)'}`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
          }}>
            <FileCode size={22} color={ext === 'dsx' ? 'var(--primary)' : 'var(--purple)'} />
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {file.name}
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 3 }}>
              {(file.size / 1024).toFixed(1)} KB · <span className={`badge badge-${ext === 'dsx' ? 'info' : 'purple'}`} style={{ padding: '1px 6px', fontSize: 10 }}>.{ext}</span>
            </div>
          </div>
          <CheckCircle size={18} color="var(--success)" />
          <button
            className="btn btn-ghost btn-icon"
            onClick={(e) => { e.preventDefault(); setFile(null); onFile?.(null) }}
            style={{ flexShrink: 0 }}
          >
            <X size={14} />
          </button>
        </div>
      )}

      {error && (
        <div style={{
          marginTop: 10,
          padding: '10px 14px',
          background: 'var(--error-bg)',
          border: '1px solid rgba(239,68,68,0.2)',
          borderRadius: 'var(--radius)',
          fontSize: 13,
          color: 'var(--error)',
          display: 'flex', gap: 8, alignItems: 'center',
        }}>
          <X size={14} /> {error}
        </div>
      )}
    </div>
  )
}
