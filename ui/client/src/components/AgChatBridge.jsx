import { useEffect, useRef, useState, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Brain, Loader2 } from 'lucide-react'
import socket from '../lib/socket.js'

/**
 * AgChatBridge — Draggable, interactive floating brain widget.
 * 
 * Reverted back to inline relative drag constraints as requested.
 */
export default function AgChatBridge() {
  const [activePrompt, setActivePrompt] = useState(null)
  const [submitted,    setSubmitted]    = useState(false)
  const [isAgChatJob,  setIsAgChatJob]  = useState(false)
  const [waiting,      setWaiting]      = useState(false)
  const [expanded,     setExpanded]     = useState(false)
  const isDragging = useRef(false)
  const sseRef      = useRef(null)
  const origTitle   = useRef(document.title)

  // ── SSE connection ────────────────────────────────────────────────────────
  useEffect(() => {
    const connect = () => {
      const es = new EventSource('/api/ag-chat/stream')
      sseRef.current = es

      es.onmessage = (e) => {
        try {
          const data = JSON.parse(e.data)
          if (data.type === 'prompt') {
            handlePromptArrived(data)
          } else if (data.type === 'response_received') {
            if (activePromptRef.current?.requestId === data.requestId) {
              clearActive()
            }
          } else if (data.type === 'cancelled') {
            if (activePromptRef.current?.requestId === data.requestId) {
              clearActive()
            }
          }
        } catch { /* ping */ }
      }

      es.onerror = () => {
        es.close()
        setTimeout(connect, 3000)
      }
    }

    connect()
    return () => {
      sseRef.current?.close()
      document.title = origTitle.current
    }
  }, [])

  const activePromptRef = useRef(null)
  useEffect(() => { activePromptRef.current = activePrompt }, [activePrompt])

  // ── Socket.io: detect ag_chat jobs ────────────────────────────────────────
  useEffect(() => {
    const onWaiting = () => {
      setIsAgChatJob(true)
      setWaiting(true)
      setExpanded(true) // auto-expand on new prompt
    }
    const onDone = () => {
      setIsAgChatJob(false)
      setWaiting(false)
      document.title = origTitle.current
    }

    socket.on('pipeline:ag_chat_waiting', onWaiting)
    socket.on('pipeline:completed',  onDone)
    socket.on('pipeline:rejected',   onDone)
    return () => {
      socket.off('pipeline:ag_chat_waiting', onWaiting)
      socket.off('pipeline:completed')
      socket.off('pipeline:rejected')
    }
  }, [])

  // ── Page title ────────────────────────────────────────────────────────────
  useEffect(() => {
    if (activePrompt && !submitted) {
      document.title = `⚡ IDE Agent Answering...`
    } else {
      document.title = origTitle.current
    }
    return () => { document.title = origTitle.current }
  }, [activePrompt, submitted])

  const handlePromptArrived = useCallback((prompt) => {
    setActivePrompt(prompt)
    setSubmitted(false)
    setWaiting(true)
    setExpanded(true)
  }, [])

  const clearActive = () => {
    setActivePrompt(null)
    setSubmitted(false)
    setWaiting(false)
    document.title = origTitle.current
  }

  const isPulsing = waiting && activePrompt && !submitted

  const handleToggleExpand = (e) => {
    if (isDragging.current) return
    setExpanded(prev => !prev)
  }

  return (
    <AnimatePresence>
      <motion.div
        drag
        dragMomentum={false}
        dragElastic={0.08}
        dragThreshold={8}
        onDragStart={() => {
          isDragging.current = true
        }}
        onDragEnd={() => {
          setTimeout(() => {
            isDragging.current = false
          }, 80)
        }}
        // Reverted to inline relative constraints
        dragConstraints={{
          left: -window.innerWidth + 100,
          right: 20,
          top: -window.innerHeight + 100,
          bottom: 20
        }}
        whileDrag={{ cursor: 'grabbing', scale: 1.05 }}
        initial={{ opacity: 0, scale: 0.8, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.8, y: 20 }}
        style={{
          position: 'fixed',
          bottom: 28,
          right: 28,
          zIndex: 9999,
          cursor: 'grab',
          userSelect: 'none',
          touchAction: 'none',
        }}
      >
        <motion.div
          onClick={handleToggleExpand}
          animate={{
            width: expanded ? 290 : 56,
          }}
          transition={{ type: 'spring', stiffness: 260, damping: 22 }}
          style={{
            display: 'flex',
            alignItems: 'center',
            background: 'rgba(15,15,25,0.92)',
            backdropFilter: 'blur(12px)',
            borderRadius: 28,
            padding: 8,
            border: `1.5px solid ${isPulsing ? 'rgba(139,92,246,0.6)' : 'rgba(255,255,255,0.06)'}`,
            boxShadow: isPulsing 
              ? '0 0 24px rgba(139,92,246,0.35), 0 4px 16px rgba(0,0,0,0.5)' 
              : '0 8px 24px rgba(0,0,0,0.35)',
            gap: 12,
            overflow: 'hidden',
            height: 56,
            boxSizing: 'border-box',
          }}
        >
          {/* Main Glowing Brain Icon Circle */}
          <div style={{
            width: 38,
            height: 38,
            borderRadius: '50%',
            background: isPulsing 
              ? 'linear-gradient(135deg, #a78bfa, #7c3aed)' 
              : 'linear-gradient(135deg, #6366f1, #4f46e5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0,
            boxShadow: isPulsing ? '0 0 12px rgba(139,92,246,0.5)' : 'none',
          }}>
            {isPulsing ? (
              <Loader2 size={18} color="#fff" style={{ animation: 'spin 1.5s linear infinite' }} />
            ) : (
              <Brain size={20} color="#fff" />
            )}
          </div>

          {/* Side-expanding status details */}
          {expanded && (
            <motion.div
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -10 }}
              transition={{ delay: 0.05 }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 12,
                whiteSpace: 'nowrap',
                overflow: 'hidden',
              }}
            >
              <div style={{ display: 'flex', flexDirection: 'column' }}>
                <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text)', lineHeight: 1.25 }}>
                  {isPulsing ? `IDE Agent: Processing...` : `IDE Agent: Ready`}
                </span>
                <span style={{ fontSize: 9.5, color: 'var(--text-dim)', marginTop: 2, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 210 }}>
                  {isPulsing ? `Answering ${activePrompt.agentName}` : `Idle — drag anywhere`}
                </span>
              </div>

              {isPulsing && (
                <div style={{
                  width: 6, height: 6, borderRadius: '50%',
                  background: 'var(--purple)',
                  animation: 'pulse-dot 1.2s ease-in-out infinite',
                  marginRight: 4,
                  flexShrink: 0,
                }} />
              )}
            </motion.div>
          )}
        </motion.div>
      </motion.div>
    </AnimatePresence>
  )
}
