/**
 * AG Chat Bridge — Routes LLM prompts from the Python pipeline to the
 * Antigravity IDE agent (the user's active chat session) via SSE.
 *
 * Flow:
 *   1. Python agent  →  POST /api/ag-chat/prompt          (submit prompt, get requestId)
 *   2. Browser UI    →  GET  /api/ag-chat/stream           (SSE — receives prompt events)
 *   3. Browser UI    →  POST /api/ag-chat/response/:id     (submit AI response)
 *   4. Python agent  →  GET  /api/ag-chat/response/:id     (poll until response arrives)
 */

const express = require('express');

// In-memory queues (sufficient for a single-server prototype)
const pendingPrompts  = new Map(); // requestId → prompt object
const responseQueue   = new Map(); // requestId → response string
const sseClients      = new Set(); // active SSE connections

function broadcastToSSE(data) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  sseClients.forEach(res => {
    try { res.write(payload); } catch { /* client disconnected */ }
  });
}

module.exports = function () {
  const router = express.Router();

  // ── 1. Python posts a prompt here ────────────────────────────────────────
  router.post('/ag-chat/prompt', express.json(), (req, res) => {
    const { requestId, jobId, agentName, systemPrompt, userPrompt } = req.body;

    if (!requestId || !userPrompt) {
      return res.status(400).json({ error: 'requestId and userPrompt required' });
    }

    const prompt = {
      requestId,
      jobId:        jobId        || null,
      agentName:    agentName    || 'Agent',
      systemPrompt: systemPrompt || '',
      userPrompt,
      receivedAt:   new Date().toISOString(),
    };

    pendingPrompts.set(requestId, prompt);
    console.log(`[AgChat] Prompt queued: requestId=${requestId} agent=${agentName}`);

    // Broadcast to all connected SSE clients (the UI's AgChatBridge component)
    broadcastToSSE({ type: 'prompt', ...prompt });

    res.json({ success: true, requestId });
  });

  // ── 2. Browser subscribes here for real-time prompt events ───────────────
  router.get('/ag-chat/stream', (req, res) => {
    res.setHeader('Content-Type',  'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection',    'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    sseClients.add(res);
    console.log(`[AgChat] SSE client connected. Total: ${sseClients.size}`);

    // Send any already-pending prompts immediately (in case browser reconnected)
    pendingPrompts.forEach(prompt => {
      if (!responseQueue.has(prompt.requestId)) {
        res.write(`data: ${JSON.stringify({ type: 'prompt', ...prompt })}\n\n`);
      }
    });

    // Keep-alive ping every 25s
    const keepAlive = setInterval(() => {
      try { res.write(': ping\n\n'); } catch { clearInterval(keepAlive); }
    }, 25000);

    req.on('close', () => {
      sseClients.delete(res);
      clearInterval(keepAlive);
      console.log(`[AgChat] SSE client disconnected. Total: ${sseClients.size}`);
    });
  });

  // ── 3. Browser posts the AI response back ────────────────────────────────
  router.post('/ag-chat/response/:requestId', express.json(), (req, res) => {
    const { requestId } = req.params;
    const { response } = req.body;

    if (!response || typeof response !== 'string') {
      return res.status(400).json({ error: 'response string required' });
    }

    responseQueue.set(requestId, response);
    pendingPrompts.delete(requestId);

    console.log(`[AgChat] Response received for requestId=${requestId} (${response.length} chars)`);

    // Broadcast completion event so other UI components can update
    broadcastToSSE({ type: 'response_received', requestId });

    res.json({ success: true, requestId });
  });

  // ── 4. Python polls here until response arrives ──────────────────────────
  router.get('/ag-chat/response/:requestId', (req, res) => {
    const { requestId } = req.params;
    const response = responseQueue.get(requestId);

    if (response !== undefined) {
      // Response is ready — return it and clean up
      responseQueue.delete(requestId);
      return res.json({ requestId, response });
    }

    // Not ready yet — return 202 Accepted so Python keeps polling
    res.status(202).json({ requestId, response: null, status: 'pending' });
  });

  // ── 5. List pending prompts (for UI on reconnect) ────────────────────────
  router.get('/ag-chat/pending', (req, res) => {
    const pending = [];
    pendingPrompts.forEach((prompt) => {
      if (!responseQueue.has(prompt.requestId)) {
        pending.push(prompt);
      }
    });
    res.json({ pending });
  });

  // ── 6. Cancel a pending prompt ───────────────────────────────────────────
  router.delete('/ag-chat/prompt/:requestId', (req, res) => {
    const { requestId } = req.params;
    pendingPrompts.delete(requestId);
    broadcastToSSE({ type: 'cancelled', requestId });
    res.json({ success: true });
  });

  return router;
};
