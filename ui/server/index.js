/**
 * IBM DataStage Modernization Suite — Express Server
 * Handles file uploads, job orchestration, WebSocket events, and file downloads.
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const uploadRoutes   = require('./routes/upload');
const jobRoutes      = require('./routes/jobs');
const downloadRoutes = require('./routes/download');
const agChatRoutes   = require('./routes/ag-chat');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

// ── Middleware ─────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Uploads directory ──────────────────────────────────────────────────────────
const UPLOAD_DIR = path.join(__dirname, '../../uploads');
const OUTPUT_DIR = path.join(__dirname, '../../output');
[UPLOAD_DIR, OUTPUT_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});
app.use('/uploads', express.static(UPLOAD_DIR));

// ── Routes ─────────────────────────────────────────────────────────────────────
app.use('/api', uploadRoutes(io));
app.use('/api', jobRoutes(io));
app.use('/api', downloadRoutes());
app.use('/api', agChatRoutes());

// ── Health check ───────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ── Socket.io ──────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[Socket] Client connected: ${socket.id}`);

  socket.on('job:subscribe', (jobId) => {
    socket.join(jobId);
    console.log(`[Socket] Client ${socket.id} subscribed to job ${jobId}`);
  });

  socket.on('job:unsubscribe', (jobId) => {
    socket.leave(jobId);
  });

  socket.on('disconnect', () => {
    console.log(`[Socket] Client disconnected: ${socket.id}`);
  });
});

// ── Start ──────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`\n  ╔══════════════════════════════════════════╗`);
  console.log(`  ║  DataStage Modernization Server          ║`);
  console.log(`  ║  Running on http://localhost:${PORT}        ║`);
  console.log(`  ╚══════════════════════════════════════════╝\n`);
});

module.exports = { app, io };
