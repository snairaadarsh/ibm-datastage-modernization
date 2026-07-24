/**
 * Pipeline Bridge — Spawns the real Python pipeline and streams events via Socket.io.
 * Replaces all simulation/mock code with actual AI agent execution.
 *
 * The Python process (pipeline_server.py) writes JSON event lines to stdout:
 *   {"event":"stage",          "stageId":"parse",   "status":"active"}
 *   {"event":"log",            "level":"info",       "message":"..."}
 *   {"event":"classification", "complexity":"Medium","confidenceScore":94.2}
 *   {"event":"human_review",   "reason":"...",       "flaggedStages":[...]}
 *   {"event":"completed",      "pyspark":"...",       "datafusion":"...", ...}
 *   {"event":"failed",         "error":"..."}
 *   {"event":"ag_chat_waiting","requestId":"...",    "agentName":"..."}
 */

const { spawn }  = require('child_process');
const path       = require('path');
const fs         = require('fs');
const { setStageStatus, addLog, updateJob, getJob } = require('./job-store');

const PROJECT_ROOT = path.join(__dirname, '../../');

/**
 * Run the Python pipeline for a real job.
 * Streams JSON events from stdout → Socket.io.
 */
function runPipeline(jobId, filePath, io, providerConfig = {}) {
  const { provider = 'gemini', apiKey: rawApiKey = '', model = '' } = providerConfig;

  // Resolve the API key: if client sent 'env', read from server environment variables
  let resolvedApiKey = rawApiKey;
  if (!rawApiKey || rawApiKey === 'env') {
    const envKeyMap = {
      gemini:    process.env.GEMINI_API_KEY    || process.env.LLM_API_KEY || '',
      openai:    process.env.OPENAI_API_KEY    || process.env.LLM_API_KEY || '',
      anthropic: process.env.ANTHROPIC_API_KEY || process.env.LLM_API_KEY || '',
    };
    resolvedApiKey = envKeyMap[provider] || process.env.LLM_API_KEY || '';
    console.log(`[Bridge] Using API key from .env for provider: ${provider}`);
  }

  const apiKey = resolvedApiKey;

  const job = getJob(jobId);
  if (!job) { console.error(`[Bridge] Job ${jobId} not found`); return; }

  const emit = (event, data) => io.to(jobId).emit(event, data);

  const log = (level, msg) => {
    addLog(jobId, level, msg);
    emit('pipeline:log', { level, message: msg, ts: new Date().toISOString() });
  };

  updateJob(jobId, { status: 'processing' });
  emit('pipeline:started', { jobId, timestamp: new Date().toISOString() });

  const pythonBin = process.env.PYTHON_BIN || 'python';

  const args = [
    path.join(PROJECT_ROOT, 'pipeline_server.py'),
    '--server-mode',
    '--input',      filePath,
    '--job-id',     jobId,
    '--provider',   provider,
    '--api-key',    apiKey,
    '--model',      model,
    '--bridge-url', `http://localhost:${process.env.PORT || 3001}`,
  ];

  console.log(`[Bridge] Spawning: ${pythonBin} ${args.slice(0,3).join(' ')} ...`);

  const child = spawn(pythonBin, args, {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      LLM_PROVIDER: provider,
      LLM_API_KEY:  apiKey,
      LLM_MODEL:    model,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  // Store child PID so we can kill if needed
  updateJob(jobId, { pid: child.pid });

  let buffer = '';

  // ── stdout: JSON event lines ───────────────────────────────────────────────
  child.stdout.on('data', (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop(); // keep incomplete last line

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const msg = JSON.parse(trimmed);
        handlePipelineEvent(msg, jobId, io, emit, log);
      } catch {
        // Non-JSON stdout (e.g. rich/progress output from sub-imports) — log as debug
        if (trimmed) log('info', trimmed.slice(0, 200));
      }
    }
  });

  // ── stderr: Python errors/warnings ────────────────────────────────────────
  child.stderr.on('data', (chunk) => {
    const text = chunk.toString().trim();
    if (text) {
      console.error(`[Bridge stderr] ${text}`);
      // Only surface actual errors (not logging noise)
      if (text.toLowerCase().includes('error') || text.toLowerCase().includes('traceback')) {
        log('error', text.slice(0, 400));
      }
    }
  });

  // ── Process exit ───────────────────────────────────────────────────────────
  child.on('exit', (code) => {
    console.log(`[Bridge] Python process exited with code ${code} for job ${jobId}`);
    const currentJob = getJob(jobId);
    if (code !== 0 && currentJob?.status === 'processing') {
      const errMsg = `Pipeline process exited with code ${code}`;
      updateJob(jobId, { status: 'failed', error: errMsg });
      log('error', errMsg);
      emit('pipeline:rejected', { jobId, reason: errMsg });
    }
  });

  child.on('error', (err) => {
    console.error(`[Bridge] Failed to spawn Python: ${err.message}`);
    const errMsg = `Failed to start pipeline: ${err.message}. Is Python installed and in PATH?`;
    updateJob(jobId, { status: 'failed', error: errMsg });
    log('error', errMsg);
    emit('pipeline:rejected', { jobId, reason: errMsg });
  });
}

/**
 * Handle a single parsed JSON event from the Python pipeline stdout.
 */
function handlePipelineEvent(msg, jobId, io, emit, log) {
  const { event } = msg;

  switch (event) {
    case 'stage': {
      const { stageId, status } = msg;
      setStageStatus(jobId, stageId, status);
      emit('pipeline:stage', { stageId, status });
      break;
    }

    case 'log': {
      const { level = 'info', message = '' } = msg;
      addLog(jobId, level, message);
      emit('pipeline:log', { level, message, ts: new Date().toISOString() });
      break;
    }

    case 'classification': {
      const classification = {
        complexity:      msg.complexity      || 'Medium',
        confidenceScore: msg.confidenceScore || 85,
        stageCount:      msg.stageCount      || 0,
      };
      updateJob(jobId, { classification });
      emit('pipeline:classification', classification);
      break;
    }

    case 'human_review': {
      const hr = {
        reason:          msg.reason        || 'Complexity threshold exceeded',
        llmExplanation:  msg.llmExplanation || '',
        complexity:      msg.complexity     || 'medium',
        flaggedStages:   msg.flaggedStages  || [],
        ambiguityFlags:  msg.ambiguityFlags || [],
        recommendations: msg.recommendations || [],
        riskAreas:       msg.riskAreas      || [],
        confidence:      msg.confidence     || 0,
        requiresApproval: true,
      };
      const existingJob = getJob(jobId);
      const hasDecision = existingJob && existingJob.humanReviewDecision && ['proceed', 'comment'].includes(existingJob.humanReviewDecision.decision);
      
      if (hasDecision) {
        // Keep status as processing and do not clear the decision
        updateJob(jobId, { humanReview: hr });
      } else {
        updateJob(jobId, { status: 'human_review', humanReview: hr, humanReviewDecision: null });
        emit('pipeline:human_review', { jobId, humanReview: hr, reason: hr.reason });
      }
      break;
    }

    case 'validation_review': {
      const vr = {
        score:          msg.score          ?? 0,
        summary:        msg.summary        || '',
        issues:         msg.issues         || [],
        strengths:      msg.strengths      || [],
        critical_issues: msg.critical_issues || false,
        loopCount:      msg.loopCount      ?? 0,
        maxLoops:       msg.maxLoops       ?? 3,
        canRetry:       msg.canRetry       ?? true,
      };
      updateJob(jobId, { status: 'validation_review', validationReview: vr, validationDecision: null });
      emit('pipeline:validation_review', vr);
      break;
    }

    case 'ag_chat_waiting': {
      // Tell the UI an agent is waiting for AG Chat response
      emit('pipeline:ag_chat_waiting', {
        requestId: msg.requestId,
        agentName: msg.agentName,
        jobId,
      });
      break;
    }

    case 'completed': {
      const { pyspark, datafusion, tests, report, elapsedSeconds } = msg;
      updateJob(jobId, {
        status: 'completed',
        outputs: { pyspark, datafusion, tests, report },
        elapsedSeconds: elapsedSeconds || 0,
      });
      emit('pipeline:completed', {
        jobId,
        status: 'completed',
        outputs: { pyspark: !!pyspark, datafusion: !!datafusion, tests: !!tests, report: !!report },
        elapsedSeconds,
        report: report ? JSON.parse(report) : null,
      });
      break;
    }

    case 'failed': {
      const { error } = msg;
      updateJob(jobId, { status: 'failed', error });
      addLog(jobId, 'error', error);
      io.to(jobId).emit('pipeline:log', { level: 'error', message: error, ts: new Date().toISOString() });
      io.to(jobId).emit('pipeline:rejected', { jobId, reason: error });
      break;
    }

    default:
      console.log(`[Bridge] Unknown event: ${event}`, msg);
  }
}

/**
 * Resume a job after human review approval.
 * Re-spawns Python from the translate stage onward.
 */
function resumeJobAfterApproval(jobId, io) {
  const job = getJob(jobId);
  if (!job) return;

  const filePath = path.join(PROJECT_ROOT, 'uploads', job.filename);
  const providerConfig = {
    provider: job.provider || 'gemini',
    apiKey:   job.apiKey   || '',
    model:    job.model    || '',
  };

  const emit = (event, data) => io.to(jobId).emit(event, data);
  const log  = (level, msg) => {
    addLog(jobId, level, msg);
    emit('pipeline:log', { level, message: msg, ts: new Date().toISOString() });
  };

  updateJob(jobId, {
    status: 'processing',
    humanReview: { ...job.humanReview, decision: 'approved', decidedAt: new Date().toISOString() },
  });
  emit('pipeline:resumed', { jobId });
  log('success', 'Human approval received — resuming pipeline with AI agents...');

  // Re-run full pipeline (Python handles resume logic by skipping already-done stages)
  runPipeline(jobId, filePath, io, providerConfig);
}

module.exports = { runPipeline, resumeJobAfterApproval };
