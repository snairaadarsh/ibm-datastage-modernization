/**
 * Persistent job store for the DataStage Modernization prototype.
 * Tracks job state, stage progress, outputs, and human review flags.
 * Survives server restarts by writing to jobs.json.
 */

const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const JOBS_FILE = path.join(__dirname, 'jobs.json');

const STAGES = [
  { id: 'parse',     label: 'Parse XML',   icon: 'FileCode',   desc: 'Parsing DataStage XML structure' },
  { id: 'normalize', label: 'Normalize',   icon: 'GitMerge',   desc: 'Normalizing to canonical schema' },
  { id: 'classify',  label: 'Classify',    icon: 'BarChart2',  desc: 'Scoring complexity & confidence' },
  { id: 'translate', label: 'Translate',   icon: 'Zap',        desc: 'Generating PySpark & DataFusion' },
  { id: 'review',    label: 'Code Review', icon: 'ShieldCheck', desc: 'AI code review & optimizations' },
  { id: 'validate',  label: 'Validate',    icon: 'CheckCircle', desc: 'Schema & data validation' },
  { id: 'report',    label: 'Report',      icon: 'FileText',   desc: 'Generating migration report' },
];

const jobs = new Map();

// Save state to JSON file
function saveStore() {
  try {
    fs.writeFileSync(JOBS_FILE, JSON.stringify(Array.from(jobs.entries()), null, 2), 'utf-8');
  } catch (err) {
    console.error('[JobStore] Save failed:', err);
  }
}

// Load state from JSON file
function loadStore() {
  try {
    if (fs.existsSync(JOBS_FILE)) {
      const content = fs.readFileSync(JOBS_FILE, 'utf-8');
      const data = JSON.parse(content);
      for (const [id, job] of data) {
        jobs.set(id, job);
      }
    }
  } catch (err) {
    console.error('[JobStore] Load failed:', err);
  }
}

// Init load
loadStore();

function createJob(filename, originalname, providerConfig = {}) {
  const id = uuidv4();
  const job = {
    id,
    filename,
    originalname,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    status: 'pending',
    provider: providerConfig.provider || 'gemini',
    apiKey:   providerConfig.apiKey   || '',
    model:    providerConfig.model    || '',
    stages: STAGES.map(s => ({ ...s, status: 'pending', startedAt: null, completedAt: null, duration: null })),
    currentStage: null,
    logs: [],
    classification: null,
    outputs: {
      pyspark: null,
      datafusion: null,
      tests: null,
      report: null,
    },
    humanReview: null,
    humanReviewDecision: null,
    error: null,
    elapsedSeconds: 0,
    pid: null,
  };
  jobs.set(id, job);
  saveStore();
  return job;
}

function getJob(id) {
  return jobs.get(id) || null;
}

function getAllJobs() {
  return Array.from(jobs.values()).sort(
    (a, b) => new Date(b.createdAt) - new Date(a.createdAt)
  );
}

function updateJob(id, updates) {
  const job = jobs.get(id);
  if (!job) return null;
  Object.assign(job, updates, { updatedAt: new Date().toISOString() });
  saveStore();
  return job;
}

function setStageStatus(jobId, stageId, status, extras = {}) {
  const job = jobs.get(jobId);
  if (!job) return;
  const stage = job.stages.find(s => s.id === stageId);
  if (!stage) return;
  stage.status = status;
  if (status === 'active') stage.startedAt = new Date().toISOString();
  if (status === 'done' || status === 'error') {
    stage.completedAt = new Date().toISOString();
    if (stage.startedAt) {
      stage.duration = ((new Date(stage.completedAt) - new Date(stage.startedAt)) / 1000).toFixed(1);
    }
  }
  Object.assign(stage, extras);
  job.updatedAt = new Date().toISOString();
  saveStore();
}

function addLog(jobId, level, message) {
  const job = jobs.get(jobId);
  if (!job) return;
  job.logs.push({
    ts: new Date().toISOString(),
    level,
    message,
  });
  saveStore();
}

function deleteJob(id) {
  const result = jobs.delete(id);
  if (result) saveStore();
  return result;
}

module.exports = { STAGES, createJob, getJob, getAllJobs, updateJob, setStageStatus, addLog, deleteJob };
