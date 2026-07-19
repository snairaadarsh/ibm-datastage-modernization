/**
 * Route: Job Management
 * CRUD for jobs + human review approve/reject.
 */

const express = require('express');
const { getJob, getAllJobs, updateJob, deleteJob } = require('../job-store');
const { resumeJobAfterApproval } = require('../pipeline-bridge');

module.exports = function (io) {
  const router = express.Router();

  // List all jobs
  router.get('/jobs', (req, res) => {
    const sanitizedJobs = getAllJobs().map(j => {
      const copy = { ...j };
      if (copy.apiKey) copy.apiKey = '****';
      return copy;
    });
    res.json(sanitizedJobs);
  });

  // Get single job
  router.get('/jobs/:id', (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    const copy = { ...job };
    if (copy.apiKey) copy.apiKey = '****';
    res.json(copy);
  });

  // Approve human review — resume pipeline
  router.post('/jobs/:id/approve', (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    if (job.status !== 'human_review') {
      return res.status(400).json({ error: 'Job is not in human_review state' });
    }

    updateJob(req.params.id, {
      humanReview: { ...job.humanReview, decision: 'approved', decidedAt: new Date().toISOString() },
      humanReviewDecision: {
        decision: 'proceed',
        comment: '',
        decidedAt: new Date().toISOString()
      }
    });

    res.json({ success: true, message: 'Job approved — resuming pipeline' });
    setImmediate(() => resumeJobAfterApproval(req.params.id, io));
  });

  // Reject human review — mark as failed
  router.post('/jobs/:id/reject', (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const { reason = 'Rejected by reviewer' } = req.body;
    updateJob(req.params.id, {
      status: 'failed',
      error: `Rejected during human review: ${reason}`,
      humanReview: { ...job.humanReview, decision: 'rejected', reason, decidedAt: new Date().toISOString() }
    });

    io.to(req.params.id).emit('pipeline:rejected', { jobId: req.params.id, reason });
    res.json({ success: true, message: 'Job rejected' });
  });

  // Request changes (sends back to review with notes)
  router.post('/jobs/:id/request-changes', express.json(), (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const { notes = '' } = req.body;
    updateJob(req.params.id, {
      status: 'human_review',
      humanReview: { ...job.humanReview, requestedChanges: notes, changesRequestedAt: new Date().toISOString() }
    });
    res.json({ success: true, message: 'Changes requested' });
  });

  // ── Human review decision (Proceed / Stop / Add Comment) ─────────────────
  // Browser POSTs the user's decision here.
  router.post('/jobs/:id/human-review-decision', express.json(), (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const { decision, comment = '' } = req.body;
    if (!['proceed', 'stop', 'comment'].includes(decision)) {
      return res.status(400).json({ error: 'decision must be proceed | stop | comment' });
    }

    updateJob(req.params.id, {
      humanReviewDecision: {
        decision,
        comment,
        decidedAt: new Date().toISOString(),
      },
      // If stopping, mark as failed immediately
      ...(decision === 'stop' ? { status: 'failed', error: 'Stopped by user during human review' } : {}),
      // If proceeding or commenting, mark as processing again
      ...(decision !== 'stop' ? { status: 'processing' } : {}),
    });

    // Notify UI via socket
    io.to(req.params.id).emit('pipeline:review_decision', { jobId: req.params.id, decision, comment });
    res.json({ success: true, decision });
  });

  // Python pipeline_server polls this until a decision is made.
  router.get('/jobs/:id/human-review-decision', (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const d = job.humanReviewDecision;
    if (!d || !d.decision) {
      // Not decided yet — return 404 so Python knows to keep polling
      return res.status(404).json({ decision: null, status: 'pending' });
    }

    // Return the decision (Python reads this once then pipeline continues)
    res.json({ decision: d.decision, comment: d.comment || '', decidedAt: d.decidedAt });
  });

  // ── Validation review decision (Proceed / AI Fix / Add Comment) ────────────
  // Browser POSTs the user's decision here when validation score < 90%.
  router.post('/jobs/:id/validation-decision', express.json(), (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const { decision, comment = '' } = req.body;
    if (!['proceed', 'ai_fix', 'comment'].includes(decision)) {
      return res.status(400).json({ error: 'decision must be proceed | ai_fix | comment' });
    }

    updateJob(req.params.id, {
      validationDecision: {
        decision,
        comment,
        decidedAt: new Date().toISOString(),
      },
    });

    io.to(req.params.id).emit('pipeline:validation_decision', { jobId: req.params.id, decision, comment });
    res.json({ success: true, decision });
  });

  // Python pipeline_server polls this until a validation decision is made.
  router.get('/jobs/:id/validation-decision', (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const d = job.validationDecision;
    if (!d || !d.decision) {
      return res.status(404).json({ decision: null, status: 'pending' });
    }

    // Clear decision so subsequent validation loops can wait for a fresh decision
    updateJob(req.params.id, { validationDecision: null });
    res.json({ decision: d.decision, comment: d.comment || '', decidedAt: d.decidedAt });
  });

  // Delete job

  router.delete('/jobs/:id', (req, res) => {
    const job = getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    // If the job is active and has a running child PID, kill it
    if (job.pid && job.status === 'processing') {
      try {
        process.kill(job.pid);
        console.log(`[JobsRoute] Killed active python process PID ${job.pid} for deleted job ${req.params.id}`);
      } catch (e) {
        console.warn(`[JobsRoute] Could not kill process PID ${job.pid}: ${e.message}`);
      }
    }

    const deleted = deleteJob(req.params.id);
    res.json({ success: deleted });
  });

  return router;
};
