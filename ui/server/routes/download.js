/**
 * Route: File Downloads
 * Serves generated outputs for download — individual files + ZIP bundle.
 */

const express = require('express');
const archiver = require('archiver');
const { getJob } = require('../job-store');

module.exports = function () {
  const router = express.Router();

  const getOutput = (jobId, type, res) => {
    const job = getJob(jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    if (!job.outputs || !job.outputs[type]) {
      return res.status(404).json({ error: `Output '${type}' not yet generated` });
    }
    return job.outputs[type];
  };

  // Download PySpark script
  router.get('/download/:jobId/pyspark', (req, res) => {
    const content = getOutput(req.params.jobId, 'pyspark', res);
    if (!content) return;
    const job = getJob(req.params.jobId);
    const name = (job.originalname || 'job').replace(/\.(dsx|isx)$/i, '');
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_pyspark.py"`);
    res.send(content);
  });

  // Download DataFusion JSON
  router.get('/download/:jobId/datafusion', (req, res) => {
    const content = getOutput(req.params.jobId, 'datafusion', res);
    if (!content) return;
    const job = getJob(req.params.jobId);
    const name = (job.originalname || 'job').replace(/\.(dsx|isx)$/i, '');
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_datafusion.json"`);
    res.send(content);
  });

  // Download Unit Tests
  router.get('/download/:jobId/tests', (req, res) => {
    const content = getOutput(req.params.jobId, 'tests', res);
    if (!content) return;
    const job = getJob(req.params.jobId);
    const name = (job.originalname || 'job').replace(/\.(dsx|isx)$/i, '');
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="test_${name}.py"`);
    res.send(content);
  });

  // Download Migration Report (JSON)
  router.get('/download/:jobId/report', (req, res) => {
    const content = getOutput(req.params.jobId, 'report', res);
    if (!content) return;
    const job = getJob(req.params.jobId);
    const name = (job.originalname || 'job').replace(/\.(dsx|isx)$/i, '');
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_migration_report.json"`);
    res.send(content);
  });

  // Download All as ZIP
  router.get('/download/:jobId/zip', (req, res) => {
    const job = getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    if (job.status !== 'completed') {
      return res.status(400).json({ error: 'Job not yet completed' });
    }

    const name = (job.originalname || 'job').replace(/\.(dsx|isx)$/i, '');
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_migration_outputs.zip"`);

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    if (job.outputs.pyspark)    archive.append(job.outputs.pyspark, { name: `${name}_pyspark.py` });
    if (job.outputs.datafusion) archive.append(job.outputs.datafusion, { name: `${name}_datafusion.json` });
    if (job.outputs.tests)      archive.append(job.outputs.tests, { name: `test_${name}.py` });
    if (job.outputs.report)     archive.append(job.outputs.report, { name: `${name}_migration_report.json` });

    archive.finalize();
  });

  return router;
};
