/**
 * Route: File Downloads
 * Serves generated outputs for download — individual files + ZIP bundle.
 * Supports all 8 v2.0 artifacts.
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

  const getJobName = (jobId) => {
    const job = getJob(jobId);
    return (job?.originalname || 'job').replace(/\.(dsx|isx)$/i, '');
  };

  // Download PySpark script
  router.get('/download/:jobId/pyspark', (req, res) => {
    const content = getOutput(req.params.jobId, 'pyspark', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_pyspark.py"`);
    res.send(content);
  });

  // Download DataFusion JSON
  router.get('/download/:jobId/datafusion', (req, res) => {
    const content = getOutput(req.params.jobId, 'datafusion', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_datafusion.json"`);
    res.send(content);
  });

  // Download Unit Tests
  router.get('/download/:jobId/tests', (req, res) => {
    const content = getOutput(req.params.jobId, 'tests', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="test_${name}.py"`);
    res.send(content);
  });

  // Download Migration Report (JSON) — legacy summary report
  router.get('/download/:jobId/report', (req, res) => {
    const content = getOutput(req.params.jobId, 'report', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_report.json"`);
    res.send(content);
  });

  // Download Migration Report v2.0 (detailed)
  router.get('/download/:jobId/migration_report', (req, res) => {
    const content = getOutput(req.params.jobId, 'migration_report', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_migration_report.json"`);
    res.send(content);
  });

  // Download Validation Report v2.0
  router.get('/download/:jobId/validation_report', (req, res) => {
    const content = getOutput(req.params.jobId, 'validation_report', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_validation_report.json"`);
    res.send(content);
  });

  // Download Metadata Catalog v2.0
  router.get('/download/:jobId/metadata', (req, res) => {
    const content = getOutput(req.params.jobId, 'metadata', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_metadata.json"`);
    res.send(content);
  });

  // Download Data Lineage Graph v2.0
  router.get('/download/:jobId/lineage', (req, res) => {
    const content = getOutput(req.params.jobId, 'lineage', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_lineage.json"`);
    res.send(content);
  });

  // Download DDL Statements v2.0
  router.get('/download/:jobId/ddl', (req, res) => {
    const content = getOutput(req.params.jobId, 'ddl', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="${name}_ddl.sql"`);
    res.send(content);
  });

  // Download Test Harness (pytest) v2.0
  router.get('/download/:jobId/test_harness', (req, res) => {
    const content = getOutput(req.params.jobId, 'test_harness', res);
    if (!content) return;
    const name = getJobName(req.params.jobId);
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Content-Disposition', `attachment; filename="test_harness_${name}.py"`);
    res.send(content);
  });

  // Download All as ZIP — includes all 8+ artifacts
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

    if (job.outputs.pyspark)           archive.append(job.outputs.pyspark,           { name: `${name}_pyspark.py` });
    if (job.outputs.datafusion)        archive.append(job.outputs.datafusion,        { name: `${name}_datafusion.json` });
    if (job.outputs.tests)             archive.append(job.outputs.tests,             { name: `test_${name}.py` });
    if (job.outputs.report)            archive.append(job.outputs.report,            { name: `${name}_report.json` });
    if (job.outputs.migration_report)  archive.append(job.outputs.migration_report,  { name: `${name}_migration_report.json` });
    if (job.outputs.validation_report) archive.append(job.outputs.validation_report, { name: `${name}_validation_report.json` });
    if (job.outputs.metadata)          archive.append(job.outputs.metadata,          { name: `${name}_metadata.json` });
    if (job.outputs.lineage)           archive.append(job.outputs.lineage,           { name: `${name}_lineage.json` });
    if (job.outputs.ddl)               archive.append(job.outputs.ddl,               { name: `${name}_ddl.sql` });
    if (job.outputs.test_harness)      archive.append(job.outputs.test_harness,      { name: `test_harness_${name}.py` });

    archive.finalize();
  });

  return router;
};
