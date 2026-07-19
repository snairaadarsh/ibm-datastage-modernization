/**
 * Route: File Upload
 * Accepts .dsx and .isx files, creates a job with provider config,
 * and triggers the real Python pipeline.
 */

const express = require('express');
const multer  = require('multer');
const path    = require('path');
const fs      = require('fs');
const { createJob }     = require('../job-store');
const { runPipeline }   = require('../pipeline-bridge');

const UPLOAD_DIR = path.join(__dirname, '../../../uploads');

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
    cb(null, UPLOAD_DIR);
  },
  filename: (req, file, cb) => {
    const unique = `${Date.now()}-${Math.round(Math.random() * 1e6)}`;
    cb(null, `${unique}-${file.originalname}`);
  },
});

const fileFilter = (req, file, cb) => {
  const ext = path.extname(file.originalname).toLowerCase();
  if (['.dsx', '.isx', '.xml'].includes(ext)) {
    cb(null, true);
  } else {
    cb(new Error('Only .dsx, .isx, and .xml files are supported'), false);
  }
};

const upload = multer({ storage, fileFilter, limits: { fileSize: 50 * 1024 * 1024 } });

module.exports = function (io) {
  const router = express.Router();

  router.post('/upload', upload.single('file'), (req, res) => {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded or invalid file type' });
    }

    // Provider config from multipart form fields
    const providerConfig = {
      provider: req.body.provider || 'gemini',
      apiKey:   req.body.apiKey   || '',
      model:    req.body.model    || '',
    };

    const job = createJob(req.file.filename, req.file.originalname, providerConfig);

    res.json({
      success:  true,
      jobId:    job.id,
      filename: req.file.originalname,
      size:     req.file.size,
      provider: providerConfig.provider,
    });

    // Kick off real pipeline asynchronously
    const filePath = path.join(UPLOAD_DIR, req.file.filename);
    setImmediate(() => runPipeline(job.id, filePath, io, providerConfig));
  });

  return router;
};
