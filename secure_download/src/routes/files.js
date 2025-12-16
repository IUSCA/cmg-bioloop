const express = require('express');
const config = require('config');
const fs = require('fs');
const path = require('path');
const createError = require('http-errors');
const { param } = require('express-validator');
const { validate } = require('../middleware/validators');
const asyncHandler = require('../middleware/asyncHandler');
const logger = require('../services/logger');

const router = express.Router();

const DATA_ROOT = config.get('data_root');
const SCOPE_PREFIX = config.get('genome_browser_scope_prefix');

// Helper to determine MIME type based on file extension
const getMimeType = (filePath) => {
  const ext = path.extname(filePath).toLowerCase();
  switch (ext) {
    case '.bam':
    case '.bw':
    case '.bigwig':
      return 'application/octet-stream';
    case '.vcf':
      return 'text/plain';
    case '.vcf.gz':
      return 'application/gzip';
    case '.html':
      return 'text/html';
    case '.css':
      return 'text/css';
    case '.js':
      return 'application/javascript';
    case '.json':
      return 'application/json';
    case '.png':
      return 'image/png';
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg';
    case '.gif':
      return 'image/gif';
    case '.svg':
      return 'image/svg+xml';
    case '.pdf':
      return 'application/pdf';
    default:
      return 'application/octet-stream';
  }
};

/**
 * GET /files/expose/* - Expose files to external applications
 *
 * This endpoint serves files to external applications (genome browsers, etc.)
 * with proper token-based authorization. It's decoupled from business logic
 * and only handles secure file serving based on token scopes.
 *
 * Security:
 * - Validates JWT token with appropriate scope
 * - Ensures requested path matches token scope exactly
 * - Prevents path traversal attacks
 * - Files must be under DATA_ROOT
 */
router.get(
  '/expose/*',
  validate([]), // No specific params from URL, path is dynamic
  asyncHandler(async (req, res, next) => {
    // 1. Extract relative path from URL
    const relativePathFromUrl = req.params[0] || ''; // everything after /files/expose/
    logger.info('relativePathFromUrl');

    const cleanedRelativePath = (`/${relativePathFromUrl}`).replace(/\/+/, '/');
    logger.info('cleanedRelativePath');

    // 2. Extract scope from token
    const rawScopeString = req.token && req.token.scope ? req.token.scope : '';
    logger.info('rawScopeString');

    const scopes = rawScopeString.split(/\s+/).filter(Boolean);
    logger.info('scopes');

    const genomeBrowserScopes = scopes.filter((s) => s.startsWith(SCOPE_PREFIX));
    logger.info('genomeBrowserScopes');

    if (!genomeBrowserScopes.length) {
      logger.error('No file exposure scope present');
      return next(createError(403, 'No file exposure scope present'));
    }

    // For now we support a single-scope-per-token model
    const scopePath = genomeBrowserScopes[0].slice(SCOPE_PREFIX.length); // removes prefix
    logger.info('scopePath');

    const cleanedScopePath = scopePath.replace(/\/+/, '/');
    logger.info('cleanedScopePath');

    // 3. Compare scope path with request path (must match exactly)
    if (cleanedScopePath !== cleanedRelativePath) {
      logger.error('Scope does not match requested path');
      return next(createError(403, 'Scope does not match requested path'));
    }

    // 4. Build full file path and enforce root containment
    const fullPath = path.join(DATA_ROOT, cleanedRelativePath.replace(/^\/+/, ''));
    logger.info('fullPath');

    const resolvedRoot = path.resolve(DATA_ROOT);
    logger.info('resolvedRoot');

    const resolvedFull = path.resolve(fullPath);
    logger.info('resolvedFull');

    if (!resolvedFull.startsWith(resolvedRoot)) {
      logger.info('!resolvedFull.startsWith(resolvedRoot)');
      logger.error('Security violation: file path outside data root', {
        resolvedFull,
        resolvedRoot,
      });
      return next(createError(400, 'Invalid file path'));
    }

    // 5. Check file existence
    try {
      await fs.promises.access(resolvedFull, fs.constants.F_OK);
      logger.info('File exists');
    } catch (err) {
      logger.error(`File not found: ${resolvedFull}`, err);
      return next(createError(404, 'File not found'));
    }

    // 6. Set headers (CORS + generic content-type)
    // res.set('Access-Control-Allow-Origin', '*');
    // res.set('Access-Control-Allow-Headers', 'Range, Authorization, Content-Type');
    res.set('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length');
    // res.set('Access-Control-Allow-Methods', 'GET, OPTIONS'); // Allow OPTIONS for preflight requests
    res.set('Content-Type', getMimeType(resolvedFull));
    res.set('Accept-Ranges', 'bytes'); // Enable Range requests

    logger.info('Content-Type');
    logger.info(getMimeType(resolvedFull));

    // 7. Handle Range requests
    const { range } = req.headers;
    logger.info('range');
    logger.info(range);

    if (range) {
      logger.info('range is present');
      const stats = await fs.promises.stat(resolvedFull);
      logger.info('stats');
      logger.info(stats);

      const fileSize = stats.size;
      logger.info('fileSize');
      logger.info(fileSize);

      const parts = range.replace(/bytes=/, '').split('-');
      logger.info('parts');
      logger.info(parts);

      const start = parseInt(parts[0], 10);
      logger.info('start');
      logger.info(start);

      const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
      logger.info('end');
      logger.info(end);

      const chunksize = (end - start) + 1;
      logger.info('chunksize');
      logger.info(chunksize);

      logger.info('sending 206 response');
      res.status(206); // Partial Content
      res.set('Content-Range', `bytes ${start}-${end}/${fileSize}`);
      res.set('Content-Length', chunksize);

      const fileStream = fs.createReadStream(resolvedFull, { start, end });
      logger.info('fileStream created');

      fileStream.on('error', (err) => {
        logger.info('fileStream error');
        logger.error(`Error streaming file range: ${err.message}`, err);
        if (!res.headersSent) {
          logger.info('next(createError(500, "Error streaming file range"))');
          next(createError(500, 'Error streaming file range'));
        }
      });

      logger.info('piping fileStream to response');
      fileStream.pipe(res);
    } else {
      logger.info('no range request');
      logger.info('streaming full file');

      // 8. Stream full file (dev / docker mode or no Range request)
      if (process.env.NODE_ENV === 'docker' || !config.get('download_server.use_x_accel_redirect')) {
        logger.info('docker mode or no x-accel-redirect');

        const fileStream = fs.createReadStream(resolvedFull);
        logger.info('fileStream created');

        fileStream.on('error', (err) => {
          logger.info('fileStream error');
          logger.error(`Error streaming file: ${err.message}`, err);
          if (!res.headersSent) {
            logger.info('next(createError(500, "Error streaming file"))');
            next(createError(500, 'Error streaming file'));
          }
        });

        logger.info('piping fileStream to response');
        fileStream.pipe(res);
      } else {
        // Production: use X-Accel-Redirect for nginx
        const relativePathForNginx = cleanedRelativePath.replace(/^\/+/, '');
        logger.info('relativePathForNginx');
        logger.info(relativePathForNginx);

        res.set('X-Accel-Redirect', `/data/${relativePathForNginx}`);
        logger.info('X-Accel-Redirect set');
        logger.info(`/data/${relativePathForNginx}`);

        res.set('X-Accel-Buffering', 'no'); // Prevent nginx buffering for large files
        logger.info('X-Accel-Buffering set');
        logger.info('sending empty response');
        res.send('');
      }
    }
  }),
);

// Handle OPTIONS requests for CORS preflight
router.options('/expose/*', (req, res) => {
  logger.info('OPTIONS request');
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Headers', 'Range, Authorization, Content-Type');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  logger.info('sending 200 response');
  res.status(200).end();
});

module.exports = router;
