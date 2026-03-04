const express = require('express');
const createError = require('http-errors');
const config = require('config');
const path = require('path');
const fs = require('fs');

const asyncHandler = require('../middleware/asyncHandler');
const logger = require('../services/logger');

const router = express.Router();

function remove_leading_slash(str) {
  return str?.replace(/^\/+/, '');
}

/**
 * Get a stream of the file at the given path
 * @param {string} filePath - The path to the file to stream
 * @returns {fs.ReadStream} - A stream of the file
 */
function getFileStream(filePath) {
  const _filePath = path.join(config.get('download_path'), filePath);
  if (!fs.existsSync(_filePath)) {
    throw createError.NotFound('File not found');
  }
  const stream = fs.createReadStream(_filePath);
  stream.on('error', (err) => {
    console.error('Error streaming file:', err);
    throw createError.InternalServerError('Error downloading file');
  });
  return stream;
}

// The wildcard route accepts a file path with any number of segments, e.g.:
// /download/<stage_alias>/<filename> (when nginx decodes %2F before proxying)
// /download/<stage_alias>%2F<filename> (when nginx preserves %2F)
// Both are handled because the logic below reads req.path and calls decodeURIComponent.
router.get(
  '/*',
  asyncHandler(async (req, res, next) => {
    if (!req.path || req.path === '/') {
      return next(createError.BadRequest('File path is required'));
    }
    logger.info('inside /download/:bundle_name');
    const SCOPE_PREFIX = config.get('scope_prefix');

    const scopes = (req.token?.scope || '').split(' ');
    const download_scopes = scopes.filter((scope) => scope.startsWith(SCOPE_PREFIX));

    // Redact sensitive fields from token before logging
    const sanitizedToken = req.token ? {
      ...req.token,
      client_id: req.token.client_id ? '[REDACTED]' : undefined,
    } : null;

    logger.info('req.token');
    logger.info(JSON.stringify(sanitizedToken, null, 2));

    logger.info('SCOPE_PREFIX');
    logger.info(SCOPE_PREFIX);
    logger.info('req.token?.scope');
    logger.info(req.token?.scope);
    logger.info('scopes');
    logger.info(JSON.stringify(scopes, null, 2));
    logger.info('download_scopes');
    logger.info(JSON.stringify(download_scopes, null, 2));

    if (download_scopes.length === 0) {
      return next(createError.Forbidden('Invalid scope'));
    }

    const token_file_path = remove_leading_slash(download_scopes[0].slice(SCOPE_PREFIX.length));
    const req_path = remove_leading_slash(decodeURIComponent(req.path));

    if (req_path === token_file_path) {
      logger.info('req_path === token_file_path');
      logger.info(`req_path: ${req_path}`);
      logger.info(`token_file_path: ${token_file_path}`);

      // make browser download response instead of attempting to render it
      res.set('content-type', 'application/octet-stream; charset=utf-8');

      if (config.get('mode') !== 'production') {
        // In docker/dev/local/test mode, directly stream the file using Express
        const stream = getFileStream(token_file_path);
        stream.pipe(res);
      } else {
        // In production mode, use nginx X-Accel-Redirect
        res.set('X-Accel-Redirect', `/data/${token_file_path}`);

        // makes nginx not cache the response file
        // otherwise the response cuts off at 1GB as the max buffer size is reached
        // and the file download fails
        // https://stackoverflow.com/a/64282626
        res.set('X-Accel-Buffering', 'no');
        res.send('');
      }
    } else {
      return next(createError.Forbidden('Invalid path'));
    }
  }),
);

module.exports = router;
