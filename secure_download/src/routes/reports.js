const express = require('express');
const createError = require('http-errors');
const config = require('config');
const { param } = require('express-validator');
const fs = require('fs');
const path = require('path');

const { validate } = require('../middleware/validators');
const asyncHandler = require('../middleware/asyncHandler');
const logger = require('../services/logger');

const router = express.Router();

function remove_leading_slash(str) {
  return str?.replace(/^\/+/, '');
}

// Serve conversion report files
// Pattern: /reports/conversions/{conversion_id}/{dataset_id}/Reports/*
router.get(
  '/:path*',
  validate([
    param('path').notEmpty(),
  ]),
  asyncHandler(async (req, res, next) => {
    logger.info('[REPORTS] Request received');
    
    // TODO: Use separate 'view_reports:' scope instead of reusing download_file
    // For now, reusing download_file scope for validation
    const SCOPE_PREFIX = config.get('scope_prefix'); // 'download_file:'
    
    const scopes = (req.token?.scope || '').split(' ');
    const reports_scopes = scopes.filter((scope) => scope.startsWith(SCOPE_PREFIX));
    
    if (reports_scopes.length === 0) {
      return next(createError.Forbidden('Invalid scope'));
    }
    
    // Extract path from token scope
    const token_reports_path = remove_leading_slash(
      reports_scopes[0].slice(SCOPE_PREFIX.length)
    );
    
    // Extract requested path from URL
    const req_path = remove_leading_slash(req.path);
    
    logger.info(`[REPORTS] Token path: ${token_reports_path}`);
    logger.info(`[REPORTS] Request path: ${req_path}`);
    
    // Extract the file path from the URL (everything after Reports/)
    // e.g., /conversions/123/456/Reports/html/index.html
    const match = req_path.match(/^conversions\/[^/]+\/[^/]+\/Reports(.*)/);
    const filePath = match && match[1] ? match[1].replace(/^\//, '') : '';
    
    logger.info(`[REPORTS] File path: ${filePath}`);
    
    // Construct full path
    // In docker: /opt/sca/data/conversions/{conversion_id}/{dataset_id}/Reports
    // In production: same pattern (volume mounted from /N/scratch/cmguser/cmg-bioloop)
    const baseDir = process.env.NODE_ENV === 'docker' 
      ? '/opt/sca/data' 
      : '/N/scratch/cmguser/cmg-bioloop';
    
    const reportsPath = path.join(baseDir, token_reports_path, filePath);
    
    logger.info(`[REPORTS] Full path: ${reportsPath}`);
    
    // Security: Ensure path is within allowed reports directory
    const resolvedPath = path.resolve(reportsPath);
    const resolvedReportsDir = path.resolve(path.join(baseDir, token_reports_path));
    
    if (!resolvedPath.startsWith(resolvedReportsDir)) {
      logger.error(`[REPORTS] Path traversal attempt: ${resolvedPath}`);
      return next(createError.Forbidden('Invalid path'));
    }
    
    // Check if file exists
    try {
      await fs.promises.access(resolvedPath, fs.constants.F_OK);
    } catch (err) {
      logger.error(`[REPORTS] File not found: ${resolvedPath}`);
      return next(createError.NotFound('File not found'));
    }
    
    // Get file stats
    const stats = await fs.promises.stat(resolvedPath);
    
    if (stats.isDirectory()) {
      // List directory contents
      logger.info(`[REPORTS] Listing directory: ${resolvedPath}`);
      const files = await fs.promises.readdir(resolvedPath, { withFileTypes: true });
      const fileList = files.map((file) => {
        const relativePath = filePath ? `${filePath}/${file.name}` : file.name;
        return {
          name: file.name,
          isDirectory: file.isDirectory(),
          path: relativePath.replace(/\\/g, '/'),
        };
      });
      return res.json(fileList);
    }
    
    // Serve file with appropriate content type
    const ext = path.extname(resolvedPath).toLowerCase();
    const contentTypes = {
      '.html': 'text/html',
      '.css': 'text/css',
      '.js': 'application/javascript',
      '.json': 'application/json',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.gif': 'image/gif',
      '.svg': 'image/svg+xml',
      '.pdf': 'application/pdf',
      '.txt': 'text/plain',
    };
    
    if (contentTypes[ext]) {
      res.setHeader('Content-Type', contentTypes[ext]);
    }
    
    logger.info(`[REPORTS] Serving file: ${resolvedPath}`);
    return res.sendFile(resolvedPath);
  })
);

module.exports = router;
