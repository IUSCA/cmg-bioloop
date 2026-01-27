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
// Pattern: /reports/{absolute_path}/...
// Token scope contains absolute filesystem path (e.g., /opt/sca/data/conversions/{conversion_id}/{dataset_name}/Reports)
router.get(
  '/*',
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
    
    // Extract absolute path from token scope
    // Token contains absolute path like: /opt/sca/data/conversions/{conversion_id}/{dataset_name}/Reports
    const tokenAbsolutePath = reports_scopes[0].slice(SCOPE_PREFIX.length);
    
    // Extract requested path from URL
    // URL will be like: /reports/{absolute_path}/html/index.html
    const req_path = remove_leading_slash(req.path);
    
    logger.info(`[REPORTS] Token absolute path: ${tokenAbsolutePath}`);
    logger.info(`[REPORTS] Request path: ${req_path}`);
    
    // The request path should start with the token's absolute path
    // Extract the file path portion (everything after the token path)
    if (!req_path.startsWith(remove_leading_slash(tokenAbsolutePath))) {
      logger.error(`[REPORTS] Request path does not match token scope`);
      return next(createError.Forbidden('Request path does not match token scope'));
    }
    
    // Extract the file-specific part of the path
    const filePath = req_path.slice(remove_leading_slash(tokenAbsolutePath).length).replace(/^\//, '');
    
    logger.info(`[REPORTS] File path: ${filePath}`);
    
    // Construct full path using the absolute path from token
    const reportsPath = filePath ? path.join(tokenAbsolutePath, filePath) : tokenAbsolutePath;
    
    logger.info(`[REPORTS] Full path: ${reportsPath}`);
    
    // Security: Ensure path is within allowed reports directory
    const resolvedPath = path.resolve(reportsPath);
    const resolvedReportsDir = path.resolve(tokenAbsolutePath);
    
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
