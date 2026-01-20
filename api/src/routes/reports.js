const express = require('express');
const path = require('path');
const fs = require('fs');
const { param } = require('express-validator');
const createError = require('http-errors');

const prisma = require('@/db');
const { validate } = require('../middleware/validators');
const asyncHandler = require('../middleware/asyncHandler');

const router = express.Router();

// Serve static files for conversion reports
// Handles both /api/reports/conversions/:id/files and nested paths like
// /api/reports/conversions/:id/files/html/index.html
router.get(
  '/conversions/:id/files*',
  validate([
    param('id').isInt({ min: 1 }).toInt(),
  ]),
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['Reports']
    // #swagger.summary = Serve conversion report files

    const conversionId = req.params.id;
    console.log(`[REPORTS] Request received for conversion ${conversionId}`);
    console.log(`[REPORTS] Full URL path: ${req.path}`);
    
    // Extract the file path from the URL (everything after /files)
    // req.path will be like '/conversions/123/files/html/index.html'
    const match = req.path.match(/\/conversions\/\d+\/files(.*)/);
    const filePath = match && match[1] ? match[1].replace(/^\//, '') : '';
    console.log(`[REPORTS] Extracted file path: "${filePath}"`);

    // Look up the conversion and dataset to get the dataset name
    console.log(`[REPORTS] Querying database for conversion ${conversionId}`);
    const conversion = await prisma.conversion.findUnique({
      where: { id: conversionId },
      include: {
        dataset: true,
      },
    });

    if (!conversion) {
      console.error(`[REPORTS] Conversion ${conversionId} not found in database`);
      return next(createError.NotFound('Conversion not found'));
    }

    console.log(`[REPORTS] Found conversion:`, {
      id: conversion.id,
      cmg_id: conversion.cmg_id,
      dataset_id: conversion.dataset_id,
    });

    if (!conversion.dataset) {
      console.error(`[REPORTS] Dataset not found for conversion ${conversionId}`);
      return next(createError.NotFound('Dataset not found for conversion'));
    }

    const datasetName = conversion.dataset.name;
    console.log(`[REPORTS] Dataset name: ${datasetName}`);

    // Construct the reports path dynamically using CMG logic:
    // Use cmg_id if available (for historic conversions), otherwise use bioloop conversion id
    // Path pattern: /opt/sca/project/ingestion_source_dir/CMG-SCA/production/conversion/{cmg_id}/{dataset_name}/Reports/
    const conversionDirName = conversion.cmg_id || String(conversionId);
    const reportsBaseDir = '/opt/sca/project/ingestion_source_dir/CMG-SCA/production/conversion';
    const reportsPath = path.join(
      reportsBaseDir,
      conversionDirName,
      datasetName,
      'Reports',
      filePath,
    );

    console.log(`[REPORTS] Path construction:`, {
      conversionDirName,
      reportsBaseDir,
      datasetName,
      requestedFile: filePath,
      fullPath: reportsPath,
    });

    // Security: Ensure the requested path is within the reports directory
    const resolvedPath = path.resolve(reportsPath);
    const resolvedReportsDir = path.resolve(
      path.join(reportsBaseDir, conversionDirName, datasetName, 'Reports'),
    );

    console.log(`[REPORTS] Security check:`, {
      resolvedPath,
      resolvedReportsDir,
      isValid: resolvedPath.startsWith(resolvedReportsDir),
    });

    if (!resolvedPath.startsWith(resolvedReportsDir)) {
      console.error(`[REPORTS] Path traversal attempt detected: ${resolvedPath}`);
      return next(createError.Forbidden('Invalid path'));
    }

    // Check if file exists
    console.log(`[REPORTS] Checking file access: ${resolvedPath}`);
    try {
      await fs.promises.access(resolvedPath, fs.constants.F_OK);
      console.log(`[REPORTS] File exists and is accessible`);
    } catch (err) {
      console.error(`[REPORTS] File not found or not accessible:`, {
        path: resolvedPath,
        error: err.message,
        code: err.code,
      });
      return next(createError.NotFound('File not found'));
    }

    // Get file stats to determine if it's a directory or file
    const stats = await fs.promises.stat(resolvedPath);
    console.log(`[REPORTS] File stats:`, {
      isDirectory: stats.isDirectory(),
      isFile: stats.isFile(),
      size: stats.size,
    });

    if (stats.isDirectory()) {
      // If it's a directory, list its contents
      console.log(`[REPORTS] Listing directory contents: ${resolvedPath}`);
      const files = await fs.promises.readdir(resolvedPath, { withFileTypes: true });
      const fileList = files.map((file) => {
        const relativePath = filePath ? `${filePath}/${file.name}` : file.name;
        return {
          name: file.name,
          isDirectory: file.isDirectory(),
          path: relativePath.replace(/\\/g, '/'),
        };
      });
      console.log(`[REPORTS] Directory contains ${fileList.length} items`);
      return res.json(fileList);
    }

    // If it's a file, send it with appropriate headers
    // Set content type based on file extension
    const ext = path.extname(resolvedPath).toLowerCase();
    console.log(`[REPORTS] File extension: ${ext}`);
    
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
    };

    if (contentTypes[ext]) {
      res.setHeader('Content-Type', contentTypes[ext]);
      console.log(`[REPORTS] Content-Type set to: ${contentTypes[ext]}`);
    } else {
      console.log(`[REPORTS] No specific Content-Type for extension ${ext}`);
    }

    console.log(`[REPORTS] Sending file: ${resolvedPath}`);
    return res.sendFile(resolvedPath, (err) => {
      if (err) {
        console.error(`[REPORTS] Error sending file:`, {
          path: resolvedPath,
          error: err.message,
          stack: err.stack,
        });
      } else {
        console.log(`[REPORTS] File sent successfully: ${resolvedPath}`);
      }
    });
  }),
);

module.exports = router;
