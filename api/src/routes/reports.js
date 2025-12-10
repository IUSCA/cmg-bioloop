const express = require('express');
const path = require('path');
const fs = require('fs');
const { param } = require('express-validator');
const createError = require('http-errors');

const config = require('config');
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
    // Extract the file path from the URL (everything after /files)
    // req.path will be like '/conversions/123/files/html/index.html'
    const match = req.path.match(/\/conversions\/\d+\/files(.*)/);
    const filePath = match && match[1] ? match[1].replace(/^\//, '') : '';

    // Look up the conversion and dataset to get the dataset name
    const conversion = await prisma.conversion.findUnique({
      where: { id: conversionId },
      include: {
        dataset: true,
      },
    });

    if (!conversion) {
      return next(createError.NotFound('Conversion not found'));
    }

    if (!conversion.dataset) {
      return next(createError.NotFound('Dataset not found for conversion'));
    }

    const datasetName = conversion.dataset.name;
    const reportsDirectory = config.get('conversion.reports_directory');
    const reportsPath = path.join(
      reportsDirectory,
      String(conversionId),
      datasetName,
      'Reports',
      filePath,
    );

    // Security: Ensure the requested path is within the reports directory
    const resolvedPath = path.resolve(reportsPath);
    const resolvedReportsDir = path.resolve(
      path.join(reportsDirectory, String(conversionId), datasetName, 'Reports'),
    );

    if (!resolvedPath.startsWith(resolvedReportsDir)) {
      return next(createError.Forbidden('Invalid path'));
    }

    // Check if file exists
    try {
      await fs.promises.access(resolvedPath, fs.constants.F_OK);
    } catch (err) {
      return next(createError.NotFound('File not found'));
    }

    // Get file stats to determine if it's a directory or file
    const stats = await fs.promises.stat(resolvedPath);

    if (stats.isDirectory()) {
      // If it's a directory, list its contents
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

    // If it's a file, send it with appropriate headers
    // Set content type based on file extension
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
    };

    if (contentTypes[ext]) {
      res.setHeader('Content-Type', contentTypes[ext]);
    }

    return res.sendFile(resolvedPath);
  }),
);

module.exports = router;
