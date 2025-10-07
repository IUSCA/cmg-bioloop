const express = require('express');
const { param, query } = require('express-validator');
const createError = require('http-errors');
const { PrismaClient } = require('@prisma/client');

const { validate } = require('../middleware/validators');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl } = require('../middleware/auth');

const prisma = new PrismaClient();
const isPermittedTo = accessControl('process_request');
const router = express.Router();

// GET /api/process-requests/:id/artifacts
// Get artifacts for a process request
router.get(
  '/:id/artifacts',
  isPermittedTo('read'),
  validate([
    param('id').isInt({ min: 1 }).toInt(),
    query('artifact_type').optional().trim(),
  ]),
  asyncHandler(async (req, res) => {
    const { id } = req.params;
    const { artifact_type } = req.query;

    // Verify process request exists
    const processRequest = await prisma.process_request.findUnique({
      where: { id },
    });

    if (!processRequest) {
      throw createError(404, `Process request ${id} not found`);
    }

    // Build where clause
    const whereClause = { process_id: id };
    if (artifact_type) {
      whereClause.artifact_type = artifact_type;
    }

    // Fetch artifacts
    const artifacts = await prisma.process_artifact.findMany({
      where: whereClause,
      orderBy: { created_at: 'asc' },
    });

    res.json(artifacts);
  }),
);

// GET /api/process-requests/:id
// Get a single process request with its artifacts
router.get(
  '/:id',
  isPermittedTo('read'),
  validate([
    param('id').isInt({ min: 1 }).toInt(),
  ]),
  asyncHandler(async (req, res) => {
    const { id } = req.params;

    const processRequest = await prisma.process_request.findUnique({
      where: { id },
      include: {
        artifacts: {
          orderBy: { created_at: 'asc' },
        },
      },
    });

    if (!processRequest) {
      throw createError(404, `Process request ${id} not found`);
    }

    res.json(processRequest);
  }),
);

module.exports = router;
