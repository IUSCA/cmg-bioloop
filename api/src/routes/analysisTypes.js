const express = require('express');
const { body } = require('express-validator');

const prisma = require('@/db');
const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl } = require('@/middleware/auth');
const { validate } = require('@/middleware/validators');

const isPermittedTo = accessControl('datasets');
const router = express.Router();

// Get all analysis types
router.get(
  '/',
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['analysis_types']
    // #swagger.summary = 'Get all analysis types'

    const analysisTypes = await prisma.analysis_type.findMany({
      orderBy: { name: 'asc' },
    });

    res.json(analysisTypes);
  }),
);

// Create new analysis type
router.post(
  '/',
  isPermittedTo('create'),
  validate([
    body('name').notEmpty().trim(),
    body('extension').notEmpty().trim(),
  ]),
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['analysis_types']
    // #swagger.summary = 'Create new analysis type'

    const { name, extension } = req.body;
    const formattedName = name.toUpperCase().replace(/\s+/g, '_').replace(/[^A-Z0-9_]/g, '');

    // Check if analysis type already exists (case-insensitive)
    const existing = await prisma.analysis_type.findFirst({
      where: {
        name: {
          equals: formattedName,
          mode: 'insensitive',
        },
      },
    });

    if (existing) {
      return res.status(409).json({
        error: 'Analysis type already exists',
        existing,
      });
    }

    const analysisType = await prisma.analysis_type.create({
      data: {
        name: formattedName,
        extension,
      },
    });

    res.status(201).json(analysisType);
  }),
);

module.exports = router;
