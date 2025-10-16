const express = require('express');
const createError = require('http-errors');
const {
  query, param,
} = require('express-validator');
const _ = require('lodash/fp');
const { Prisma } = require('@prisma/client');

const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl } = require('@/middleware/auth');
const { validate } = require('@/middleware/validators');
const CONSTANTS = require('@/constants');
const prisma = require('@/db');

const isPermittedTo = accessControl('datasets');

const router = express.Router();

// Used by:
//  - UI
//  - Workers
router.get(
  '/',
  validate([
    query('dataset_name').optional().trim().isLength({ min: 1 }),
    query('limit').isInt({ min: 1 }).toInt().optional(),
    query('offset').isInt({ min: 0 }).toInt().optional(),
  ]),
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Retrieve past imports'

    const {
      dataset_name, offset, limit,
    } = req.query;

    const where = {};
    if (dataset_name) {
      where.dataset = {
        name: {
          contains: dataset_name,
          mode: 'insensitive',
        },
      };
    }

    const filter_query = {
      skip: offset ?? Prisma.skip,
      take: limit ?? Prisma.skip,
      where,
      orderBy: {
        created_at: 'desc',
      },
    };

    const [dataset_import_logs, count] = await prisma.$transaction([
      prisma.dataset_import_log.findMany({
        ...filter_query,
        include: {
          dataset: {
            select: {
              id: true,
              name: true,
              type: true,
              origin_path: true,
              audit_logs: {
                where: {
                  create_method: CONSTANTS.DATASET_CREATE_METHODS.IMPORT,
                },
                select: {
                  user: {
                    select: {
                      id: true,
                      name: true,
                      username: true,
                    },
                  },
                  timestamp: true,
                },
                orderBy: {
                  timestamp: 'desc',
                },
                take: 1,
              },
            },
          },
        },
      }),
      prisma.dataset_import_log.count({ where }),
    ]);

    res.json({ metadata: { count }, imports: dataset_import_logs });
  }),
);

// Used by UI
router.get(
  '/:username',
  validate([
    query('dataset_name').optional().trim().isLength({ min: 1 }),
    query('limit').isInt({ min: 1 }).toInt().optional(),
    query('offset').isInt({ min: 0 }).toInt().optional(),
    param('username').trim().notEmpty(),
  ]),
  isPermittedTo('read', { checkOwnership: true }),
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Retrieve past imports for a specific user'

    const {
      dataset_name, offset, limit,
    } = req.query;

    const where = {
      dataset: {
        audit_logs: {
          some: {
            create_method: CONSTANTS.DATASET_CREATE_METHODS.IMPORT,
            user: {
              username: req.params.username,
            },
          },
        },
      },
    };

    if (dataset_name) {
      where.dataset.name = {
        contains: dataset_name,
        mode: 'insensitive',
      };
    }

    const filter_query = {
      skip: offset ?? Prisma.skip,
      take: limit ?? Prisma.skip,
      where,
      orderBy: {
        created_at: 'desc',
      },
    };

    const [dataset_import_logs, count] = await prisma.$transaction([
      prisma.dataset_import_log.findMany({
        ...filter_query,
        include: {
          dataset: {
            select: {
              id: true,
              name: true,
              type: true,
              origin_path: true,
              audit_logs: {
                where: {
                  create_method: CONSTANTS.DATASET_CREATE_METHODS.IMPORT,
                  user: {
                    username: req.params.username,
                  },
                },
                select: {
                  user: {
                    select: {
                      id: true,
                      name: true,
                      username: true,
                    },
                  },
                  timestamp: true,
                },
                orderBy: {
                  timestamp: 'desc',
                },
                take: 1,
              },
            },
          },
        },
      }),
      prisma.dataset_import_log.count({ where }),
    ]);

    res.json({ metadata: { count }, imports: dataset_import_logs });
  }),
);

module.exports = router;
