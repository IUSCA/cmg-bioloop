/**
 * Legacy Migration Routes
 *
 * Routes for querying legacy CMG dataset and session migration status.
 */

const express = require('express');

const router = express.Router();
const auth = require('@/middleware/auth');
const prisma = require('@/db');
const legacyMigrationService = require('@/services/legacyMigration');

/**
 * GET /legacy/migrations/datasets/by-cmg-id/:cmgId
 *
 * Get a dataset by its CMG ID.
 *
 * Authentication: Required
 *
 * Response: Dataset object or 404 if not found
 */
router.get('/datasets/by-cmg-id/:cmgId', auth.authenticate, async (req, res) => {
  try {
    const { cmgId } = req.params;

    const dataset = await prisma.dataset.findFirst({
      where: {
        cmg_id: cmgId,
      },
    });

    if (!dataset) {
      return res.status(404).json({
        error: 'Dataset not found',
        message: `No dataset found with CMG ID: ${cmgId}`,
      });
    }

    res.json(dataset);
  } catch (error) {
    console.error('Error fetching dataset by CMG ID:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to fetch dataset by CMG ID',
    });
  }
});

/**
 * GET /legacy/migrations/datasets/:id
 *
 * Get migration status for a specific dataset.
 * Returns information about whether the dataset has been hydrated,
 * validated, and fully migrated.
 *
 * Authentication: Required
 *
 * Response:
 * {
 *   is_legacy: boolean,
 *   is_migration_initiated: boolean,
 *   is_retrieved: boolean,
 *   is_inspected: boolean,
 *   is_metadata_populated: boolean,
 *   is_hydrated: boolean,
 *   is_validated: boolean,
 *   is_migrated: boolean
 * }
 */
router.get('/datasets/:id', auth.authenticate, async (req, res) => {
  try {
    const datasetId = parseInt(req.params.id, 10);

    if (Number.isNaN(datasetId)) {
      return res.status(400).json({
        error: 'Invalid dataset ID',
        message: 'Dataset ID must be a number',
      });
    }

    const migrationStatus = await legacyMigrationService.getDatasetMigrationStatus(datasetId);

    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching dataset migration status:', error);

    if (error.message && error.message.includes('not found')) {
      return res.status(404).json({
        error: 'Dataset not found',
        message: error.message,
      });
    }

    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to fetch dataset migration status',
    });
  }
});

/**
 * GET /legacy/migrations/sessions/:id
 *
 * Get migration status for a specific genome browser session.
 *
 * Authentication: Required
 *
 * Response:
 * {
 *   is_legacy: boolean,      // true if session has cmg_id
 *   is_hydrated: boolean     // true if session.metadata.is_hydrated is true
 * }
 */
router.get('/sessions/:id', auth.authenticate, async (req, res) => {
  try {
    const sessionId = parseInt(req.params.id, 10);

    if (Number.isNaN(sessionId)) {
      return res.status(400).json({
        error: 'Invalid session ID',
        message: 'Session ID must be a number',
      });
    }

    const migrationStatus = await legacyMigrationService.getSessionMigrationStatus(sessionId);

    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching session migration status:', error);

    if (error.message && error.message.includes('not found')) {
      return res.status(404).json({
        error: 'Session not found',
        message: error.message,
      });
    }

    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to fetch session migration status',
    });
  }
});

module.exports = router;
