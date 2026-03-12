/**
 * CMG Legacy Migration Routes
 *
 * Endpoints for querying the status of datasets, sessions, and conversions
 * that were migrated from the legacy CMG MongoDB database into Bioloop.
 *
 * All routes are mounted under: /legacy/migrations/cmg/
 */

const express = require('express');

const router = express.Router();
const auth = require('@/middleware/auth');
const prisma = require('@/db');
const cmgMigrationService = require('@/services/legacyMigration/cmg');

/**
 * GET /legacy/migrations/cmg/datasets/by-cmg-id/:cmgId
 *
 * Get a dataset by its CMG MongoDB ObjectId.
 */
router.get('/datasets/by-cmg-id/:cmgId', auth.authenticate, async (req, res) => {
  try {
    const { cmgId } = req.params;

    const dataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgId },
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
 * GET /legacy/migrations/cmg/datasets/:id
 *
 * Get migration status for a CMG-migrated dataset.
 *
 * Response: { is_legacy, is_migration_initiated, is_retrieved, is_inspected,
 *             is_metadata_populated, is_hydrated, is_validated, is_migrated }
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

    const migrationStatus = await cmgMigrationService.getDatasetMigrationStatus(datasetId);
    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching CMG dataset migration status:', error);

    if (error.message?.includes('not found')) {
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
 * GET /legacy/migrations/cmg/sessions/:id
 *
 * Get migration status for a CMG-migrated genome browser session.
 *
 * Response: { is_legacy, is_hydrated }
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

    const migrationStatus = await cmgMigrationService.getSessionMigrationStatus(sessionId);
    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching CMG session migration status:', error);

    if (error.message?.includes('not found')) {
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
