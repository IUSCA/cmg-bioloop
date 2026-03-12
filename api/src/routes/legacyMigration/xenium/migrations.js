/**
 * Xenium Legacy Migration Routes
 *
 * Endpoints for querying the status of datasets migrated from the Xenium
 * PostgreSQL database into cmg-bioloop.
 *
 * Xenium does not have Sessions or Conversions features.
 *
 * All routes are mounted under: /legacy/migrations/xenium/
 */

const express = require('express');

const router = express.Router();
const auth = require('@/middleware/auth');
const prisma = require('@/db');
const xeniumMigrationService = require('@/services/legacyMigration/xenium');

/**
 * GET /legacy/migrations/xenium/datasets/by-xenium-id/:xeniumId
 *
 * Get a dataset by its Xenium PostgreSQL ID.
 */
router.get('/datasets/by-xenium-id/:xeniumId', auth.authenticate, async (req, res) => {
  try {
    const xeniumId = parseInt(req.params.xeniumId, 10);

    if (Number.isNaN(xeniumId)) {
      return res.status(400).json({
        error: 'Invalid Xenium ID',
        message: 'Xenium ID must be a number',
      });
    }

    const dataset = await prisma.dataset.findFirst({
      where: { xenium_id: xeniumId },
    });

    if (!dataset) {
      return res.status(404).json({
        error: 'Dataset not found',
        message: `No dataset found with Xenium ID: ${xeniumId}`,
      });
    }

    res.json(dataset);
  } catch (error) {
    console.error('Error fetching dataset by Xenium ID:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to fetch dataset by Xenium ID',
    });
  }
});

/**
 * GET /legacy/migrations/xenium/datasets/:id
 *
 * Get migration status for a Xenium-migrated dataset.
 *
 * Response: { is_legacy_xenium, is_migration_initiated, is_retrieved, is_inspected,
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

    const migrationStatus = await xeniumMigrationService.getDatasetMigrationStatus(datasetId);
    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching Xenium dataset migration status:', error);

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

module.exports = router;
