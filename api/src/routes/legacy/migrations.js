/**
 * Legacy Migration Routes - Dataset Migration Status
 * 
 * Routes for querying dataset migration status (hydration, validation, etc.)
 */

const express = require('express');
const router = express.Router();
const auth = require('@/middleware/auth');
const legacyMigrationService = require('@/services/legacyMigration');

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
    
    if (isNaN(datasetId)) {
      return res.status(400).json({ 
        error: 'Invalid dataset ID',
        message: 'Dataset ID must be a number'
      });
    }
    
    const migrationStatus = await legacyMigrationService.getDatasetMigrationStatus(datasetId);
    
    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching dataset migration status:', error);
    
    if (error.message && error.message.includes('not found')) {
      return res.status(404).json({
        error: 'Dataset not found',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to fetch dataset migration status'
    });
  }
});

module.exports = router;

