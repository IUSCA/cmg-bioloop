/**
 * Legacy Migration Routes - Session Migration Status
 * 
 * Routes for querying session migration status.
 * Logic is currently placeholder and can be extended as needed.
 */

const express = require('express');
const router = express.Router();
const auth = require('@/middleware/auth');
const legacyMigrationService = require('@/services/legacyMigration');

/**
 * GET /legacy/sessions/:id
 * 
 * Get migration status for a specific genome browser session.
 * 
 * Authentication: Required
 * 
 * Response:
 * {
 *   is_legacy: boolean,
 *   is_migrated: boolean
 * }
 * 
 * Note: This is a placeholder implementation. Extend as needed
 * for session-specific migration tracking.
 */
router.get('/:id', auth.authenticate, async (req, res) => {
  try {
    const sessionId = parseInt(req.params.id, 10);
    
    if (isNaN(sessionId)) {
      return res.status(400).json({ 
        error: 'Invalid session ID',
        message: 'Session ID must be a number'
      });
    }
    
    const migrationStatus = await legacyMigrationService.getSessionMigrationStatus(sessionId);
    
    res.json(migrationStatus);
  } catch (error) {
    console.error('Error fetching session migration status:', error);
    
    if (error.message && error.message.includes('not found')) {
      return res.status(404).json({
        error: 'Session not found',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to fetch session migration status'
    });
  }
});

module.exports = router;

