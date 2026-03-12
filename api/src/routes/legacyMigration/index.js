/**
 * Legacy Migration Router
 *
 * Aggregates migration status routes for all legacy data sources.
 * Mounted under /legacy/migrations/ in the main router.
 *
 *   /legacy/migrations/cmg/     → CMG (MongoDB → PostgreSQL) migration routes
 *   /legacy/migrations/xenium/  → Xenium (PostgreSQL → PostgreSQL) migration routes
 *
 * Backward compatibility:
 *   The old /legacy/migrations/datasets/* and /legacy/migrations/sessions/* routes
 *   (CMG-only) are preserved and continue to work via the cmg sub-router.
 */

const express = require('express');

const router = express.Router();

router.use('/cmg', require('./cmg'));
router.use('/xenium', require('./xenium'));

module.exports = router;
