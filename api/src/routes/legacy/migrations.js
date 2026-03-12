/**
 * Legacy Migration Routes — Namespace + Backward Compatibility
 *
 * Mounts migration status routes for all legacy sources and preserves the
 * original /legacy/migrations/datasets/* and /legacy/migrations/sessions/* paths.
 *
 * Route namespaces (new):
 *   /legacy/migrations/cmg/*    → CMG (MongoDB → PostgreSQL) routes
 *   /legacy/migrations/xenium/* → Xenium (PostgreSQL → PostgreSQL) routes
 *
 * Backward-compatible paths (old, CMG-only, still active):
 *   /legacy/migrations/datasets/by-cmg-id/:cmgId
 *   /legacy/migrations/datasets/:id
 *   /legacy/migrations/sessions/:id
 */

const express = require('express');

const router = express.Router();

// Namespaced routes for source-specific consumers
router.use('/cmg', require('@/routes/legacyMigration/cmg'));
router.use('/xenium', require('@/routes/legacyMigration/xenium'));

// Root-level CMG routes preserved for backward compatibility.
// These serve the original /legacy/migrations/datasets/* and
// /legacy/migrations/sessions/* paths used by existing UI code.
router.use('/', require('@/routes/legacyMigration/cmg'));

module.exports = router;
