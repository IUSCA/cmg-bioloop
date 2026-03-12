/**
 * CMG Legacy Migration Sub-Router
 *
 * Mounts CMG-specific migration status routes.
 * All routes are accessible under: /legacy/migrations/cmg/
 */

const express = require('express');

const router = express.Router();

router.use('/', require('./migrations'));

module.exports = router;
