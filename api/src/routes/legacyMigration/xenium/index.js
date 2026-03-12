/**
 * Xenium Legacy Migration Sub-Router
 *
 * Mounts Xenium-specific migration status routes.
 * All routes are accessible under: /legacy/migrations/xenium/
 */

const express = require('express');

const router = express.Router();

router.use('/', require('./migrations'));

module.exports = router;
