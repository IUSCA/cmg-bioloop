/**
 * Legacy Migration Routes
 *
 * Routes for querying legacy CMG dataset and session migration status.
 *
 * All routes are mounted under /legacy/migrations/
 */

const express = require('express');

const router = express.Router();

// Import legacy migration routes
const migrationsRouter = require('./migrations');

// Mount sub-routes
router.use('/migrations', migrationsRouter);

module.exports = router;
