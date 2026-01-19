/**
 * Legacy Migration Routes
 * 
 * Routes for querying legacy CMG dataset and session migration status.
 */

const express = require('express');
const router = express.Router();

// Import legacy migration routes
const migrationsRouter = require('./migrations');
const sessionsRouter = require('./sessions');

// Mount sub-routes
router.use('/migrations', migrationsRouter);
router.use('/sessions', sessionsRouter);

module.exports = router;

