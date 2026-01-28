const express = require('express');

const { authenticate } = require('../middleware/auth');
const featureService = require('../services/features');
const uploadRouter = require('./datasets/uploads');
const { fileExposureRouter } = require('./sessions');

const router = express.Router();

router.get('/health', (req, res) => {
  res.send('OK');
});
router.use('/auth', require('./auth/index'));
router.use('/about', require('./about'));
router.use('/env', require('./env'));

// Mount file exposure routes BEFORE global authentication
// These routes use cookie-based authentication instead of Bearer tokens
router.use('/sessions', fileExposureRouter);

// From this point on, all routes require authentication.
router.use(authenticate);

/**
 * Note: The `/datasets/uploads` route needs to be registered before the `/datasets` route.
 * If the `/datasets` route is registered first, Express interprets the path `/datasets/uploads`
 * as a call to the `/datasets/:datasetId` API.
 */
if (featureService.isFeatureEnabled({ key: 'upload' })) {
  router.use('/datasets/uploads', uploadRouter /* #swagger.security = [{"BearerAuth": []}] */);
}

router.use('/datasets', require('./datasets') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/metrics', require('./metrics') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/users', require('./users') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/workflows', require('./workflows') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/projects', require('./projects') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/statistics', require('./statistics') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/notifications', require('./notifications') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/tracks', require('./tracks') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/sessions', require('./sessions') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/instruments', require('./instruments') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/uploads', require('./uploads') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/conversions', require('./conversions') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/process_requests', require('./process_requests') /* #swagger.security = [{"BearerAuth": []}] */);
router.use('/legacy', require('./legacy') /* #swagger.security = [{"BearerAuth": []}] */);

if (featureService.isFeatureEnabled({ key: 'fs' })) {
  router.use('/fs', require('./fs') /* #swagger.security = [{"BearerAuth": []}] */);
}

module.exports = router;
