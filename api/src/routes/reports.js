const express = require('express');

const router = express.Router();

// Mount conversion reports routes
router.use('/conversions', require('./reports/conversions'));

module.exports = router;
