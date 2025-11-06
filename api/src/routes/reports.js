const express = require('express');

const router = express.Router();

const options = {
  dotfiles: 'ignore',
  etag: true,
  index: ['index.html'],
  lastModified: false,
  maxAge: '1d',
  redirect: true,
};

router.use(express.static('reports', options));

module.exports = router;
