/* eslint-disable no-console */
/**
 * Minimal TUS server test - no handlers, no middleware
 */

const express = require('express');
const { Server } = require('@tus/server');
const { FileStore } = require('@tus/file-store');
const config = require('config');

const app = express();

// Create minimal TUS server
const tusServer = new Server({
  path: '/',
  maxSize: 100 * 1024 * 1024 * 1024,
  datastore: new FileStore({
    directory: config.get('upload.path'),
  }),
});

// Mount TUS directly
app.use('/', (req, res) => {
  console.log(`Request: ${req.method} ${req.url}`);
  tusServer.handle(req, res).catch((err) => {
    console.error('TUS error:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    }
  });
});

const port = 4000;
app.listen(port, () => {
  console.log(`Minimal TUS server listening on port ${port}`);
  console.log(`Test with: curl -X POST http://localhost:${port} -H "Tus-Resumable: 1.0.0" -H "Upload-Length: 100"`);
});
