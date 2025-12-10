Bioloop – Secure File Exposure for Genome Browsers via secure_download
1. Goals

The secure_download microservice must:

Expose arbitrary files under /opt/sca/data over HTTP for WashU / IGV and regular downloads.

Enforce authorization using file-scoped tokens issued by the main Bioloop API.

Remain decoupled from business logic

It should not know about users, projects, datasets, sessions, tracks, etc.

It only knows about:
“This token is allowed to read this path under /opt/sca/data.”

This document specifies:

How the API generates tokens and URLs.

How secure_download validates tokens and file paths.

How secure_download streams files (local dev + prod).

2. High-Level Architecture
2.1 Components

Bioloop API (api/):

Knows business logic (users, roles, projects, datasets, sessions, tracks).

Validates that the requesting user is allowed to see a dataset / session.

Issues short-lived OAuth2/JWT tokens with file-path scopes.

Returns fully-formed URLs (including token) to the client (UI or genome browser).

secure_download service:

Dedicated file server backed by /opt/sca/data.

Validates tokens and their scopes.

Serves files via:

direct streaming (dev / NODE_ENV=docker), or

nginx X-Accel-Redirect (prod).

2.2 Trust model

API and secure_download share trust via:

A JWKS endpoint (e.g. via Signet) used to validate tokens.

A shared scope_prefix (e.g. "download_file:").

Clients (UI / WashU / IGV) are untrusted:

They receive a URL + token.

They must not be able to modify the path to escape access bounds.

3. Token & URL Design
3.1 Token scope format

Scope prefix (in config):

scope_prefix = "download_file:"

Scope value:

download_file:<relative_path>

Where:

<relative_path> is the path inside /opt/sca/data that the token is allowed to read.

Example:

download_file:/staged_data/datasets/42/sample.bam
download_file:/staged_data/datasets/42/sample.bam.bai
download_file:/staged_data/data_products/NA12878/NA12878.GRCh38.genotype.vcf.gz

3.2 File URL format

The URL that the client (browser / WashU / IGV) will use:

https://<secure_download_host>/download/<relative_path>?token=<jwt>


Where:

<relative_path> matches exactly the path used in the token scope.

<jwt> is a short-lived OAuth2 access token whose scope list includes:

download_file:/<same relative path>


Examples:

https://localhost:3060/download/staged_data/datasets/42/sample.bam?token=eyJhbGciOi...
https://localhost:3060/download/staged_data/datasets/42/sample.bam.bai?token=eyJhbGciOi...
https://localhost:3060/download/staged_data/data_products/NA12878/NA12878.GRCh38.genotype.vcf.gz?token=eyJhbGciOi...

4. Token Generation in the API
4.1 Config

api/config/default.json (or similar):

{
  "oauth": {
    "download": {
      "client_id": "secure_download_client",
      "client_secret": "******",
      "scope_prefix": "download_file:"
    }
  },
  "secure_download": {
    "base_url": "https://localhost:3060",
    "download_path_prefix": "/download"
  }
}

4.2 Helper: compute file’s relative path

Goal: Given a dataset_file record, compute the path relative to /opt/sca/data that will be used both in scope and in URL.

Example (Node / TS-ish pseudocode):

function getRelativeFilePathForDownload({ dataset, datasetFile }) {
  // Example assumption:
  // dataset.metadata.stage_alias = "staged_data/datasets/42"
  // datasetFile.path = "sample.bam"
  //
  // Then: "/staged_data/datasets/42/sample.bam"

  const stageAlias = dataset.metadata.stage_alias; // no leading slash
  const filePath = datasetFile.path;              // may or may not have leading slash

  const cleanedStageAlias = stageAlias.replace(/^\/+/, '');
  const cleanedFilePath = filePath.replace(/^\/+/, '');

  return `/${cleanedStageAlias}/${cleanedFilePath}`;
}

4.3 Helper: get file-scoped download token
async function getFileDownloadToken(relativePath) {
  const scopePrefix = config.get('oauth.download.scope_prefix'); // "download_file:"
  const scope = `${scopePrefix}${relativePath}`;                  // e.g. "download_file:/staged_data/..."

  const tokenResponse = await oAuth2SecureTransferClient.clientCredentials({
    scope: [scope],
  });

  return tokenResponse.accessToken; // raw JWT string
}

4.4 Helper: build secure_download URL
function buildSecureDownloadUrl(relativePath, token) {
  const baseUrl = config.get('secure_download.base_url');         // "https://localhost:3060"
  const prefix = config.get('secure_download.download_path_prefix'); // "/download"

  const url = new URL(baseUrl);
  // e.g. "/download/staged_data/datasets/42/sample.bam"
  url.pathname = `${prefix}${relativePath}`;
  url.searchParams.set('token', token);

  return url.toString();
}

4.5 Putting it together in API

Example flow in /sessions/:id/datahub or /datasets/download/:id:

async function buildTrackEntry({ dataset, datasetFile }) {
  const relativePath = getRelativeFilePathForDownload({ dataset, datasetFile });
  const token = await getFileDownloadToken(relativePath);
  const url = buildSecureDownloadUrl(relativePath, token);

  // For BAM+BAI, you may create a second entry for indexURL similarly
  return {
    type: inferTrackTypeFromFile(datasetFile), // "bam", "vcf", "bigwig", etc.
    name: datasetFile.name || dataset.name,
    url,
    // indexURL: ... (if needed)
  };
}


This is what your “datahub” / track JSON will contain and what WashU/IGV will call.

5. Authentication Middleware in secure_download
5.1 Config

secure_download/config/default.json:

{
  "scope_prefix": "download_file:",
  "auth": {
    "jwks_uri": "http://127.0.0.1:5050/oauth/jwks"  // Signet / JWKS endpoint
  },
  "data_root": "/opt/sca/data"
}

5.2 Middleware: extract & validate JWT

File: secure_download/src/middleware/auth.js

Responsibilities:

Extract token from:

req.query.token, or

Authorization: Bearer ... header.

Validate JWT signature & expiration using JWKS.

Attach decoded token to req.token.

Pseudocode:

const jwt = require('jsonwebtoken');
const jwksClient = require('jwks-rsa');
const config = require('config');

const client = jwksClient({
  jwksUri: config.get('auth.jwks_uri'),
});

function getKey(header, callback) {
  client.getSigningKey(header.kid, function(err, key) {
    if (err) return callback(err);
    const signingKey = key.getPublicKey();
    callback(null, signingKey);
  });
}

function extractToken(req) {
  if (req.query && req.query.token) {
    return req.query.token;
  }
  const authHeader = req.headers.authorization || '';
  const parts = authHeader.split(' ');
  if (parts.length === 2 && parts[0] === 'Bearer') {
    return parts[1];
  }
  return null;
}

function authenticate(req, res, next) {
  const token = extractToken(req);
  if (!token) {
    return res.status(401).json({ error: 'Missing token' });
  }

  jwt.verify(token, getKey, {}, (err, decoded) => {
    if (err || !decoded) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    req.token = decoded;
    next();
  });
}

module.exports = authenticate;


This middleware should be applied to any file-exposing route.

6. secure_download – File Exposure Endpoint

We define a generic, path-based endpoint:

GET /download/*

6.1 Mounting in Express

In secure_download main app:

const express = require('express');
const authenticate = require('./middleware/auth');
const downloadRouter = require('./routes/download');

const app = express();

// All /download requests must be authenticated
app.use('/download', authenticate, downloadRouter);

module.exports = app;

6.2 Route handler logic

File: secure_download/src/routes/download.js

Requirements:

Take the part after /download/ as the relative path.

Compare that path with the scopes in req.token.scope.

Ensure:

At least one scope starts with scope_prefix.

The path in the scope equals the requested path.

Ensure:

The resolved file path stays under /opt/sca/data.

Stream file:

Add CORS header for browsers.

Set Content-Type to application/octet-stream (or use MIME detection).

Use fs.createReadStream in dev; optionally X-Accel-Redirect in prod.

Pseudocode:

const express = require('express');
const config = require('config');
const fs = require('fs');
const path = require('path');

const router = express.Router();

const DATA_ROOT = config.get('data_root');       // "/opt/sca/data"
const SCOPE_PREFIX = config.get('scope_prefix'); // "download_file:"

router.get('/*', (req, res) => {
  // 1. Extract relative path from URL
  const relativePathFromUrl = req.params[0] || ''; // everything after /download/
  const cleanedRelativePath = ('/' + relativePathFromUrl).replace(/\/+/, '/');

  // 2. Extract scope from token
  const rawScopeString = req.token && req.token.scope ? req.token.scope : '';
  const scopes = rawScopeString.split(/\s+/).filter(Boolean);
  const downloadScopes = scopes.filter((s) => s.startsWith(SCOPE_PREFIX));

  if (!downloadScopes.length) {
    return res.status(403).json({ error: 'No download scope present' });
  }

  // For now we support a single-scope-per-token model
  const scopePath = downloadScopes[0].slice(SCOPE_PREFIX.length); // removes prefix
  const cleanedScopePath = scopePath.replace(/\/+/, '/');

  // 3. Compare scope path with request path (must match exactly)
  if (cleanedScopePath !== cleanedRelativePath) {
    return res.status(403).json({ error: 'Scope does not match requested path' });
  }

  // 4. Build full file path and enforce root containment
  const fullPath = path.join(DATA_ROOT, cleanedRelativePath.replace(/^\/+/, ''));
  const resolvedRoot = path.resolve(DATA_ROOT);
  const resolvedFull = path.resolve(fullPath);

  if (!resolvedFull.startsWith(resolvedRoot)) {
    return res.status(400).json({ error: 'Invalid file path' });
  }

  // 5. Check file existence
  if (!fs.existsSync(resolvedFull)) {
    return res.status(404).json({ error: 'File not found' });
  }

  // 6. Set headers (CORS + generic content-type)
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Headers', 'Range, Authorization, Content-Type');
  res.set('Content-Type', 'application/octet-stream');

  // 7. Stream file (dev / docker mode)
  const stream = fs.createReadStream(resolvedFull);
  stream.on('error', (err) => {
    console.error('Error streaming file:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Error streaming file' });
    }
  });
  stream.pipe(res);
});

module.exports = router;


Note: This is the dev-friendly version (no Range implementation).
An agent can later add proper Range handling and 206 responses as needed.

7. Example Flows
7.1 WashU / IGV external (hosted) flow

User opens Bioloop UI and selects a session.

UI calls GET /sessions/datahub/:session_id.

API:

Validates the user can view the session’s datasets.

For each dataset_file, computes relativePath.

Calls getFileDownloadToken(relativePath).

Builds URL: https://localhost:3060/download<relativePath>?token=<jwt>.

Returns an array of track objects:

[
  {
    "type": "bam",
    "name": "Sample Alignments",
    "url": "https://localhost:3060/download/staged_data/datasets/42/sample.bam?token=eyJhbGciOi..."
  }
]


UI opens WashU or IGV with a URL or session JSON containing those track URLs.

Browser executes JS from WashU/IGV, which fetches https://localhost:3060/download/....

secure_download validates token → serves file.

7.2 Regular “download this bundle” flow

The same pattern applies, but:

Instead of /download/<relativePath>, you might keep /download/:bundle_name* for bundle semantics.

The logic is the same:

API mints file-specific token with correct scope.

secure_download verifies scope against requested path.

8. Security Invariants

The implementation MUST ensure:

Token scope matches requested path exactly

No partial prefix matches.

No ability to reuse a token for a sibling/parent path.

File path always stays under DATA_ROOT (/opt/sca/data)

Must resolve and check against path traversal (../ etc.).

Token validation uses JWKS / public keys

Never trust unsigned or self-signed tokens.

All file-serving routes go through authenticate middleware

No unprotected file endpoints.

9. Extension Points (for later)

An agent implementing this should keep hooks for:

HTTP Range support:

Proper handling of Range / If-Range for big BAM/VCF.

nginx X-Accel-Redirect (prod):

Instead of streaming from Node, set:

X-Accel-Redirect to /data/<path>

X-Accel-Buffering: no

Audit logging:

Log user_id (embedded in token), file path, timestamp.

Rate limiting:

Per user / per IP on the secure_download side.

This is the spec an AI (or dev) can use to wire up:

API-side token + URL generation, and

secure_download validation + file streaming,

for genome-browser-safe, secure file exposure.