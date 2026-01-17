# CMG-Bioloop API Customizations

**Extends:** `/bioloop/api_conventions.md`

This file documents API patterns **specific to CMG customizations**, particularly genome browser file serving.

---

## Router Mounting Order for File Exposure

**CRITICAL:** File exposure routers must be mounted **before** global authentication middleware:

```javascript
// api/src/routes/index.js

// ✅ CORRECT ORDER
const fileExposureRouter = require('./sessions').fileExposureRouter;

// 1. Mount file exposure (has its own auth)
app.use('/sessions', fileExposureRouter);

// 2. Global authentication middleware
app.use(authenticate);

// 3. Other protected routes
app.use('/sessions', sessionsRouter);
app.use('/tracks', tracksRouter);
```

**Reason:** Genome browsers make `GET` requests with cookies; global middleware would trigger CORS preflight for routes that need cookie auth.

---

## Cookie-Based File Authentication

For genome browser file serving:

```javascript
// 1. Set cookie
router.post('/:id/set-file-cookie', async (req, res) => {
  const token = jwt.sign(
    { user_id: req.user.id, session_id: req.params.id },
    JWT_SECRET,
    { expiresIn: '1h' }
  );
  
  res.cookie('bioloop_session_token', token, {
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
    maxAge: 3600000,
  });
  
  res.json({ success: true });
});

// 2. Authenticate with cookie
const authenticateWithCookie = asyncHandler(async (req, res, next) => {
  const token = req.cookies.bioloop_session_token;
  if (!token) throw createError(401, 'Token not found');
  
  const decoded = jwt.verify(token, JWT_SECRET);
  
  if (decoded.session_id !== parseInt(req.params.id)) {
    throw createError(403, 'Token does not match session');
  }
  
  req.user = await prisma.user.findUnique({
    where: { id: decoded.user_id }
  });
  
  next();
});
```

---

## Disable Compression for Binary Files

**CRITICAL for genome browser files:**

```javascript
// In app.js
app.use(compression({
  filter: (req, res) => {
    if (req.path && req.path.includes('/files/expose')) {
      return false; // Disable compression
    }
    return compression.filter(req, res);
  },
}));

// In file exposure route
router.get('/:id/files/expose/*', asyncHandler(async (req, res) => {
  // Explicitly disable compression
  res.locals.compress = false;
  res.set('Content-Encoding', 'identity');
  res.removeHeader('Vary');
  
  // Serve file with range support
  const stat = await fs.promises.stat(absolutePath);
  res.set('Content-Length', stat.size);
  res.set('Accept-Ranges', 'bytes');
  
  const readStream = fs.createReadStream(absolutePath);
  readStream.pipe(res);
}));
```

---

## Range Request Support

```javascript
const range = req.headers.range;
if (range) {
  const parts = range.replace(/bytes=/, '').split('-');
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
  const chunkSize = end - start + 1;

  res.status(206); // Partial Content
  res.set('Content-Range', `bytes ${start}-${end}/${stat.size}`);
  res.set('Content-Length', chunkSize);

  const readStream = fs.createReadStream(absolutePath, { start, end });
  readStream.pipe(res);
} else {
  // Full file
  const readStream = fs.createReadStream(absolutePath);
  readStream.pipe(res);
}
```

---

## File Path Construction

**Pattern for staged file paths:**

```javascript
function getRelativeFilePath({ dataset, datasetFile }) {
  const stageAlias = dataset.metadata?.stage_alias || '';
  const filePath = datasetFile.path || '';

  const cleanedStageAlias = stageAlias.replace(/^\/+/, '').replace(/\/+$/, '');
  const cleanedFilePath = filePath.replace(/^\/+/, '');

  return cleanedStageAlias ? `${cleanedStageAlias}/${cleanedFilePath}` : cleanedFilePath;
}

function buildFileExposureUrl(sessionId, relativePath) {
  const cleanedPath = relativePath.replace(/^\/+/, '');
  return `/api/sessions/${sessionId}/files/expose/${cleanedPath}`;
}
```

---

**Last Updated:** 2026-01-16

