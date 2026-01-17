# Genome Browser Implementation Notes

## Generic Terminology in Shared Code

**Use generic naming in shared code:**

```javascript
// ✅ CORRECT - Generic naming
const showGenomeBrowserModal = ref(false);
const selectedBrowserType = ref(null);
const genomeBrowserTracks = ref([]);

const handleBrowserSelection = (browserType) => {
  if (browserType === BROWSER_TYPES.IGV) {
    initializeIGV();
  } else if (browserType === BROWSER_TYPES.WASHU) {
    initializeWashU();
  }
};

// ❌ WRONG - Browser-specific naming in shared code
const showIGVModal = ref(false);
const showWashUModal = ref(false);
```

**Browser-specific code is OK within initialization functions:**

```javascript
// ✅ OK - Inside browser-specific function
const initializeIGV = async () => {
  const igvModule = await import('igv');
  const igvOptions = { /* IGV-specific */ };
  const igvBrowser = await igv.createBrowser(container, igvOptions);
};

const initializeWashU = async () => {
  const washuProps = { /* WashU-specific */ };
  // ...
};
```

---

## File Serving: Disable Compression for Binary Files

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

## Cookie-Based File Access

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

## React-in-Vue Integration

When embedding React components (e.g., WashU browser) in Vue:

**Key Points:**
- Use `createRoot()` from `react-dom/client` (React 18+)
- Always unmount in `onBeforeUnmount()`
- Stop event propagation with `@click.stop` on container to prevent React-Vue conflicts
- Use `:disable-attachment="true"` on parent `va-modal` if inside a modal
- Convert relative URLs to absolute for Web Workers
- Force remounts with dynamic `:key` when props change

See `.ai/CONVENTIONS_UI.md` for full code example.

---

**Last Updated:** 2026-01-16

