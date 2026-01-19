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

## Dataset Path Resolution (Production vs Local)

**Service:** `api/src/services/pathResolver.js`

### Environment-Specific Behavior

Path resolution differs significantly between **production** and **local (docker)** environments due to Docker volume mounts.

---

### Production Environment

#### Host Paths (Database Storage)

In production, the database stores **host filesystem paths**:

```
/N/scratch/cmguser/cmg-bioloop/stage/data_products/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
```

**Structure:**
- `/N/scratch` - Base directory (shared research storage)
- `/cmguser` - System user
- `/cmg-bioloop/stage` - Application staging directory
- `/data_products` - Dataset type folder
- `/67940c63007c2f729eae2214de97808a` - Dataset hash
- `/bigWig_h3k4me3_hg19_2` - Dataset folder name

#### Container Paths (Mount Translation)

The API container accesses these files via Docker volume mounts at different paths:

```
/opt/sca/scratch/ingestion_source_dir/cmguser/cmg-bioloop/stage/data_products/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
```

**Docker Compose Mount:**
```yaml
volumes:
  - ${FILESYSTEM_BASE_DIR_SCRATCH}:${FILESYSTEM_MOUNT_DIR_SCRATCH}
  # /N/scratch → /opt/sca/scratch/ingestion_source_dir
```

**Environment Variables** (`.env`):
```bash
FILESYSTEM_BASE_DIR_SCRATCH=/N/scratch
FILESYSTEM_MOUNT_DIR_SCRATCH=/opt/sca/scratch/ingestion_source_dir
```

---

### Local (Docker) Environment

In local development, paths are accessed directly without mount translation:

```
Database:  /opt/sca/data/data_products/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
Container: /opt/sca/data/data_products/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
```

No path conversion needed - the `DATA_ROOT` is used directly.

---

### Path Resolver Service

**Usage:**

```javascript
const pathResolver = require('@/services/pathResolver');

// Get relative path for URL construction
const relativePath = pathResolver.getRelativeFilePath({
  dataset: { staged_path: '/N/scratch/cmguser/...', type: 'DATA_PRODUCT' },
  datasetFile: { path: 'GSM429321.bigWig' }
});

// Resolve to absolute container path
const absolutePath = pathResolver.resolveToAbsolutePath(relativePath);
```

**Key Functions:**

1. **`getFileAccessRoot()`** - Returns base directory for file access
   - Production: `/opt/sca/scratch/ingestion_source_dir/cmguser`
   - Docker: `/opt/sca/data` (from `DATA_ROOT` config)

2. **`resolveHostPathToContainerPath(stagedPath)`** - Converts host path to container path
   - Production: Strips `/N/scratch`, prepends mount directory + system user
   - Docker: Returns path unchanged

3. **`getRelativeFilePath({ dataset, datasetFile })`** - Constructs relative path for URLs
   - Converts host → container path
   - Makes relative to access root
   - In production: Database paths already contain dataset_type, so no insertion needed
   - In docker: May insert dataset_type folder if missing

4. **`resolveToAbsolutePath(relativePath)`** - Converts relative → absolute container path
   - Joins access root + relative path

---

### Path Transformation Examples

#### Production Mode

**Input (Database):**
```
dataset.staged_path: /N/scratch/cmguser/cmg-bioloop/stage/data_products/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
datasetFile.path:    GSM429321.bigWig
```

**Step 1: Host → Container**
```
Remove base_dir:  /cmguser/cmg-bioloop/stage/data_products/.../bigWig_h3k4me3_hg19_2
Remove user:      cmg-bioloop/stage/data_products/.../bigWig_h3k4me3_hg19_2
Add mount dir:    /opt/sca/scratch/ingestion_source_dir/cmguser/cmg-bioloop/stage/data_products/.../bigWig_h3k4me3_hg19_2
```

**Step 2: Make Relative**
```
Access root:      /opt/sca/scratch/ingestion_source_dir/cmguser
Relative path:    cmg-bioloop/stage/data_products/.../bigWig_h3k4me3_hg19_2
```

**Step 3: Add File Path**
```
Final relative:   cmg-bioloop/stage/data_products/.../bigWig_h3k4me3_hg19_2/GSM429321.bigWig
```

**Step 4: Resolve to Absolute**
```
Final absolute:   /opt/sca/scratch/ingestion_source_dir/cmguser/cmg-bioloop/stage/data_products/.../bigWig_h3k4me3_hg19_2/GSM429321.bigWig
```

#### Docker Mode

**Input (Database):**
```
dataset.staged_path: /opt/sca/data/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
datasetFile.path:    GSM429321.bigWig
```

**Processing:**
```
Access root:      /opt/sca/data
Relative path:    67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
Insert type:      data_products/67940c63007c2f729eae2214de97808a/bigWig_h3k4me3_hg19_2
Add file:         data_products/.../bigWig_h3k4me3_hg19_2/GSM429321.bigWig
Final absolute:   /opt/sca/data/data_products/.../bigWig_h3k4me3_hg19_2/GSM429321.bigWig
```

---

### Configuration Requirements

**Config Files** (`api/config/default.json`):
```json
{
  "mode": "production",  // or "docker"
  "data_root": "/opt/sca/data",
  "system_user": {
    "username": "cmguser"
  },
  "filesystem": {
    "base_dir": {
      "slateScratch": "/N/scratch"
    },
    "mount_dir": {
      "slateScratch": "/opt/sca/scratch/ingestion_source_dir"
    }
  }
}
```

**Environment Variable Mapping** (`api/config/custom-environment-variables.json`):
```json
{
  "data_root": "DATA_ROOT",
  "filesystem": {
    "base_dir": {
      "slateScratch": "FILESYSTEM_BASE_DIR_SCRATCH"
    },
    "mount_dir": {
      "slateScratch": "FILESYSTEM_MOUNT_DIR_SCRATCH"
    }
  }
}
```

---

### Important Notes

1. **Production database paths are always host paths** (e.g., `/N/scratch/...`)
2. **Container must convert these to mount paths** for file access
3. **Dataset type folder** (`data_products`, `raw_data`) is already in production paths
4. **Local development** doesn't need path conversion - uses `DATA_ROOT` directly
5. **Always use pathResolver service** - never construct paths manually
6. **Mode detection** is automatic via `config.get('mode')`

---

### File Exposure URL Construction

```javascript
const pathResolver = require('@/services/pathResolver');

// In datahub endpoint (for browser track URLs)
function serializeTrackForIGV(sessionTrack, sessionId, filesByDataset) {
  const { track } = sessionTrack;
  const { dataset_file: datasetFile } = track;
  const { dataset } = datasetFile;
  
  // Get relative path (environment-aware)
  const relativePath = pathResolver.getRelativeFilePath({ dataset, datasetFile });
  
  // Build URL for browser
  const url = buildFileExposureUrl(sessionId, relativePath);
  
  return {
    type: 'wig',
    format: 'bigwig',
    name: track.name,
    url,  // e.g., /api/sessions/383/files/expose/cmg-bioloop/stage/data_products/.../file.bw
  };
}

// In file exposure endpoint
fileExposureRouter.get('/:id/files/expose/*', asyncHandler(async (req, res) => {
  const requestedPath = req.params[0];
  
  // Find matching track
  const matchedTrack = session.session_tracks.find((st) => {
    const relativePath = pathResolver.getRelativeFilePath({
      dataset: st.track.dataset_file.dataset,
      datasetFile: st.track.dataset_file,
    });
    return relativePath === requestedPath;
  });
  
  // Resolve to absolute container path
  const absolutePath = pathResolver.resolveToAbsolutePath(requestedPath);
  
  // Serve file
  const readStream = fs.createReadStream(absolutePath);
  readStream.pipe(res);
}));
```

---

**Last Updated:** 2026-01-17

