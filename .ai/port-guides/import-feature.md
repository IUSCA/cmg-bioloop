# Import Feature — Port Guide (cmg-bioloop → bioloop)

**Purpose:** This document gives a precise, comprehensive spec for an agent to carry over all import-feature changes from `cmg-bioloop` to the base `bioloop` platform.

**Scope:** Platform-generic changes only. The following are **CMG-specific** and must **NOT** be ported:
- Genomic Details step and all its fields (`genome_type`, `genome_value`, `import_notes`)
- Analysis Type / File Type field (`analysis_type`, `selectedFileType`, `analysisTypes`)
- Source Data Product field (`willAssignSourceDataProduct`, `selectedSourceDataProduct`, `source_data_product_id` on import form)
- Any reference to `analysis_type` model or `analysisType` service

---

## 1. What Was There Before (Old State)

Before these changes, the import feature used a concept of **filesystem "search spaces"** (e.g., `slateScratch`, `slateProject`) to let users pick which filesystem root to search under. These spaces were hardcoded in UI config (`filesystem_search_spaces`) and API config (`filesystem.base_dir`). Alongside this, a **denylist** of restricted paths (`restricted_import_dirs` / `SCRATCH_IMPORT_RESTRICTED_DIRS`) controlled which paths could not be imported from. Both were env-var driven.

**Problems with that approach:**
- Users had to know what "Slate-Scratch" and "Slate-Project" mean (infrastructure jargon, not meaningful to researchers)
- The denylist is the wrong security model for this use case (default-allow, explicitly deny)
- Adding more entry points required new env vars per entry point
- No way to give paths human-friendly labels

---

## 2. Summary of Changes

| Area | Change |
|---|---|
| Database | New `import_source` table |
| API route (new) | `GET /datasets/imports/sources` — lists configured import sources |
| API route (refactored) | `GET /fs` — path-based allowlist using import sources from DB |
| API route (updated) | `POST /datasets` — allowlist check on `origin_path` using import sources |
| API ACL | `import_sources` resource added to all roles |
| API config | Remove `filesystem.search_spaces`, `restricted_import_dirs` keys |
| API env | Remove `FILESYSTEM_SEARCH_SPACES`, `SCRATCH_IMPORT_RESTRICTED_DIRS` |
| API service | Remove `import_space` from import log metadata |
| UI service (new) | `ui/src/services/import.js` |
| UI service (updated) | `ui/src/services/fs.js` — remove `search_space` param |
| UI component (updated) | `ImportStepper.vue` — load import sources from API; remove hardcoded spaces |
| UI config | Remove `filesystem_search_spaces`, `restricted_import_dirs` from `config.js` |
| UI env | Remove `VITE_SCRATCH_BASE_DIR`, `VITE_SCRATCH_MOUNT_DIR`, `VITE_PROJECT_BASE_DIR`, `VITE_PROJECT_MOUNT_DIR`, `VITE_SCRATCH_IMPORT_RESTRICTED_DIRS`, `VITE_FILESYSTEM_SEARCH_SPACES` |
| Seed | Add import source seeding |
| Init script (new) | `api/src/scripts/init_prod_import_sources.js` |

---

## 3. Database

### 3a. Prisma Schema

Add the `import_source` model **before** any CMG-specific sections. Also add the `owned_import_sources` relation to the `user` model.

**In `api/prisma/schema.prisma`:**

Add to `model user { ... }`:
```prisma
owned_import_sources  import_source[]
```

Add new model (place it with other core platform models, e.g., after `dataset_import_log`):
```prisma
model import_source {
  id          Int       @id @default(autoincrement())
  path        String    @unique
  label       String?
  description String?
  sort_order  Int?
  owner_id    Int?
  owner       user?     @relation(fields: [owner_id], references: [id], onDelete: SetNull)
  created_at  DateTime  @default(now()) @db.Timestamp(6)
  updated_at  DateTime  @default(now()) @updatedAt @db.Timestamp(6)
  metadata    Json?
}
```

### 3b. Migration

Create `api/prisma/migrations/20260303_add_import_sources/migration.sql`:

```sql
CREATE TABLE "import_source" (
    "id"          SERIAL NOT NULL,
    "path"        TEXT NOT NULL,
    "label"       TEXT,
    "description" TEXT,
    "sort_order"  INTEGER,
    "owner_id"    INTEGER,
    "created_at"  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at"  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "import_source_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "import_source_path_key" ON "import_source"("path");

ALTER TABLE "import_source"
    ADD CONSTRAINT "import_source_owner_id_fkey"
    FOREIGN KEY ("owner_id") REFERENCES "user"("id") ON DELETE SET NULL ON UPDATE CASCADE;
```

---

## 4. API

### 4a. New route file: `api/src/routes/datasets/imports.js`

Create this file:

```javascript
const express = require('express');
const prisma = require('@/db');
const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl } = require('@/middleware/auth');

const isPermittedTo = accessControl('import_sources');
const router = express.Router();

// TODO: Future enhancement - filter import sources by user role or ownership
router.get(
  '/sources',
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    const sources = await prisma.import_source.findMany({
      orderBy: [
        { sort_order: { sort: 'asc', nulls: 'last' } },
        { label: 'asc' },
      ],
    });
    res.json(sources);
  }),
);

module.exports = router;
```

### 4b. Register in `api/src/routes/index.js`

Sub-routes under `/datasets` **must be registered before** `/datasets` itself, or Express will interpret `/datasets/imports` as `/datasets/:datasetId`.

Find the block that registers `/datasets/uploads` before `/datasets`, and add the imports route alongside it:

```javascript
/**
 * Sub-routes under /datasets must be registered before /datasets itself,
 * otherwise Express interprets /datasets/anything as /datasets/:datasetId.
 */
if (featureService.isFeatureEnabled({ key: 'upload' })) {
  router.use('/datasets/uploads', uploadRouter /* #swagger.security = [{"BearerAuth": []}] */);
}
router.use('/datasets/imports', require('./datasets/imports') /* #swagger.security = [{"BearerAuth": []}] */);

router.use('/datasets', require('./datasets') /* #swagger.security = [{"BearerAuth": []}] */);
```

### 4c. Update `api/src/services/accesscontrols.js`

Add `import_sources: { 'read:any': ['*'] }` to **all three roles** (`admin`, `operator`, `user`).

For `admin`:
```javascript
import_sources: {
  'read:any': ['*'],
},
```

For `operator`:
```javascript
import_sources: {
  'read:any': ['*'],
},
```

For `user`:
```javascript
import_sources: {
  'read:any': ['*'],
},
```

### 4d. Replace `api/src/routes/fs.js` entirely

The route previously used a `search_space` query parameter (a config key like `slateScratch`) to look up the base directory from config. Replace the entire file with the version below.

**Key design principles of the new implementation:**
- No `search_space` or `import_source_id` param from the client
- Client sends the full `path` it wants to browse (which already starts with the import source path, since the client got that from `GET /datasets/imports/sources`)
- Server finds which configured `import_source` is a prefix of the requested path — that is the allowlist check
- `FILESYSTEM_BASE_DIR_*` / `FILESYSTEM_MOUNT_DIR_*` config is still used for Docker volume-mount path translation only (not for access control)
- Path traversal (`/../`) is automatically blocked: `path.resolve()` collapses it, and then the prefix check against the import source will fail

```javascript
const { constants } = require('node:fs');
const fs = require('fs');
const express = require('express');
const { query } = require('express-validator');
const path = require('node:path');
const createError = require('http-errors');

const config = require('config');
// eslint-disable-next-line lodash-fp/use-fp
const _ = require('lodash');
const prisma = require('@/db');
const logger = require('@/services/logger');
const asyncHandler = require('../middleware/asyncHandler');
const { accessControl } = require('../middleware/auth');

const isPermittedTo = accessControl('fs');
const router = express.Router();

/**
 * Check if a directory contains files with the specified extension.
 * @param {string} dirPath - Path to the directory to check
 * @param {string} extension - File extension to look for (e.g., '.fastq.gz')
 * @returns {Promise<boolean>}
 */
function directoryContainsExtension(dirPath, extension) {
  return new Promise((resolve) => {
    if (!extension) {
      resolve(true);
      return;
    }

    fs.readdir(dirPath, { withFileTypes: true }, (err, files) => {
      if (err) {
        logger.warn('[FS] Error reading directory for extension check', {
          dirPath,
          extension,
          error: err.message,
        });
        resolve(false);
        return;
      }

      const hasMatchingFiles = files.some((file) => {
        if (file.isDirectory()) return false;
        return file.name.endsWith(extension);
      });

      resolve(hasMatchingFiles);
    });
  });
}

/**
 * Given a path, find the configured mount mapping whose base_dir is a prefix
 * of that path. Returns { baseDir, mountDir } or null.
 */
function findMountMapping(targetPath) {
  const baseDirs = config.get('filesystem.base_dir');
  const mountDirs = config.get('filesystem.mount_dir');

  const matchingKey = Object.keys(baseDirs).find((key) => {
    const baseDir = baseDirs[key];
    if (!baseDir) return false;
    const normalized = baseDir.endsWith('/') ? baseDir : `${baseDir}/`;
    return targetPath === baseDir || targetPath.startsWith(normalized);
  });

  if (!matchingKey) return null;
  return { baseDir: baseDirs[matchingKey], mountDir: mountDirs[matchingKey] };
}

/**
 * Translate a user-visible path (under baseDir) to the actual mounted path
 * accessible by the API process (under mountDir).
 */
function toMountedPath(userPath, baseDir, mountDir) {
  const relative = userPath.slice(baseDir.length);
  return path.join(mountDir, relative);
}

/**
 * Middleware: resolve the import source for a requested path by finding which
 * configured import_source's path is a prefix of the requested path.
 *
 * This is the allowlist enforcement: paths are only served if they fall within
 * a configured import source. No client-supplied import source ID is needed —
 * the server derives policy entirely from the path itself.
 */
async function resolveImportSource(req, res, next) {
  const { path: queryPath } = req.query;

  if (!queryPath) {
    logger.warn('[FS] resolveImportSource called without path');
    return next(createError.Forbidden());
  }

  // Preserve trailing slash before normalization
  req.hasTrailingSlash = queryPath.endsWith('/');

  const normalized = path.normalize(queryPath);
  if (!normalized || !path.isAbsolute(normalized)) {
    logger.warn('[FS] Path not absolute after normalization', { queryPath, normalized });
    return res.status(400).send('Invalid path');
  }

  const resolved = path.resolve(normalized);

  // Find a matching import source whose path is a prefix of the requested path
  const importSources = await prisma.import_source.findMany();
  const importSource = importSources.find((source) => {
    const sourceWithSlash = source.path.endsWith('/') ? source.path : `${source.path}/`;
    return resolved === source.path || resolved.startsWith(sourceWithSlash);
  });

  if (!importSource) {
    logger.warn('[FS] No import source found for path — access denied', { resolved });
    return res.status(403).send('Forbidden');
  }

  // Find docker/container mount mapping for this source
  const mapping = findMountMapping(importSource.path);
  if (!mapping) {
    logger.error('[FS] No mount mapping found for import source', { sourcePath: importSource.path });
    return res.status(500).send('Filesystem configuration error');
  }

  req.query.path = resolved;
  req.importSource = importSource;
  req.mountMapping = mapping;

  logger.info('[FS] resolveImportSource passed', {
    sourcePath: importSource.path,
    resolvedPath: resolved,
    baseDir: mapping.baseDir,
    mountDir: mapping.mountDir,
    hasTrailingSlash: req.hasTrailingSlash,
  });

  return next();
}

router.get(
  '/',
  asyncHandler(resolveImportSource),
  isPermittedTo('read'),
  query('dirs_only').default(false),
  query('extension').optional().trim(),
  asyncHandler(async (req, res, next) => {
    const { dirs_only, path: query_path, extension } = req.query;
    const { baseDir, mountDir } = req.mountMapping;
    const { hasTrailingSlash } = req;

    logger.info('[FS] Request received', {
      query_path,
      dirs_only,
      import_source: req.importSource.path,
      extension,
      user: req.user?.username,
    });

    if (!query_path) {
      logger.info('[FS] No query_path provided, returning empty array');
      res.json([]);
      return;
    }

    const mounted_search_dir = toMountedPath(query_path, baseDir, mountDir);

    logger.info('[FS] Path resolution', {
      baseDir,
      mountDir,
      mounted_search_dir,
      query_path,
    });

    fs.access(mounted_search_dir, constants.F_OK, (err) => {
      if (err) {
        logger.info('[FS] Exact path not found, attempting substring match', {
          mounted_search_dir,
          error: err.message,
          code: err.code,
        });

        const parent_query_path = path.dirname(query_path);
        const search_term = path.basename(query_path);
        const parent_mounted_dir = toMountedPath(parent_query_path, baseDir, mountDir);

        logger.info('[FS] Attempting case-insensitive substring match', {
          parent_query_path,
          search_term,
          parent_mounted_dir,
        });

        // Ensure the parent path is still within the import source
        const sourcePath = req.importSource.path;
        const sourcePathWithSlash = sourcePath.endsWith('/') ? sourcePath : `${sourcePath}/`;
        if (parent_query_path !== sourcePath && !parent_query_path.startsWith(sourcePathWithSlash)) {
          logger.warn('[FS] Parent path outside import source', { parent_query_path, sourcePath });
          res.json([]);
          return;
        }

        fs.access(parent_mounted_dir, constants.F_OK, (parentErr) => {
          if (parentErr) {
            logger.warn('[FS] Parent directory access failed', {
              parent_mounted_dir,
              error: parentErr.message,
            });
            res.json([]);
            return;
          }

          fs.readdir(parent_mounted_dir, { withFileTypes: true }, (readErr, files) => {
            if (readErr) {
              logger.error('[FS] Error reading parent directory', {
                parent_mounted_dir,
                error: readErr.message,
              });
              res.json([]);
              return;
            }

            let matchingFiles = files
              .filter((f) => {
                const nameMatches = f.name.toLowerCase().includes(search_term.toLowerCase());
                const isDirCheck = dirs_only ? f.isDirectory() : true;
                return nameMatches && isDirCheck;
              })
              .map((f) => ({
                name: f.name,
                isDir: f.isDirectory(),
                path: path.join(parent_query_path, f.name),
              }));

            if (extension && dirs_only) {
              const extensionFilterPromises = matchingFiles.map(async (file) => {
                if (!file.isDir) return file;
                const mountedPath = toMountedPath(file.path, baseDir, mountDir);
                const hasExtension = await directoryContainsExtension(mountedPath, extension);
                return hasExtension ? file : null;
              });

              Promise.all(extensionFilterPromises).then((filtered) => {
                matchingFiles = _.compact(filtered);
                logger.info('[FS] Substring match results (after extension filter)', {
                  search_term,
                  extension,
                  total_matches: matchingFiles.length,
                });
                res.json(matchingFiles);
              });
            } else {
              logger.info('[FS] Substring match results', {
                search_term,
                total_matches: matchingFiles.length,
              });
              res.json(matchingFiles);
            }
          });
        });
        return;
      }

      if (!hasTrailingSlash) {
        logger.info('[FS] Exact path found without trailing slash, returning directory as match', {
          query_path,
        });

        const dirResult = {
          name: path.basename(query_path),
          isDir: true,
          path: query_path,
        };

        if (extension && dirs_only) {
          directoryContainsExtension(mounted_search_dir, extension).then((hasExtension) => {
            res.json(hasExtension ? [dirResult] : []);
          });
        } else {
          res.json([dirResult]);
        }
        return;
      }

      logger.info('[FS] Exact path found with trailing slash, returning directory contents', {
        mounted_search_dir,
      });

      fs.readdir(mounted_search_dir, { withFileTypes: true }, (_err, files) => {
        if (_err) {
          logger.error('[FS] Error reading directory', {
            mounted_search_dir,
            error: _err.message,
            code: _err.code,
          });
          return next(createError.InternalServerError('Error reading directory'));
        }

        logger.info('[FS] Directory read successful', {
          mounted_search_dir,
          total_entries: files ? files.length : 0,
        });

        let filesData = files.map((f) => {
          const file = {
            name: f.name,
            isDir: f.isDirectory(),
            path: path.join(query_path, f.name),
          };
          if (dirs_only) return file.isDir ? file : null;
          return file;
        });
        filesData = _.compact(filesData);

        if (extension && dirs_only) {
          const extensionFilterPromises = filesData.map(async (file) => {
            if (!file.isDir) return file;
            const mountedPath = path.join(mounted_search_dir, file.name);
            const hasExtension = await directoryContainsExtension(mountedPath, extension);
            return hasExtension ? file : null;
          });

          Promise.all(extensionFilterPromises).then((filtered) => {
            filesData = _.compact(filtered);
            logger.info('[FS] Response prepared (after extension filter)', {
              dirs_only,
              extension,
              total_before_filter: files ? files.length : 0,
              total_after_filter: filesData.length,
            });
            res.json(filesData);
          });
        } else {
          logger.info('[FS] Response prepared', {
            dirs_only,
            total_before_filter: files ? files.length : 0,
            total_after_filter: filesData.length,
          });
          res.json(filesData);
        }
      });
    });
  }),
);

module.exports = router;
```

### 4e. Update `api/src/routes/datasets/index.js` — POST / (create dataset)

**Remove** the old `import_space` / `restricted_import_dirs` block:
```javascript
// OLD — DELETE THIS ENTIRE BLOCK
if (import_space) {
  const restricted_import_dirs = config.restricted_import_dirs[import_space].split(',');
  const is_origin_path_restricted = restricted_import_dirs.some((glob) => {
    const isMatch = pm(glob);
    const matches = isMatch(decoded_origin_path, glob);
    return matches.isMatch;
  });
  if (is_origin_path_restricted) {
    return next(createError.Forbidden({
      message: `Import space ${import_space} is restricted for dataset creation`,
    }));
  }
}
```

**Replace** with the import source allowlist check:
```javascript
// NEW — ADD THIS
if (create_method === CONSTANTS.DATASET_CREATE_METHODS.IMPORT && decoded_origin_path) {
  // Validate origin_path is within one of the configured import sources (allowlist)
  const importSources = await prisma.import_source.findMany();
  const isWithinImportSource = importSources.some((source) => {
    const sourcePathWithSlash = source.path.endsWith('/') ? source.path : `${source.path}/`;
    return decoded_origin_path === source.path || decoded_origin_path.startsWith(sourcePathWithSlash);
  });
  if (!isWithinImportSource) {
    return next(createError.Forbidden({
      message: 'Dataset origin path is not within any configured import source',
    }));
  }
}
```

Also update the destructuring of `req.body` — remove `import_space`:
```javascript
// OLD
const {
  import_space, create_method, project_id, ...
} = req.body;

// NEW
const {
  create_method, project_id, ...
} = req.body;
```

Also **remove** the `picomatch` import from the top of the file if it is only used for the `import_space` check:
```javascript
// REMOVE if no longer used elsewhere:
const pm = require('picomatch');
```

Also update the JSDoc comment for the create endpoint:
```javascript
// OLD
* @throws {Error} If dataset creation fails or if the origin path is restricted.

// NEW
* @throws {Error} If dataset creation fails or if the origin path is outside any configured import source.
```

### 4f. Update `api/src/services/dataset.js` — remove `import_space` from import log metadata

Find the `buildDatasetCreateQuery` function. In the `import_logs` create block, remove `import_space` from the metadata:

```javascript
// OLD
create: [{
  source_run: src_dataset_id ? String(src_dataset_id) : null,
  metadata: {
    import_space: data.import_space || null,
    notes: import_notes || null,
  },
}],

// NEW
create: [{
  source_run: src_dataset_id ? String(src_dataset_id) : null,
  metadata: {
    notes: import_notes || null,
  },
}],
```

> **Note:** `import_notes` is CMG-specific. In base bioloop, if `import_notes` does not exist, the entire `metadata` object may be omitted from the import log create, or kept as `metadata: {}`. Match whatever base bioloop's `buildDatasetCreateQuery` currently does.

### 4g. Update `api/config/custom-environment-variables.json`

**Remove** the `filesystem.search_spaces` and `restricted_import_dirs` keys. The filesystem section should only contain `base_dir` and `mount_dir`:

```json
"filesystem": {
  "base_dir": {
    "slateScratch": "FILESYSTEM_BASE_DIR_SCRATCH",
    "slateProject": "FILESYSTEM_BASE_DIR_PROJECT"
  },
  "mount_dir": {
    "slateScratch": "FILESYSTEM_MOUNT_DIR_SCRATCH",
    "slateProject": "FILESYSTEM_MOUNT_DIR_PROJECT"
  }
}
```

> The key names (`slateScratch`, `slateProject`) and env var names may differ in base bioloop. Adapt to whatever names the base repo uses. The important thing is that `search_spaces` and `restricted_import_dirs` keys are removed.

### 4h. Update `api/config/default.json`

**Remove** the following keys entirely:
- `filesystem_search_spaces` (top-level)
- `filesystem.search_spaces`
- `restricted_import_dirs`

The `filesystem` key should only retain `base_dir` and `mount_dir`:
```json
"filesystem": {
  "base_dir": {
    "slateScratch": "",
    "slateProject": ""
  },
  "mount_dir": {
    "slateScratch": "",
    "slateProject": ""
  }
}
```

### 4i. Update `api/.env.default`

**Remove** these lines:
```bash
FILESYSTEM_SEARCH_SPACES=...
SCRATCH_IMPORT_RESTRICTED_DIRS=...
# SCRATCH_IMPORT_RESTRICTED_DIRS=...  (any commented variants)
PROJECT_IMPORT_RESTRICTED_DIRS=...   (if present)
```

**Keep** these (they are still needed for Docker mount translation):
```bash
FILESYSTEM_BASE_DIR_SCRATCH=/opt/sca/data/imports
FILESYSTEM_MOUNT_DIR_SCRATCH=/opt/sca/data/imports
FILESYSTEM_BASE_DIR_PROJECT=/opt/sca/data/project
FILESYSTEM_MOUNT_DIR_PROJECT=/opt/sca/data/project
```

---

## 5. Seed / Init Scripts

### 5a. Update `api/prisma/seed.js`

Add import source seeding **before** any optional test-data steps (tracks, sessions, etc.) so that a failure in those steps cannot prevent import sources from being seeded.

Add this block inside `main()`:

```javascript
// Seed import sources for non-production environments
const importSources = [
  {
    path: '/opt/sca/data/imports/entrypoint',
    label: 'Imports',
    description: 'Default import source for docker/dev environment',
    sort_order: 1,
  },
  {
    path: '/opt/sca/data/project/entrypoint',
    label: 'Project',
    description: 'Project filesystem import source for docker/dev environment',
    sort_order: 2,
  },
];
await Promise.all(
  importSources.map((source) => prisma.import_source.upsert({
    where: { path: source.path },
    create: source,
    update: { label: source.label, description: source.description, sort_order: source.sort_order },
  })),
);
console.log(`seeded ${importSources.length} import sources`);
```

> The paths here match `FILESYSTEM_BASE_DIR_SCRATCH/entrypoint` and `FILESYSTEM_BASE_DIR_PROJECT/entrypoint` from `.env.default`. Adjust for base bioloop's equivalent paths.

### 5b. Create `api/src/scripts/init_prod_import_sources.js`

This script populates import sources in production. It is **idempotent** (uses `upsert`). Customize the paths/labels for the specific deployment.

```javascript
/* eslint-disable no-console */
require('module-alias/register');
const path = require('path');
const { PrismaClient } = require('@prisma/client');

global.__basedir = path.join(__dirname, '..', '..');

const prisma = new PrismaClient();

const importSources = [
  {
    path: '/path/to/prod/import/source/1',
    label: 'Source One',
    description: 'Description of source one',
    sort_order: 1,
  },
  {
    path: '/path/to/prod/import/source/2',
    label: 'Source Two',
    description: 'Description of source two',
    sort_order: 2,
  },
];

async function main() {
  await Promise.all(
    importSources.map((source) => prisma.import_source.upsert({
      where: { path: source.path },
      create: source,
      update: { label: source.label, description: source.description, sort_order: source.sort_order },
    })),
  );

  console.log(`created/updated ${importSources.length} import sources`);
}

main()
  .then(() => {
    prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
```

---

## 6. UI

### 6a. New service: `ui/src/services/import.js`

Create this file. Convention: method names prefixed with `_` do not make API calls.

```javascript
import api from './api';

export default {
  /**
   * Get all configured import sources.
   * @returns {Promise} Axios response with list of import_source records
   */
  getSources() {
    return api.get('/datasets/imports/sources');
  },

  /**
   * Returns a display label for an import source, falling back to its path.
   * @param {Object} source - import_source record
   * @returns {string}
   */
  _getLabel(source) {
    return source?.label || source?.path || '';
  },
};
```

### 6b. Update `ui/src/services/fs.js`

Remove the `search_space` parameter:

```javascript
// OLD
class FileSystemService {
  getPathFiles({ path, dirs_only, search_space, extension }) {
    return api.get('/fs', { params: { path, dirs_only, search_space, extension } });
  }
}

// NEW
class FileSystemService {
  getPathFiles({ path, dirs_only, extension }) {
    return api.get('/fs', { params: { path, dirs_only, extension } });
  }
}
```

### 6c. Update `ui/src/config.js`

**Remove** the `filesystem_search_spaces` and `restricted_import_dirs` keys entirely:

```javascript
// OLD — REMOVE THESE
filesystem_search_spaces: [
  {
    slateScratch: {
      base_path: import.meta.env.VITE_SCRATCH_BASE_DIR || '/bioloop/scratch/space',
      mount_path: import.meta.env.VITE_SCRATCH_MOUNT_DIR || '/bioloop/user/scratch/mount/dir',
      key: 'slateScratch',
      label: 'Slate-Scratch',
    },
  },
  {
    slateProject: {
      base_path: import.meta.env.VITE_PROJECT_BASE_DIR || '/bioloop/project/space',
      mount_path: import.meta.env.VITE_PROJECT_MOUNT_DIR || '/bioloop/user/project/mount/dir',
      key: 'slateProject',
      label: 'Slate-Project',
    },
  },
],
restricted_import_dirs: {
  slateScratch: {
    paths: import.meta.env.VITE_SCRATCH_IMPORT_RESTRICTED_DIRS || '/scratch/space/restricted',
    key: 'scratch',
  },
  slateProject: {
    paths: import.meta.env.VITE_PROJECT_IMPORT_RESTRICTED_DIRS || '/project/space/restricted',
    key: 'project',
  },
},
```

> These keys may have different names in base bioloop. Remove whichever keys served as the hardcoded filesystem space configuration.

### 6d. Update `ui/.env.default`

**Remove** all lines related to filesystem search spaces and restricted dirs:
```bash
# REMOVE ALL OF THESE
VITE_SCRATCH_BASE_DIR=...
VITE_SCRATCH_MOUNT_DIR=...
VITE_SCRATCH_IMPORT_RESTRICTED_DIRS=...
VITE_PROJECT_BASE_DIR=...
VITE_PROJECT_MOUNT_DIR=...
VITE_PROJECT_IMPORT_RESTRICTED_DIRS=...   (if present)
VITE_FILESYSTEM_SEARCH_SPACES=...
```

### 6e. Update `ui/src/components/dataset/import/ImportStepper.vue`

This is the most substantial UI change. Below are the specific changes to make. Apply them on top of **base bioloop's** ImportStepper (not the CMG fork), so do not add any CMG-specific fields.

#### Add import
```javascript
import importService from '@/services/import';
```

#### Remove import
```javascript
// REMOVE — no longer needed
import pm from 'picomatch';
```

#### Remove constant
```javascript
// REMOVE
const IMPORT_NOT_ALLOWED_ERROR = "Selected file cannot be imported as a dataset";
```

#### Replace hardcoded FILESYSTEM_SEARCH_SPACES with reactive ref

**Old:**
```javascript
const FILESYSTEM_SEARCH_SPACES = (config.filesystem_search_spaces || []).map(
  (space) => space[Object.keys(space)[0]],
);
```

**New:**
```javascript
// Import sources loaded from the API
const importSources = ref([]);
```

#### Replace `searchSpace` ref with `selectedImportSource` ref

**Old:**
```javascript
const searchSpace = ref(
  FILESYSTEM_SEARCH_SPACES instanceof Array && FILESYSTEM_SEARCH_SPACES.length > 0
    ? FILESYSTEM_SEARCH_SPACES[0]
    : "",
);
```

**New:**
```javascript
const selectedImportSource = ref(null);
```

#### Replace `searchSpaceBasePath` / `_searchText` computed properties

**Old:**
```javascript
const searchSpaceBasePath = computed(() => searchSpace.value.base_path);

const _searchText = computed(() => {
  return (
    (searchSpace.value.base_path.endsWith("/")
      ? searchSpace.value.base_path
      : searchSpace.value.base_path + "/") + fileListSearchText.value
  );
});
```

**New:**
```javascript
const importSourcePath = computed(() => selectedImportSource.value?.path ?? '');

const _searchText = computed(() => {
  if (!importSourcePath.value) return '';
  return (
    (importSourcePath.value.endsWith("/")
      ? importSourcePath.value
      : importSourcePath.value + "/") + fileListSearchText.value
  );
});
```

#### Add `loadingImportSources` ref

```javascript
const loadingImportSources = ref(false);
```

#### Add `loadImportSources` function (add alongside other resource-loading functions)

```javascript
const loadImportSources = () => {
  loadingImportSources.value = true;
  return importService
    .getSources()
    .then((res) => {
      importSources.value = res.data;
      // Default to the first configured source
      if (importSources.value.length > 0 && !selectedImportSource.value) {
        selectedImportSource.value = importSources.value[0];
      }
    })
    .catch((err) => {
      toast.error('Failed to load import sources');
      console.error(err);
    })
    .finally(() => {
      loadingImportSources.value = false;
    });
};
```

#### Call `loadImportSources` in `onMounted`

Inside the `onMounted` that loads initial resources, add:
```javascript
await loadImportSources();
```

#### Remove `getRestrictedImportPaths` function entirely

```javascript
// REMOVE THIS FUNCTION
const getRestrictedImportPaths = () => {
  return config.restricted_import_dirs[searchSpace.value.key].paths.split(",");
};
```

#### Simplify step 0 form error validation

In `setFormErrors`, replace the old restricted-path check for step 0 with a simple null-check:

**Old:**
```javascript
if (step.value === 0) {
  if (!selectedFile.value) {
    formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = NO_FILE_SELECTED_ERROR;
    return;
  }
  const restricted_dataset_paths = getRestrictedImportPaths();
  const origin_path_is_restricted = selectedFile.value
    ? restricted_dataset_paths.some((pattern) => {
        const _path = selectedFile.value.path;
        let isMatch = pm(pattern);
        const matches = isMatch(_path, pattern);
        return matches.isMatch;
      })
    : false;
  if (origin_path_is_restricted) {
    formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = IMPORT_NOT_ALLOWED_ERROR;
    return;
  } else {
    formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = null;
  }
}
```

**New:**
```javascript
if (step.value === 0) {
  if (!selectedFile.value) {
    formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = NO_FILE_SELECTED_ERROR;
    return;
  }
  formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = null;
}
```

#### Update `searchFiles` function

Guard against no selected import source, and remove `search_space` param:

**Old:**
```javascript
const searchFiles = async () => {
  fileSystemService
    .getPathFiles({
      path: _searchText.value,
      dirs_only: true,
      search_space: searchSpace.value.key,
    })
    ...
};
```

**New:**
```javascript
const searchFiles = async () => {
  if (!selectedImportSource.value) {
    setRetrievedFiles([]);
    searchingFiles.value = false;
    return;
  }

  fileSystemService
    .getPathFiles({
      path: _searchText.value,
      dirs_only: true,
    })
    ...
};
```

#### Update `importFormData` computed — remove `import_space`

**Old:**
```javascript
origin_path: selectedFile.value.path,
import_space: searchSpace.value.key,
create_method: Constants.DATASET_CREATE_METHODS.IMPORT,
```

**New:**
```javascript
origin_path: selectedFile.value.path,
create_method: Constants.DATASET_CREATE_METHODS.IMPORT,
```

#### Update the watcher dependency array

Replace `searchSpace` with `selectedImportSource` in the large watch that triggers form error revalidation:

```javascript
// OLD entry in the watch array:
searchSpace,

// NEW:
selectedImportSource,
```

#### Update the template — Replace search space `<va-select>` with import source selector

**Old template (step-content-0):**
```html
<va-select
  class="mr-2"
  v-model="searchSpace"
  @update:modelValue="resetSearch"
  :options="FILESYSTEM_SEARCH_SPACES"
  :text-by="'label'"
  :track-by="'key'"
  label="Search space"
  :disabled="submitAttempted || searchingFiles || validatingForm"
/>
```

**New template (step-content-0):**
```html
<va-select
  class="mr-2"
  v-model="selectedImportSource"
  @update:modelValue="resetSearch"
  :options="importSources"
  :text-by="importService._getLabel"
  :track-by="'id'"
  label="Import Source"
  :disabled="submitAttempted || searchingFiles || validatingForm || importSources.length === 0"
  :loading="loadingImportSources"
/>
```

#### Update `FileListAutoComplete` `:base-path` binding

**Old:**
```html
:base-path="searchSpaceBasePath"
```

**New:**
```html
:base-path="importSourcePath"
```

#### Update step-content-3 (review/import step) — `import-space` prop

Find the component in step-content-3 (usually `<ImportInfo>` or similar review display) that receives an `import-space` prop:

**Old:**
```html
:import-space="searchSpace.label"
```

**New:**
```html
:import-space="importService._getLabel(selectedImportSource)"
```

---

## 7. Design Notes for the Implementing Agent

- **`sort_order`** controls display order in the dropdown. `NULL` values sort last, then alphabetically by label. To reorder sources after deployment, just `UPDATE import_source SET sort_order = N WHERE path = '...'` — no code change needed.
- **Security model is now an allowlist**, not a denylist. A user can only browse paths within a configured import source. Path traversal (`/../`) is automatically neutralised by `path.resolve()` before the prefix check.
- **No client-supplied import source ID in the `/fs` API.** The client sends the full path (which starts with the import source path). The server finds the matching import source by prefix lookup. This is the standard filesystem-API pattern and removes any ID coupling from the client.
- **`FILESYSTEM_BASE_DIR_*` / `FILESYSTEM_MOUNT_DIR_*` env vars** are kept but serve only one purpose: Docker volume-mount path translation. In production (no Docker), set them equal to the actual filesystem paths. They are not the access policy.
- **TODO (future):** Role-based or ownership-based filtering of import sources is not yet implemented. When implementing, add it to the `GET /datasets/imports/sources` route handler in `api/src/routes/datasets/imports.js`.
