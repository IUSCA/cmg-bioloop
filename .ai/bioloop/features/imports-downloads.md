# Imports & Downloads Feature

**Feature Scope:** Dataset import (from external sources) and secure download mechanisms.

**Status:** Core Platform Feature

**Note:** Browser-based file uploads are covered in `uploads.md`, not here.

---

## Imports (External Dataset Ingestion)

### Overview
The Import feature ingests datasets from external sources (like SDA/tape archives) into the system. This is different from browser uploads.

### Import Flow
1. Operator schedules import via API
2. Worker downloads dataset from source (SDA, network path)
3. Dataset registered in system
4. `integrated` workflow processes the dataset
5. Import status tracked in `dataset_import_log`

### Database
- `dataset_import_log` - Tracks import operations
- `dataset_audit` - Audit trail for imported datasets

### Import Sources
- SDA (Scholarly Data Archive) via HSI commands
- Network file systems
- External URLs

### UI Import Stepper

The import UI (`ImportStepper.vue`) has a 4-step process:

1. **Select Directory** - Choose dataset path and file type
2. **General Info** - Dataset type, project, source data, source data product
3. **Genomic Details** - Genome type, assembly, notes
4. **Import** - Review and initiate import

**File Type Filtering (Added 2026-02-09):**
- File Type field is now in Step 1 (Select Directory), above the Dataset Path field
- When a file type is selected, the directory list is filtered to show only directories containing files with that extension
- API endpoint `/fs` now accepts `extension` parameter for server-side filtering
- Filtering improves UX by showing only relevant directories for the selected data type

**Source Data Product Field (Added 2026-02-09):**
- New "Assign source Data Product" field in Step 2 (General Info), after Source Instrument field
- Only shown when Dataset Type is DATA_PRODUCT AND File Type (analysis_type) is FASTQ
- Establishes parent-child lineage in `dataset_hierarchy` table (selected source is parent, imported dataset is child)
- Automatically hidden and cleared if Dataset Type or File Type changes to non-qualifying values
- Allows tracking data provenance for derived FASTQ datasets
- Multiple source relationships supported (can assign both Raw Data source and Data Product source)
- Uses `analysis_type` relation from database to check if file type is FASTQ
- **Access Control:** Dropdown filters Data Products to show only those the user has access to (uses `/:username/all` endpoint for non-operators)
- **Validation:** Next button is disabled if "Assign source Data Product" checkbox is checked but no data product is selected

---

## Downloads

### Overview
Secure download mechanism that enforces access control and provides audit logging.

### Download Types

#### Direct Download
- Small files served directly from API
- Appropriate for files < 100MB
- Uses standard HTTP file serving

#### Secure Download Service
- Large files served via dedicated download service
- Supports resume (Range requests)
- Token-based authentication
- Rate limiting and quota enforcement

### Secure Download Flow
1. User requests download via API
2. API validates user access to dataset/file
3. API generates time-limited download token (via Signet)
4. User redirected to secure download service with token
5. Download service validates token
6. File streaming begins

### API Endpoints
- `GET /datasets/:id/files/:file_id/download` - Request download
- `GET /download/:token` - Execute download (download service)

---

## Configuration

### Download Limits
```json
{
  "download": {
    "rate_limit": "100MB/s",
    "concurrent_downloads": 3,
    "token_expiry": "1h"
  }
}
```

---

## Changelog

### 2026-02-09 - Import UI File Type Filtering

**Feature:** Enhanced import UX with file type-based directory filtering.

**Changes:**
- **UI (ImportStepper.vue):**
  - Moved File Type field from Step 3 (Genomic Details) to Step 1 (Select Directory)
  - Positioned File Type field above Dataset Path field for better workflow
  - File Type field is clearable to allow unfiltered directory browsing
  - Directory search automatically refreshes when file type changes
  
- **API (fs.js):**
  - Added `extension` query parameter to `/fs` endpoint
  - Implemented `directoryContainsExtension()` helper function
  - Server-side filtering: only returns directories containing files with selected extension
  - Applied filtering in all three return paths:
    - Substring matches (search results)
    - Exact path matches (direct navigation)
    - Directory contents (trailing slash navigation)
  
- **Service (fs.js):**
  - Updated `getPathFiles()` to accept and pass `extension` parameter

**Rationale:**
- Users importing specific data types (e.g., FASTQ, BAM) should only see directories containing that file type
- Moving file type to first step allows early filtering, improving search experience
- Server-side filtering prevents showing irrelevant directories, reducing clutter

**Files Modified:**
- `ui/src/components/dataset/import/ImportStepper.vue`
- `ui/src/services/fs.js`
- `api/src/routes/fs.js`

### 2026-02-11 - Schema Refactor: Direct Dataset Linking

**Schema Change:** Import logs now link directly to datasets (matching upload logs refactor).

**Changes:**
- `dataset_import_log.audit_log_id` removed, replaced with `dataset_id`
- Import logs now link directly to datasets via `dataset_id` foreign key
- Audit logs remain independent for user tracking

**Query Changes:**
- Old: `where: { audit_log: { dataset_id, create_method: 'IMPORT' } }`
- New: `where: { dataset_id }`
- User filtering via `dataset.audit_logs.some({ action: 'create', user: { username } })`

**CMG Sync Updated:**
- Bigbang script sets `create_method` on dataset
- Import logs created with `dataset_id` instead of `audit_log_id`
- Audit logs created independently for user tracking

**Benefits:**
- Consistent with upload logs schema
- Simpler queries and better performance
- Clearer separation of concerns

**Files Modified:**
- API: routes/datasets/index.js (import log queries)
- data_sync: sync_import_logs.js (bigbang script)
- UI: pages/datasets/imports/index.vue

**Migration:** Same migration as uploads (`20260211_refactor_upload_import_logs_remove_audit_relation`)

---

### 2026-03-03 - Import Sources: DB-Driven Import Entry Points

**Feature:** Replaced hardcoded filesystem "search spaces" (slateScratch/slateProject) with a DB-managed `import_source` table, providing configurable, labeled import entry points shown directly in the UI.

**Design Decisions:**

- **Terminology:** "Import Source" chosen over "entry point" or "search space" as a more descriptive, industry-standard term. DB table: `import_source`. UI label: "Import Source".
- **DB-first:** Import sources are stored in the `import_source` table (`id`, `path`, `label`, `description`, `owner_id`), not in env vars or config. This avoids per-entry-point env vars proliferating as instances grow.
- **UX:** The UI step-0 dropdown now lists human-friendly import source labels (or paths as fallback) loaded from `GET /import-sources`. No knowledge of filesystem "spaces" required.
- **Path containment (allowlist):** The `/fs` route validates that any requested path starts with the selected import source's path. Path traversal attempts (`/../`) are rejected at the API level. This replaces the old `restricted_import_dirs` concept, which was a denylist.
- **Mount translation preserved:** `FILESYSTEM_BASE_DIR_*` / `FILESYSTEM_MOUNT_DIR_*` env vars and config remain for docker volume-mount path translation. These are infrastructure-level and bounded by the number of mounted filesystems, not by import source count.
- **No role-based filtering (for now):** All authenticated users see all configured import sources. TODO: implement per-user or per-role filtering of import sources.
- **Dataset creation validation:** `origin_path` on dataset create is validated against all configured import sources in the DB (allowlist). The old `import_space` + `restricted_import_dirs` config-based check is removed.
- **`import_space` removed:** The `import_space` field previously sent from UI and stored in import log metadata is removed. The `origin_path` on the dataset already encodes which source it came from.

**Population Strategy:**
- Non-prod: `prisma/seed.js` seeds two sources at `/opt/sca/data/imports/entrypoint` and `/opt/sca/data/project/entrypoint`.
- Production: `api/src/scripts/init_prod_import_sources.js` seeds `/N/project/yunliu-general/SCA_incoming` and `/N/project/CMG-SCA`.

**Removed:**
- `FILESYSTEM_SEARCH_SPACES`, `SCRATCH_IMPORT_RESTRICTED_DIRS`, `PROJECT_IMPORT_RESTRICTED_DIRS` env vars (API)
- `VITE_SCRATCH_BASE_DIR`, `VITE_SCRATCH_MOUNT_DIR`, `VITE_PROJECT_BASE_DIR`, `VITE_PROJECT_MOUNT_DIR`, `VITE_SCRATCH_IMPORT_RESTRICTED_DIRS`, `VITE_PROJECT_IMPORT_RESTRICTED_DIRS`, `VITE_FILESYSTEM_SEARCH_SPACES` env vars (UI)
- `filesystem_search_spaces` and `restricted_import_dirs` keys from UI `config.js`
- `filesystem.search_spaces` and `restricted_import_dirs` from API `default.json` / `custom-environment-variables.json`

**Files Modified:**
- `api/prisma/schema.prisma` — added `import_source` model; added `owned_import_sources` relation to `user`
- `api/prisma/migrations/20260303_add_import_sources/migration.sql` — new migration
- `api/prisma/seed.js` — seeds two import sources for non-prod
- `api/src/scripts/init_prod_import_sources.js` — new prod init script
- `api/src/routes/importSources.js` — new `GET /import-sources` route
- `api/src/routes/index.js` — registered `/import-sources` route
- `api/src/routes/fs.js` — refactored to load import source from DB by `import_source_id`, validate path containment, infer mount mapping
- `api/src/routes/datasets/index.js` — replaced `import_space`/`restricted_import_dirs` check with DB-based import source path validation
- `api/src/services/dataset.js` — removed `import_space` from import log metadata
- `api/src/services/accesscontrols.js` — added `import_sources: read:any` to all roles
- `api/config/custom-environment-variables.json` — removed obsolete keys
- `api/config/default.json` — removed obsolete keys
- `api/.env.default` — removed obsolete vars
- `ui/src/services/importSource.js` — new service
- `ui/src/services/fs.js` — renamed `search_space` param to `import_source_id`
- `ui/src/components/dataset/import/ImportStepper.vue` — loads import sources from API; replaces search space dropdown; removes client-side restricted-path check; removes picomatch dependency
- `ui/src/config.js` — removed `filesystem_search_spaces` and `restricted_import_dirs`
- `ui/.env.default` — removed all filesystem-search-space env vars

**Last Updated:** 2026-03-03
