# Conversions Feature

**Feature Scope:** Genomic data conversion pipelines that transform datasets from one format to another (e.g., FASTQ → BAM → VCF).

**Status:** Implemented

**Related Documentation:**
- `/genome-conversion-pipelines-2026-01-11.md`
- `/genome-browser-sessions-tracks-conversions-2026-01-03.md`
- `/workers/README_conversion_tools.md`

---

## 2026-01-16

### Initial State Documentation

**Context:** This feature provides a flexible, extensible framework for executing bioinformatics pipelines that transform genomic datasets.

**Key Architecture Decisions:**
- Decision: Conversions are stateless pipeline executors
- Decision: Conversions do NOT create genome browser sessions directly
- Decision: Sessions are created only after data products exist
- Constraint: Input datasets must match conversion definition's `dataset_types` whitelist
- Constraint: Only `enabled` conversion definitions can be initiated

**Database Schema:**
- `conversion_definition`: Registry of available conversion pipelines
  - Includes: name, description, enabled, dataset_types, tags, program_id
  - References: cmd_line_program, user (author)
  
- `cmd_line_program`: Executable programs used by conversions
  - Includes: name, executable_path, executable_directory, allow_additional_args
  - Has many: arguments
  
- `argument`: Command-line arguments for programs
  - Includes: flag, datatype, required, default_value, description
  
- `conversion`: Instance of a conversion execution
  - Includes: status, input_dataset_id, output_dataset_id
  - References: conversion_definition, user, datasets
  - Has many: conversion_outputs (for multiple output datasets)

**Execution Flow:**
1. User initiates conversion via API
2. API validates input dataset type against conversion definition
3. API creates conversion record with status `pending`
4. Celery task submitted to worker queue
5. Worker executes command-line program with arguments
6. Worker creates output dataset(s) and links to conversion
7. Conversion status updated to `completed` or `failed`

**Key Code Locations:**
- API Routes: `/api/src/routes/conversions.js`
- API Service: `/api/src/services/conversion.js`
- Worker Tasks: `/workers/workers/tasks/conversion_task.py`
- UI Components: `/ui/src/components/conversions/`
- Prisma Schema: `/api/prisma/schema.prisma` (models: conversion, conversion_definition, cmd_line_program, argument)

**CMG Migration Notes:**
- CMG's conversion pipelines are being migrated to Bioloop's conversion_definition table
- Mapping: CMG `pipeline` field → Bioloop `conversion_definition.name`
- Seed data: `/api/prisma/seed_data/conversion/` (mock data for testing)
- Future: Populate conversion_definition based on unique CMG pipeline values

**Current Status:**
- Core conversion framework: Implemented
- API endpoints: Implemented
- Worker integration: Implemented
- UI components: Implemented
- CMG migration: Planned (not yet executed)

---

## 2026-01-23

### Report Viewing Moved to Secure Download Service

**Decision:** Conversion reports are now served through the secure_download microservice instead of directly from the core API.

**Architecture Change:**
- Reports previously served via `/api/reports/conversions/:id/files*` (core API)
- Reports now served via `/secure/reports/conversions/{conversion_id}/{dataset_name}/Reports/*` (secure_download)
- Core API endpoint `/api/reports/conversions/:id/url` issues time-limited JWT tokens for secure access
- Legacy endpoint kept for backwards compatibility but marked for deprecation

**Token-Based Authentication:**
- UI requests secure URL from core API: `GET /api/reports/conversions/:id/url`
- Core API validates conversion exists and issues JWT with `download_file:` scope
- JWT token embedded in URL returned to UI
- secure_download service validates token before serving files
- TODO: Create separate `view_reports:` scope for better separation from download scope

**Path Resolution:**
- Legacy conversions: Use `conversion.cmg_id` for conversion identifier
- New conversions: Use `conversion.id` for conversion identifier
- Dataset identifier: Use `dataset.name` (NOT dataset ID or cmg_id)
- Pattern: `conversions/{conversion_id}/{dataset_name}/Reports/`
- Matches structure created by `clone_legacy_conversion_reports.py` script

**File Locations:**
- Legacy reports: `/N/project/CMG-SCA/production/conversion/{cmg_id}/{dataset_name}/Reports/`
- New reports: `/N/scratch/cmguser/cmg-bioloop/conversions/{conversion_id}/{dataset_name}/Reports/`
- secure_download mount: `/N/scratch/cmguser/cmg-bioloop` → `/opt/sca/data` (read-only)

**Implementation:**
- secure_download route: `/secure_download/src/routes/reports.js`
- Core API endpoint: `/api/src/routes/reports.js` (new `/conversions/:id/url` endpoint)
- Reuses existing OAuth2 client credentials flow from download feature

**Rationale:**
- Separates file serving from business logic (microservice architecture)
- Provides time-limited, scoped access to reports
- Consistent with existing download authentication pattern
- Enables fine-grained access control

---

## 2026-01-27

### Report URLs Fixed to Use Secure Download Service

**Issue:** The "View Reports" button was redirecting to the core API (`/api/reports/conversions/:id/files/html/index.html`) instead of the secure_download microservice, and authentication was failing because the bearer token wasn't being passed.

**Solution:**
- Use existing `/api/reports/conversions/:id/url` endpoint that generates JWT tokens
- API returns URLs with bearer token already appended as query parameter
- UI simply prefixes with secure_download base URL and opens the link

**API Changes (`/api/reports/conversions/:id/url`):**
- Fixed path construction to use `dataset.name` instead of `dataset.id` or `dataset.cmg_id`
- Now returns `reports_url` and `index_url` with `?access_token={token}` already appended
- Response includes: `conversion_id`, `dataset_name`, `reports_url`, `index_url`, `bearer_token`

**UI Changes:**
- `ConversionView.vue`: Calls `getReportsUrl()` (which calls `/api/reports/conversions/:id/url`)
- `conversion/api.js`: Added `getReportsUrl()` method
- UI constructs final URL: `${VITE_UPLOAD_API_BASE_PATH}${index_url}` (token already in index_url)

**Path Construction:**
- Uses `conversion.cmg_id` for legacy conversions, `conversion.id` for new conversions
- Uses `dataset.name` (NOT dataset ID) as the second path segment
- Final URL format: `/reports/conversions/{conversion_id}/{dataset_name}/Reports/html/index.html?access_token={jwt}`
- Example: `/reports/conversions/67efd4f32e05981ba17a8f74/20250401_LH00300_0132_B232C5VLT3/Reports/html/index.html?access_token=eyJhbGc...`

**Authentication Flow:**
1. UI calls `/api/reports/conversions/:id/url`
2. API generates JWT token with scope `download_file:conversions/{conversion_id}/{dataset_name}/Reports`
3. API returns URLs with token appended
4. UI prefixes with `VITE_UPLOAD_API_BASE_PATH` and opens URL
5. secure_download validates token and serves files

**Configuration:**
- UI gets secure_download base URL from `VITE_UPLOAD_API_BASE_PATH` environment variable
- Token is passed as query parameter: `?access_token={jwt}`

**Rationale:** 
- Properly routes report viewing through the secure_download microservice with authentication
- Maintains separation of concerns between core API (business logic) and secure_download (file serving)
- Matches actual filesystem structure: `conversions/{conversion_id}/{dataset_name}/Reports/`
- Uses existing OAuth2 token-based authentication pattern

---

## 2026-01-27 (Later)

### Report Endpoints Reorganization and Absolute Path Support

**Changes:**

1. **Endpoint Reorganization:**
   - Moved endpoints from `api/src/routes/reports.js` to `api/src/routes/reports/conversions.js`
   - Updated `reports.js` to be a simple router that mounts conversion reports subrouter
   - Routes remain the same: `/api/reports/conversions/:id/url` and `/api/reports/conversions/:id/files*`

2. **Absolute Path Implementation:**
   - Core API now constructs absolute filesystem paths using `CONVERSION_OUTPUT_DIR` environment variable
   - Path format: `{CONVERSION_OUTPUT_DIR}/{conversion_id}/{dataset_name}/Reports`
   - Example: `/opt/sca/data/conversions/67efd4f32e05981ba17a8f74/20250401_LH00300_0132_B232C5VLT3/Reports`
   - Token scope now contains the absolute path instead of relative path
   - URLs returned include absolute path: `/reports/{absolute_path}/html/index.html?access_token={jwt}`

3. **secure_download Updates:**
   - Updated to expect absolute paths in token scope
   - No longer prepends `baseDir` - uses token path directly
   - Simplified path resolution since paths are already absolute
   - Pattern changed from `/reports/conversions/{conversion_id}/{dataset_name}/Reports/*` to `/reports/{absolute_path}/*`

**Configuration:**
- `CONVERSION_OUTPUT_DIR` environment variable:
  - Docker: `/opt/sca/data/conversions` (default)
  - Production: Set via env var or config
  - Falls back to `config.conversion.output_dir` if not set

**Rationale:**
- secure_download no longer needs to extrapolate filesystem paths
- Paths work correctly in Docker container context (not host paths)
- Cleaner separation: core API knows about filesystem structure, secure_download just serves files
- More flexible: can support different storage locations without changing secure_download

---

## 2026-01-27 (Final)

### API Endpoint Consolidation

**Changes:**

1. **Primary Endpoint:**
   - UI now calls `/api/conversions/:id/reports` (primary endpoint)
   - Added this endpoint to `api/src/routes/conversions/index.js`
   - This endpoint generates tokens and returns URLs with absolute paths
   - Removed `bearer_token` field from response (token is already in URLs)

2. **Deprecated Endpoint:**
   - `/api/reports/conversions/:id/url` remains in `api/src/routes/reports/conversions.js` for backwards compatibility
   - Will be removed in future version

3. **UI Updates:**
   - `ConversionView.vue`: Changed from `getReportsUrl()` to `getReports()`
   - `conversion/api.js`: Removed `getReportsUrl()` method (kept only `getReports()`)
   - Both methods now call `/api/conversions/:id/reports`

**Response Format (simplified):**
```json
{
  "conversion_id": "67efd4f32e05981ba17a8f74",
  "dataset_name": "20250401_LH00300_0132_B232C5VLT3",
  "reports_url": "/reports/{absolute_path}?access_token={jwt}",
  "index_url": "/reports/{absolute_path}/html/index.html?access_token={jwt}"
}
```

**Rationale:**
- Consolidates report URL generation under conversions endpoint
- Cleaner API structure: conversions-related functionality under `/conversions`
- Removed redundant `bearer_token` field (already embedded in URLs)
- Simpler UI code with single endpoint

---

## 2026-01-27 (HTML Token Propagation)

### Cross-Domain HTML Resource Authentication

**Problem Identified:**
- UI opens reports in new tab on different domain (`cmg3.sca.iu.edu` vs `cmg-test.sca.iu.edu`)
- Initial HTML request includes token in query string: `index.html?access_token=xyz` (✓ works)
- Browser loads HTML, then requests nested resources without token: `tree.html`, `style.css` (✗ 401 errors)
- Cookie-based auth won't work across different domains (same-origin policy)

**Solution Implemented:**
- HTML Token Propagation in secure_download service
- When serving HTML files, service reads content and rewrites it in-memory
- Injects authentication token into all relative resource URLs (href, src attributes)
- Skips absolute URLs, data URIs, and protocol-relative URLs
- Original files remain untouched (read-only filesystem mount)

**Implementation Details:**
- Location: `secure_download/src/routes/reports.js`
- Applies only to `.html` files
- Requires token in query string (`?access_token=` or `?token=`)
- Uses regex to match and modify `href=""` and `src=""` attributes
- Adds token as query parameter to relative URLs

**Technical Rationale:**
- Generic solution for cross-domain HTML file serving with token auth
- No filesystem modifications (read-only mount at `/opt/sca/data:ro`)
- Keeps secure_download decoupled from business logic
- Reusable pattern for any HTML content requiring token propagation

**Example Transformation:**
```html
<!-- Original HTML (read from disk) -->
<link rel="stylesheet" href="style.css">
<script src="script.js"></script>
<a href="tree.html">Tree</a>

<!-- Modified HTML (sent to browser) -->
<link rel="stylesheet" href="style.css?access_token=xyz">
<script src="script.js?access_token=xyz"></script>
<a href="tree.html?access_token=xyz">Tree</a>
```

**Result:**
- All resources (CSS, JS, images, nested HTML) include authentication token
- Browser successfully loads complete HTML page with all resources
- Works across different domains

---

## 2026-02-22

### Conversion-Derived Data Products Tracked via dataset_hierarchy

**Change:** Parent-child relationships for conversion-derived data products now use `dataset_hierarchy` table with metadata instead of separate `conversion_derived_dataset` table.

**Architecture Decision:**
- `dataset_hierarchy.metadata.derivation_method` tracks how a data product was derived:
  - `'conversion'`: Created by a conversion pipeline (e.g., bcl2fastq, cellranger)
  - `'manual_assignment'`: Manually created/uploaded by user or linked manually
- Eliminates redundant tracking - all parent-child relationships in single table
- Simplifies queries: no need to join multiple tables to get complete derivation information

**Database Schema:**
- `dataset_hierarchy` table: Added `metadata` JSON column
- `conversion_derived_dataset` table: Removed (functionality merged into dataset_hierarchy)
- Migration: `20260222225751_refactor_conversion_derived_to_hierarchy_metadata`

**API Changes:**
- `POST /datasets/associations`: Defaults `metadata.derivation_method` to `'manual_assignment'` if not provided
- `GET /conversions/:id/derived_datasets`: Now queries `dataset_hierarchy` table filtered by `derivation_method='conversion'`
- Removed: `POST /conversions/derived_datasets` endpoint (no longer needed)
- `dataset` service: Simplified derived datasets logic - reads directly from `dataset_hierarchy.metadata`

**Worker Changes:**
- `derive_data_products` task: Creates dataset_hierarchy with `metadata: { derivation_method: 'conversion' }`
- Removed: `api.post_conversion_derived_datasets()` function (no longer needed)
- `buildDatasetCreateQuery`: Defaults to `'manual_assignment'` when creating hierarchies via `src_dataset_id` parameter

**Bigbang Migration:**
- `sync_dataset_hierarchies.js`: Determines derivation_method based on CMG `dataproduct.conversion` field
  - If `dataproduct.conversion` exists → `'conversion'`
  - Otherwise → `'manual_assignment'`
- `sync_conversions.js`: No longer creates `conversion_derived_dataset` records
- Tracks derivation method counts in migration logs

**UI Changes:**
- No changes required: `AssocDatasetList.vue` already reads `derivation_method` from datasets_meta
- Removed: `include_derived_datasets` parameter from conversion API calls (no longer supported)

**Query Pattern for Conversion-Derived Datasets:**
```javascript
// Old pattern: separate table
const derivedDatasets = await prisma.conversion_derived_dataset.findMany({
  where: { conversion_id }
});

// New pattern: dataset_hierarchy with metadata filter
const conversion = await prisma.conversion.findUnique({ where: { id } });
const derivedDatasets = await prisma.dataset_hierarchy.findMany({
  where: {
    source_id: conversion.dataset_id,
    metadata: { path: ['derivation_method'], equals: 'conversion' }
  }
});
```

**Rationale:**
- Single source of truth for parent-child relationships
- More flexible metadata tracking (can add additional fields as needed)
- Simpler schema with fewer junction tables
- Consistent with Bioloop's approach of using metadata for extensibility

---

## Future Entries

Add entries here as decisions are made, changes are implemented, or issues are resolved.

Format:
```
## YYYY-MM-DD

- Decision: [What was decided]
- Constraint: [New constraint or limitation]
- Clarification: [Behavior clarification]
- Change: [What changed and why]
```

---

**Last Updated:** 2026-02-22

