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

## 2026-02-27

### Conversion metadata.origin Tracking for Derived Data Products

**Problem:** Data products derived from conversions submitted via Bioloop UI were getting `metadata.origin = 'legacy'` instead of `metadata.origin = 'bioloop'`, causing the integrated workflow inspect step to incorrectly treat them as legacy datasets (attempting to read from an extracted archive path that does not exist).

**Root Cause:** `derive_data_products.py` was propagating `origin: 'legacy'` to all derived data products whenever `config.legacy_application_active` was `True`, regardless of whether the conversion was submitted from Bioloop UI or from the legacy CMG sync scripts.

**Fix:**

1. **API layer** (`api/src/routes/conversions/index.js`):
   - `POST /conversions` (single): Now sets `metadata.origin = 'bioloop'` by default when creating the conversion record. Accepts an optional `metadata` body field to allow callers to override (e.g., future legacy poller scripts can pass `{ origin: 'legacy' }`).
   - `POST /conversions/bulk`: Same — accepts optional `metadata` body field, threads it into each `validateAndCreateConversion` call, which also defaults to `origin: 'bioloop'`.

2. **Worker layer** (`workers/workers/tasks/derive_data_products.py`):
   - Removed dependency on `config.legacy_application_active` for determining data product origin.
   - Now reads `conversion.metadata.origin` directly and propagates it to all derived data product payloads.
   - Falls back to `'bioloop'` if `metadata.origin` is absent (safety net for existing conversions created before this change that lack metadata).

**Design Decision:**
- `conversion.metadata.origin` is the source of truth for how the conversion was initiated.
- Derived data products inherit the origin of their parent conversion.
- Default is `'bioloop'` at the API layer; `'legacy'` will be set explicitly by the CMG poller scripts once that feature is implemented.

**TODO (not yet implemented):**
- CMG poller scripts that submit conversions to Bioloop on behalf of legacy CMG sequencing runs must pass `metadata: { origin: 'legacy' }` when calling `POST /conversions` or `POST /conversions/bulk`.

## 2026-03-01

### Bug Fix: Derived Datasets Scoped to Their Conversion

**Problem:** `GET /conversions/:id/derived_datasets` returned the same set of data products for all conversions that shared the same input dataset. All conversions had `dataset_id` pointing to the same raw dataset, so the old query (`source_id = conversion.dataset_id` + `metadata.derivation_method = 'conversion'`) returned every conversion-derived child of that dataset, regardless of which conversion produced it.

**Fix:** `dataset_hierarchy.metadata` now stores `{ conversion_id }` instead of `{ derivation_method }`. The query filters by `metadata.conversion_id = conversionId`, which uniquely identifies the producing conversion.

**`derivation_method` removed entirely:** The field was redundant with `dataset.create_method` (values `'conversion'` ↔ `CONVERSION`, `'manual_assignment'` ↔ everything else). All reads/writes of `metadata.derivation_method` have been removed. The "Derivation Method" column in `AssocDatasetList.vue` now reads `rowData.create_method` from the already-fetched dataset objects.

**Bigbang step order changed:** `syncDatasetHierarchies` moved from step 12 to step 14 (after `syncConversions`) so bioloop `conversion.id` is available when writing hierarchy metadata.

**Files changed:**
- `workers/workers/tasks/derive_data_products.py`
- `api/src/routes/conversions/index.js`
- `api/src/services/dataset.js`
- `api/src/routes/datasets/index.js`
- `data_sync/src/sync/bigbang/sync_dataset_hierarchies.js`
- `data_sync/src/bigbang_cmg_sync.js`
- `ui/src/components/dataset/AssocDatasetList.vue`

## 2026-03-03

### Access Control Audit (pre-emptive — user role not yet granted conversion access)

**Context:** During the sessions/tracks access-control overhaul, the Conversions feature was audited for the same class of issues. User role currently has **zero grants** on the `conversion` resource, so every API route returns 403 and every UI page is guarded with `requiresRoles: ["operator", "admin"]`. No active vulnerabilities exist.

**Latent issues — must be resolved before granting any conversion grants to user role:**

- **Issue A — `GET /conversions` collection endpoint has no user scoping.** The `where` clause is built entirely from request params (`dataset_id`, `definition_name`, `initiator`, etc.). There is no code path that enforces `initiator_id = req.user.id` when the requester is user role. A user with `read:own` would see every conversion in the DB unless they pass their own `initiator` filter (which is not enforced server-side).

- **Issue B — `GET /conversions/:id` has no `conversion_access_check` middleware.** Calls `prisma.conversion.findUniqueOrThrow({ where: { id } })` with no ownership verification. Identical in structure to the gap fixed in tracks/sessions during this overhaul. Needs a `conversion_access_check` middleware (mirroring `datasetService.dataset_access_check`) that verifies the conversion's source dataset is in one of the requesting user's projects.

- **Issue C — `GET /conversions/:id/logs`, `/:id/reports`, `/:id/derived_datasets` share the same gap as Issue B.** All three route directly to `req.params.id` without ownership verification.

- **Issue D — `POST /conversions` has a TODO but no implementation.** An existing comment reads: `// TODO: users should be able to create conversions if they have access to the dataset`. When this is implemented, the route must verify the `dataset_id` in the request body is accessible to the requesting user via project membership (same check as `datasetService.dataset_access_check`).

**Correct implementation pattern when user access is added** (see `api_conventions.md` Access Control Pattern section):
- Grant `read:own`, `create:own` (not `:any`) to user role
- `GET /conversions/:username/all` — add this route with `isPermittedTo('read', { checkOwnership: true })`, filtering by `initiator_id = targetUser.id`
- `GET /conversions/:id` — replace `isPermittedTo('read')` with `conversion_access_check` middleware
- `POST /conversions` — add dataset ownership check (use `has_dataset_assoc` or equivalent)
- UI stores/components — route to `/:username/all` for user role, general endpoint for admin/operator

## 2026-03-04

### Platform-Based Execution Feature Flag

**Decision:** The platform-based execution feature (SLURM/external cluster submission for conversions) is now gated behind a feature flag, disabled by default.

**Feature flag keys:**
- API (`api/config/default.json`): `enabled_features.platform_based_execution` (boolean, default `false`)
- UI (`ui/src/config.js`): `enabledFeatures.platformBasedExecution` (boolean, default `false`)
- Workers (`workers/workers/config/common.py`): `enabled_features.platform_based_execution` (boolean, default `False`)

**Behavior when disabled:**
- UI: Step 2 ("Execution Platform") is hidden from the `ConversionForm` stepper; `executionMetadata` is always reset to `{}`
- API: `process_requests` in `POST /conversions` and `POST /conversions/bulk` are silently ignored; no `process_request` or `process_artifact` DB records are created
- Workers: `convert_genomic` task always runs the conversion locally, ignoring any `process_request` records already present on the conversion

**Behavior when enabled:**
- Identical to prior behavior: Step 2 visible, `process_requests` processed by API, worker dispatches to SLURM if a `process_request` is found

**Files changed:**
- `api/config/default.json`
- `api/src/routes/conversions/index.js`
- `ui/src/config.js`
- `ui/src/components/conversions/ConversionForm.vue`
- `workers/workers/config/common.py`
- `workers/workers/tasks/convert_genomic.py`

## 2026-03-04 (Status Column)

### Status Column Added to Conversions List View

**Change:** A Status column was added to the `/conversions` list view (`ConversionList.vue`), consistent with the pattern used in Dataset Uploads and Dataset Imports history tables.

**Implementation:**
- `GET /conversions` now called with `include_workflow_status=true`, which fetches the live `workflow_status` (raw Rhythm status) for each conversion's `convert_genomic` workflow via the existing API support.
- `workflow_status` is normalized to display states: `ACTIVE` (PENDING/STARTED), `SUCCESS`, or `FAILURE` (all other terminal states).
- Icons: animated spinner (warning color) for active, `check_circle` (success) for completed, `warning` (warning color) for failed — each wrapped in a `va-popover` tooltip.
- Polling: `useIntervalFn` (interval from `config.dataset_polling_interval`) re-fetches the list while any conversion on the current page has an active workflow status; pauses automatically when none are active.
- Column layout adjusted: `status` (8%), `dataset` (flexible/no width), `program` (20%), `initiated on` (15%), `initiator` (15%).
- Legacy conversions (`conversion.metadata.origin === 'legacy'`) always show the success icon regardless of `workflow_status`, since they were completed in the legacy CMG system before Bioloop workflow tracking existed.

## 2026-03-05

### Convert Button Added to /conversions View; Dataset Picker Added to BulkConversionModal

**Change 1 — ConversionList.vue:**
- A Convert button (matching the styling and placement of the one in `DatasetList.vue`) has been added to the `/conversions` list view toolbar, positioned before the Filters button.
- Clicking Convert directly opens `BulkConversionModal` with `allowRawDataSelection=true` (no row-selection phase needed, since the conversions list does not list raw datasets).

**Change 2 — BulkConversionModal.vue:**
- New prop: `allowRawDataSelection` (Boolean, default `false`).
- When `true`, a **Dataset** field (backed by `DatasetSelectAutoComplete` filtered to `datasetType="RAW_DATA"`) is rendered at the top of Step 1 inside `ConversionForm` (above the pipeline/definition selector), not outside the stepper.
- `effectiveDatasetIds` computed replaces `props.datasetIds` everywhere internally:
  - `allowRawDataSelection=true`: `[selectedRawDataset.id]` (or `[]` if none selected)
  - `allowRawDataSelection=false`: `props.datasetIds` (existing behavior unchanged)
- The Convert button is additionally disabled when `allowRawDataSelection=true` and no dataset has been selected.
- Dataset selection state (`selectedRawDataset`, `datasetSearchTerm`) is reset on modal close.
- Removed leftover debug `console.log` statements from `watch(execution_metadata)` and `onMounted`.

**Files changed:**
- `ui/src/components/conversions/ConversionList.vue`
- `ui/src/components/dataset/BulkConversionModal.vue`
- `ui/src/components/conversions/ConversionForm.vue`

## 2026-04-01

### QC/MultiQC Reports: Unified Storage, Direct Serving, Token Removal

**Problem:** Legacy QC/MultiQC reports (FastQC + MultiQC per data product) lived at `/N/project/CMG-SCA/production/qc/<conversion_cmg_id>/<dataproduct_name>/` but were not viewable in the Bioloop UI. Future conversions wrote QC to a different location (`<conversion_output_dir>/qc/fastqc/`). The existing "View Reports" (bcl2fastq Reports/) used a token-in-URL approach via the secure_download microservice.

**Architecture Decisions:**

- Decision: All conversion QC/MultiQC reports (legacy and future) share a single base directory: `/N/scratch/cmguser/cmg-bioloop/conversions/qc_reports/`
- Decision: Directory structure is `<conversion_identifier>/<dataset_name>/` where identifier is `cmg_id` for legacy conversions, `conversion.id` for new ones
- Decision: Mount the conversions directory (`/N/scratch/cmguser/cmg-bioloop/conversions/`) read-only in the API container at `/opt/sca/data/conversion_reports`
- Decision: API serves QC reports and bcl2fastq Reports directly via cookie auth (same-origin JWT), eliminating the need for the secure_download token approach
- Decision: Full filesystem paths are never exposed in URLs; the API maps conversion ID + dataset name to the filesystem internally
- Decision: The standard bioloop `qc.py` task has an opt-in config flag (`genomic_conversion.qc.use_conversion_dirs`) that, when enabled, writes QC to the conversion-specific directory instead of the standard bioloop QC path. Enabled in CMG production config.
- Decision: `derive_data_products.py` now stores `conversion_id` and `conversion_cmg_id` in each data product's `metadata`, allowing `qc.py._generate_qc` to resolve the correct output directory without extra API calls
- Decision: Bigbang adds a new step (16/19) that copies legacy QC report dirs from `/N/project/CMG-SCA/production/qc/` to the unified scratch location, following symlinks (copying actual files, not symlinks)

**Worker Changes:**
- `workers/workers/config/common.py`: Added `paths.conversion.qc_reports` and `genomic_conversion.qc.use_conversion_dirs` (default `False`)
- `workers/workers/config/production.py`: Set `qc_reports` path and enabled `use_conversion_dirs`
- `workers/workers/conversion.py`: Added `get_conversion_qc_reports_dir()` helper
- `workers/workers/tasks/qc.py`: Added `_resolve_qc_dir()` that routes to conversion-specific dir when config is enabled and dataset has conversion metadata
- `workers/workers/tasks/conversion_qc.py`: Updated to use unified QC reports dir when `use_conversion_dirs` is enabled
- `workers/workers/tasks/derive_data_products.py`: Stores `conversion_id` and `conversion_cmg_id` in data product metadata

**Bigbang Changes:**
- New module: `data_sync/src/sync/cmg/bigbang/sync_qc_reports.js` — copies legacy QC dirs
- Added as step 16/19 in both `bigbang_cmg_sync.js` and `bigbang_sync.js`
- Config: `cmg.legacyQcReportsDir` (source), `cmg.qcReportsTargetDir` (target)

**Docker Changes:**
- `docker-compose-prod.yml`: Added `${CONVERSION_REPORTS_HOST_DIR}:${CONVERSION_REPORTS_MOUNT_DIR}:ro` volume mount to API service

**API Changes:**
- `api/config/default.json`: Added `conversion.reports_base_dir`
- New routes in `api/src/routes/conversions/index.js`:
  - `GET /:id/qc-reports` — lists available QC report directories per conversion
  - `GET /:id/qc-reports/:datasetName/*` — serves QC report files (cookie auth)
  - `GET /:id/conversion-reports/*` — serves bcl2fastq report files (cookie auth, replaces token approach)
- Path traversal prevention on all file-serving routes

**UI Changes:**
- `ConversionView.vue`: "Reports" row split into "Conversion Reports" and "QC Reports"
  - Conversion Reports: opens bcl2fastq Reports via direct API route (no token)
  - QC Reports: fetches available report dirs from API, renders a button per data product that opens MultiQC report
- `conversion/api.js`: Added `getQcReports(id)` method

**Token approach removed:** The old `/:id/reports` token-generation endpoint and `api/src/routes/reports/conversions.js` have been deleted. The `reports/index.js` router no longer mounts the conversions sub-router. All report serving now goes through the direct cookie-auth routes above.

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

**Last Updated:** 2026-04-01

