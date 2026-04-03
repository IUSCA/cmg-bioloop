# CMG Database Migration to Bioloop

**Feature Scope:** Migrating CMG's MongoDB database to Bioloop's PostgreSQL database with ongoing synchronization.

**Status:** In Progress

**Related Documentation:**
- `/CMG_BIOLOOP_DATABASE_SYNC---POLLING.md`
- `/data_sync/` directory
- `/CMG_SYNC_IMPLEMENTATION_PROGRESS.md`
- `.ai/features/migration-documentation-checklists.md` (CMG + Cross-App sections)

---

## 2026-01-16 (Major Refactor & Production Readiness)

### Architecture Changes

- **Change:** Migration scripts relocated from `api/src/scripts/cmg_sync/` to dedicated `data_sync/` directory
  - Creates isolated microservice for sync operations
  - Separate Docker container (`db_sandbox`) with isolated PostgreSQL instance
  - Independent from main application lifecycle
  
- **Decision:** Sync scripts use main app's Prisma schema (`api/prisma/schema.prisma`)
  - Single source of truth for database schema
  - No duplicate schema definitions
  - `data_sync/package.json` points to `../api/prisma`

- **Decision:** Isolated sandbox database for safe testing, with flexible production targeting
  - Default: writes to isolated `bioloop_sync` database in `db_sandbox` container
  - Production: `--target-db=app` reads `api/.env` and writes to production database
  - Custom: `--target-db=custom` uses `DATABASE_URL` environment variable

### Docker & Infrastructure

- **Change:** All Docker commands updated to `docker compose` (v2 syntax)
  - Reflects modern Docker Compose standalone binary
  - Updated in all markdown documentation

- **Decision:** `db_sandbox` container runs on isolated Docker network
  - Prevents accidental interference with main application containers
  - Production: connects to `bioloop_network` for access to app's postgres
  - Localhost: connects to `cmg-bioloop-2_default` for access to app's postgres

- **Change:** Container memory limits configured to prevent OOM errors
  - `db_sandbox`: 8GB limit, 4GB reservation
  - Node.js: `--max-old-space-size=6144` (6GB heap)

### Idempotency & Data Integrity

- **Fix:** All `findUnique()` calls using non-unique fields changed to `findFirst()`
  - Affected: `cmg_id` lookups across 12+ files (sync modules, pollers)
  - Root cause: `cmg_id` is not defined as unique constraint in Prisma schema
  - Prevention: queries now work correctly without schema modification

- **Decision:** All bigbang sync modules are fully idempotent
  - Seed constants: check for existing roles, users, programs before creating
  - Datasets: check by `cmg_id` and `name` before inserting
  - Projects: pre-check by `cmg_id` and `slug` before batch insertion
  - Conversions: check `cmg_id` before creating conversion and derived dataset links
  - Sessions: check `cmg_id` before creating genome browser sessions
  - Import logs: check for existing logs by `cmg_id` before insertion
  - Dataset hierarchies: check for existing parent-child relationships

- **Fix:** Bigbang can be safely re-run without creating duplicates
  - Removed misleading "migration has been rolled back" error message
  - Data persists across failed runs (partial progress is retained)
  - Re-running continues from last successful step

### Memory Optimization

- **Fix:** "JavaScript heap out of memory" errors resolved
  - Replaced `.toArray()` with MongoDB cursor streaming: `for await (const doc of cursor)`
  - Implemented batch processing: 50 datasets per batch
  - Added `global.gc()` hints after batch processing
  - Cross-batch duplicate checking via database queries (not in-memory sets)

- **Clarification:** Large collections processed incrementally
  - Datasets: ~2000 items streamed and batched
  - Audit logs: ~7000+ events streamed
  - Memory footprint now constant regardless of collection size

### Database Targeting & Configuration

- **Change:** Dynamic database URL construction via `src/utils/db_config.js`
  - `setDatabaseUrl('sandbox')`: uses default sandbox credentials
  - `setDatabaseUrl('app')`: reads `api/.env` and constructs URL
  - `setDatabaseUrl('custom')`: uses `process.env.DATABASE_URL` as-is

- **Decision:** Sandbox database uses configurable credentials
  - Default: `appuser` / `example` / `bioloop_sync`
  - Configured via `data_sync/.env.default` and `data_sync/.env`
  - PostgreSQL initialization handled by `bin/entrypoint.sh`

- **Change:** `api/.env` mounted read-only into `db_sandbox` container
  - Path: `../api/.env:/opt/sca/api/.env:ro`
  - Enables `--target-db=app` to work in production
  - No credentials duplicated across `.env` files

### Security & Operational Safety

- **Fix:** MongoDB passwords no longer logged or displayed
  - Implemented `sanitizeUri()` function to mask credentials
  - Applied to all `logger.info()`, `logger.error()`, and error messages
  - Format: `mongodb://username:***@host:port/database`

- **Decision:** Rhythm MongoDB references completely removed
  - `RHYTHM_MONGO_*` environment variables deleted
  - `buildMongoUri()` no longer accepts `rhythm_mongodb` config
  - Workflow Status Poller removed (was only poller using Rhythm)
  - Sync scripts only interact with CMG MongoDB and Bioloop PostgreSQL

- **Constraint:** Sync scripts do not write to `workflow` table or Rhythm MongoDB
  - Verified: no `prisma.workflow.create()` calls in sync code
  - Verified: no Rhythm MongoDB connection in sync scripts
  - Workflow management remains separate concern

### Schema & Data Model

- **Change:** `genome_browser_session.cmg_id` field added
  - Type: `String?` (nullable varchar(100))
  - Purpose: idempotency and provenance tracking
  - Enables poller to sync session metadata changes

- **Decision:** Audit logs created only for completed events
  - CMG events like "Stage - start" and "Stage - finish" become single "staged" audit log
  - Mapped via `parseCMGEventToAction()` function
  - Prevents duplicate audit logs for workflow lifecycle events

### Import Logs & Dataset Hierarchies

- **Fix:** Import logs migration corrected to understand CMG upload model
  - CMG uploads *create* new dataproducts (not reference existing ones)
  - Reverse lookup: find `dataproducts` where `dataproduct.upload == cmgUpload._id`
  - One upload can create multiple dataproducts (one-to-many)
  - Result: 2,054 import logs created (was 0)

- **Fix:** `undefined` values filtered before Prisma insert
  - Prisma 5.20.0 rejects explicitly undefined values
  - Implemented pattern to filter out undefined while preserving null/0/false
  - Applied to import logs metadata field

- **Change:** Import log metadata now includes comprehensive provenance
  - `cmg_upload_id`: original CMG upload ID
  - `cmg_dataproduct_id`: dataproduct created by this upload
  - `cmg_source_dataset`: upload.dataset reference (lineage)
  - `cmg_source_dataproduct`: upload.dataproduct reference (lineage)
  - `cmg_upload_data`: filtered original upload data

- **Fix:** Dataset hierarchies detailed logging for skipped records
  - Result: 5,279 hierarchies created (was 0)
  - Skip reasons tracked: no parent, no child, already exists

### Conversions Migration

- **Fix:** Conversion schema mapping corrected
  - CMG fields not in Bioloop schema moved to `additional_args` JSON
  - `source_dataset_id` → `dataset_id`
  - `created_at` → `initiated_at`
  - Non-existent fields: `name`, `status`, `output_directory`, `log_file_path` → `additional_args`

- **Fix:** Conversion arguments correctly parsed from CMG format
  - CMG stores options without `--` prefix: `['barcode-mismatches 0']`
  - Parser adds `--` prefix and splits space-separated key-value pairs
  - Handles both `key=value` and `key value` formats
  - Result: historical conversion arguments preserved in `additional_args`

- **Decision:** Conversion arguments migrated to match current Bioloop workflow behavior (2026-01-19)
  - **Predefined arguments** → `argument_values` table (linked to `argument` definitions)
  - **Ad-hoc arguments** → `additional_args` JSON field
  - **Sample sheets** → `argument_values` table (linked to `--sample-sheet` argument)
  - Implementation: `sync_conversions.js` parses CMG's `options` array at "wildcards" separator
  - Before "wildcards": predefined args matched against `argument` definitions, create `argument_values` records
  - After "wildcards": ad-hoc args stored in `additional_args` JSON
  - CMG's `samplesheet` field → `argument_values` record for `--sample-sheet` argument
  - Ensures legacy conversions have same data structure as new Bioloop conversions

- **Change:** Argument value validation during migration (2026-01-19)
  - Bigbang checks CMG argument values against `allowed_values` constraint in `argument` table
  - Example: `--barcode-mismatches` must be in `['0', '1', '2']`
  - If value is outside allowed range, logs detailed warning to bigbang log:
    - Conversion CMG ID, Dataset CMG ID, Pipeline name
    - Argument name, invalid value, allowed values list
    - Note that value will be migrated as-is but may fail Bioloop validation
  - Invalid values are still migrated (not skipped) to preserve historical data
  - Allows manual review and correction after migration

- **Fix:** Conversions now fully migrated (2,732 records)
  - Idempotency check by `cmg_id` before creation
  - Derived dataset links checked before creation
  - No duplicate conversions on re-runs

### Conversion Logs (Historical)

- **Decision:** Conversion logs migrated from CMG filesystem to Bioloop database
  - Source: `/N/project/CMG-SCA/production/runlogs/convert_{dataset_name}.log`
  - Target: `worker_process` and `log` tables
  - Step 11 in bigbang migration order

- **Change:** Log parsing and inference implemented
  - Multiple conversions append to same log file (separated by "logfile header")
  - Log level inferred from message content (ERROR, WARN, INFO, DEBUG)
  - Timestamp extracted from log lines when present
  - Section matching by conversion creation time (heuristic)

- **Constraint:** Conversion logs migration only works in production
  - Requires access to `/N/project/CMG-SCA/production/runlogs`
  - Gracefully skips if directory not accessible (local dev)
  - Can be skipped with `--skip-conversion-logs` flag

- **Change:** Worker process metadata includes migration provenance
  - `tags.cmg_conversion_id`: original CMG conversion ID
  - `tags.cmg_dataset_name`: dataset name
  - `tags.cmg_log_file`: source log file name
  - `tags.migration_note`: indicates historical migration

### Pipeline Definitions & Executables

- **Change:** Conversion binary paths updated to production values
  - `bcl2fastq`: `/usr/local/bin/bcl2fastq`
  - `bcl-convert`: `/usr/bin/bcl-convert`
  - `cellranger-*`: `/N/project/CMG-SCA/bin/cellranger-*/cellranger`
  - `spaceranger-*`: `/N/project/CMG-SCA/bin/spaceranger-*/spaceranger`

- **Change:** `executable_directory` correctly handles empty strings
  - Empty strings converted to `null` for Prisma `String?` type
  - Prevents validation errors on upsert

- **Decision:** Pipeline definition seeding now updates existing records
  - Previous: skipped if `cmd_line_program` already existed
  - Current: upserts to update paths and properties if they differ
  - Ensures changes in `src/sync/constants.js` propagate to database

- **Change:** Conversion definitions use shared output directory
  - Path: `/N/scratch/cmguser/cmg-bioloop/conversions/output`
  - Consistent with CMG production environment

### Logging & Observability

- **Change:** Logs persist to host filesystem
  - Container path: `/tmp/bigbang_sync_*.log`, `/tmp/poller_sync_*.log`
  - Host mount: `data_sync/logs/` (via volume mount)
  - Production: `/tmp/data_sync_logs/` on host

- **Change:** Winston logger configured for sync-specific paths
  - Timestamped log files with daily rotation
  - Format: `bigbang_sync_YYYY-MM-DDTHH-MM-SS.log`
  - Combined (all levels) + error-specific logs

- **Tool:** `bin/pull_logs_from_prod.sh` script created
  - SSH to production host and download logs via SCP
  - Remote path: `/tmp/data_sync_logs/` on production host
  - Local output: `./logs_from_prod/` by default
  - Options: `--last N` (default 1), `--all`, `--list`, `--host`, `--user`, `--output`
  - Sorts by timestamp (newest first)
  - Disables SSH port forwarding conflicts
  - Full documentation: `data_sync/LOGS.md`

- **Change:** Bigbang tracks and reports logging statistics
  - Wrapper counts all `logger.info/warn/error/debug()` calls
  - Final message shows total log statements and formatted runtime
  - Helps estimate log volume and migration performance

### Process Management & Locking

- **Decision:** Bigbang and pollers use separate process locks
  - Lock names: `bigbang`, `poller`
  - Prevents concurrent runs of same process
  - Allows bigbang and poller to run simultaneously (different processes)

- **Fix:** Lock operations survive `--clear-target-db`
  - Sync infrastructure tables excluded from truncation:
    - `cmg_sync_process_lock`
    - `cmg_sync_cursor`
    - `cmg_sync_retry`
  - Prevents "Record to update not found" errors during lock extension

- **Change:** Bigbang checks if pollers are running before starting
  - Exits with warning if poller lock detected
  - Prevents data inconsistencies during migration
  - User must stop pollers before bigbang

- **Change:** Success/failure messages include poller restart instructions
  - Success: guides user to start pollers with `bin/start_pollers.sh`
  - Failure: warns user to check state before restarting pollers

### Cursor Initialization & Edge Cases

- **Fix:** Critical data loss bug in cursor initialization resolved
  - Previous: `last_cmg_objectid` set to `null` during initialization
  - Issue: documents updated *during* bigbang with same `updatedAt` timestamp would be skipped by pollers
  - Solution: retrieve and store actual `_id` of latest document per collection
  - Poller query: `updatedAt > cursor OR (updatedAt = cursor AND _id > objectId)`
  - Result: ensures eventual consistency even for concurrent updates

- **Clarification:** Bounded window prevents "moving target" problem
  - Poller query: `updatedAt > lastCursor AND updatedAt <= roundEnd`
  - `roundEnd` set at polling cycle start
  - Documents updated *during* polling cycle deferred to next cycle
  - Prevents infinite loop of chasing new updates

### Database Clearing

- **Change:** `--clear-target-db` flag implemented for clean slate migrations
  - Truncates all tables except `_prisma_migrations` and sync infrastructure
  - Uses `TRUNCATE TABLE ... RESTART IDENTITY CASCADE`
  - No superuser privileges required (no `session_replication_role` modification)
  - Identity sequences reset automatically

### User Interface & Workflow Scripts

- **Change:** Interactive `bin/migrate.sh` wrapper script created
  - Uses `bash select` for arrow-key navigation menu
  - Options:
    1. Populate database only (bigbang)
    2. Populate database + start pollers (bigbang → pollers)
    3. Start pollers only (skip bigbang) - with warnings
  - Comprehensive in-script help and examples
  - Passes arguments to underlying scripts

- **Change:** `bin/bigbang.sh` wrapper created
  - Dedicated entry point for bigbang migration
  - Argument passthrough
  - Detailed help documentation

- **Change:** `bin/start_pollers.sh` wrapper created
  - Dedicated entry point for poller sync
  - Argument passthrough
  - Detailed help documentation

- **Change:** `bin/README.md` created with usage examples and troubleshooting

### Bundle Population (Standalone)

- **Decision:** Bundle population implemented as standalone script (NOT in bigbang)
  - Location: `data_sync/populate_bundles.js`
  - Runs directly on host where HSI is available
  - Not part of bigbang/poller process (separate utility)

- **Constraint:** Bundle script requires HSI binary and HPSS authentication
  - Must run on production host with HSI installed
  - Requires Kerberos ticket or HPSS credentials
  - Local HSI execution (no SSH)

- **Decision:** Bundle script only populates `bundle` table
  - No workflow triggering
  - No dataset field updates (e.g., `is_staged`)
  - Simpler than Python version (`workers/scripts/populate_bundles.py`)

- **Change:** Bundle script supports dry-run and batch processing
  - `--dry-run`: show what would happen without changes
  - `--limit=N`: process only N datasets (testing)
  - `--target-db`: sandbox/app/custom (same as bigbang)
  - Idempotent: skips existing bundles

- **Clarification:** Bundles only needed for legacy archived datasets
  - New datasets archived through Bioloop get bundles automatically
  - Bundle population can be run separately after bigbang
  - Not required for initial migration (can be deferred)

- **Safety:** Bundle script is standalone only
  - NOT called by bigbang or poller scripts
  - NOT part of any automated workflow or container startup
  - Must be manually executed by operator
  - Only runs when invoked directly: `node populate_bundles.js`
  - Requires HSI access (not available in most environments)

### Migration Order (Current: 13 Steps)

1. Create roles
2. Create CMG system user
3. Populate pipeline definitions
4. Convert users
5. Convert datasets (RAW_DATA and DATA_PRODUCT)
6. Convert audit logs
7. Convert import logs
8. Convert dataset hierarchies
9. Convert projects
10. Convert conversions
11. Convert conversion logs (filesystem → database, production only)
12. Convert sessions (optional, often skipped)
13. Initialize cursors

### Documentation Structure

- **Change:** Documentation consolidated and refactored
  - `README.md`: high-level overview, links to detailed docs
  - `SETUP_GUIDE.md`: network isolation, environment setup
  - `BIGBANG_SYNC_USAGE.md`: one-time migration details
  - `POLLER_SYNC_USAGE.md`: continuous sync details, edge cases
  - `POPULATE_BUNDLES_USAGE.md`: bundle script usage and troubleshooting
  - `TARGET_DATABASE_CONFIGURATION.md`: database targeting options
  - `LOGS.md`: log management and retrieval
  - `bin/README.md`: script relationships and workflows

- **Change:** Network isolation warnings prominently placed
  - Docker Compose network configuration critical for safety
  - Incorrect network config can write to wrong database
  - Multiple warnings in SETUP_GUIDE.md and README.md

### Production Deployment Considerations

- **Constraint:** Production environment restrictions
  - No sudo access on `cmg-new-service1.sca.iu.edu`
  - Only `/opt/sca/cmg` writable
  - `/tmp` used for temporary operations
  - Must clean up `/tmp` files after operations

- **Decision:** Never reset database or restart Docker without explicit user permission
  - Memory: AI must ask before running migrations or restarting containers
  - Memory: AI must ask before executing `--clear-target-db`

---

## 2026-01-19

### Conversion Logs Sync - Container Execution

- **Change:** `sync_conversion_logs.sh` refactored to run inside Docker container
  - Script now automatically execs into `db_sandbox` container when run from host
  - Detects if already inside container and executes directly
  - No longer requires Node.js/dependencies on host machine
  
- **Decision:** Conversion logs sync works both standalone and as part of bigbang
  - Integrated into bigbang as step 12 (already implemented)
  - Can be run independently via `./bin/sync_conversion_logs.sh`
  - Can be skipped with `--skip-conversion-logs` flag in bigbang
  
- **Change:** Created comprehensive documentation
  - New file: `CONVERSION_LOGS_SYNC_USAGE.md`
  - Updated: `bin/README.md` with conversion logs sync details
  - Updated: `README.md` to reference new documentation
  
- **Clarification:** Script execution patterns
  - **From host:** `./bin/sync_conversion_logs.sh [options]` (auto-execs into container)
  - **Inside container:** `node /opt/sca/app/src/standalone_sync_conversion_logs.js [options]`
  - **Part of bigbang:** Automatically included unless `--skip-conversion-logs`
  
- **Change:** Container detection logic added
  - Checks for `/opt/sca/app` directory and script file
  - If inside container, runs directly without docker exec
  - If on host, validates container is running before exec
  
- **Decision:** Script remains idempotent and production-safe
  - Skips conversions with existing `workflow_id`
  - Gracefully handles missing log directory (local/dev)
  - Safe to re-run multiple times
  - `--overwrite-existing` flag for forced re-processing

### Legacy Dataset Hydration Workflow

- **Change:** Implemented `stage_migrated` workflow for legacy CMG datasets
  - Purpose: Stage and hydrate legacy datasets that were migrated from MongoDB but lack complete file/track metadata
  - Steps: begin_migration → retrieve_archive → inspect → populate_metadata → stage → validate → setup_download → end_migration
  - Automatically triggered for legacy datasets (with `cmg_id`) that haven't been hydrated yet

- **Change:** Created new workflow task files in `workers/workers/tasks/`:
  - `begin_migration.py`: Sets MIGRATION_INITIATED state
  - `retrieve_archive.py`: Downloads/copies archive bundle to staging location, verifies checksum, sets RETRIEVED state
  - `populate_metadata.py`: Populates bundle metadata from existing archive, sets METADATA_POPULATED state
  - `end_migration.py`: Sets final MIGRATED state
  - All tasks registered in `workers/workers/tasks/declarations.py`

- **Change:** Added new dataset states for migration tracking:
  - `MIGRATION_INITIATED`: Migration workflow has started
  - `RETRIEVED`: Archive bundle has been retrieved to staging
  - `INSPECTED`: Dataset files have been inspected (added to existing inspect task)
  - `METADATA_POPULATED`: Bundle and file metadata populated (hydration complete)
  - `MIGRATED`: All migration steps completed successfully

- **Change:** Updated `inspect_dataset` task to add INSPECTED state
  - Enables tracking of inspection completion in stage_migrated workflow

- **Change:** Created legacy migration API routes under `/legacy/`:
  - `GET /legacy/migrations/datasets/:id`: Returns migration status for a dataset
    - Fields: is_legacy, is_migration_initiated, is_retrieved, is_inspected, is_metadata_populated, is_hydrated, is_validated, is_migrated
  - `GET /legacy/sessions/:id`: Returns migration status for sessions (placeholder implementation)
  - Routes mounted in `api/src/routes/index.js`

- **Change:** Created `api/src/services/legacyMigration.js`
  - Provides `getDatasetMigrationStatus()`, `getSessionMigrationStatus()`, `isMigrationInProgress()`, `hasReachedState()`
  - Checks dataset states to determine hydration/migration progress

- **Decision:** POST `/datasets/:id/workflow/stage` automatically selects correct workflow
  - If dataset has `cmg_id` AND not yet hydrated: triggers `stage_migrated` workflow
  - If dataset has `cmg_id` AND already hydrated: triggers standard `stage` workflow
  - If dataset has no `cmg_id`: triggers standard `stage` workflow
  - Returns 409 error if migration already in progress

- **Change:** Created `ui/src/services/legacyMigration.js`
  - Client-side service for checking dataset migration status
  - Functions: `getDatasetMigrationStatus()`, `isLegacyDataset()`, `needsHydration()`, `isMigrationInProgress()`

- **Change:** Updated "Browse Files" button behavior in `ui/src/components/dataset/Dataset.vue`
  - If dataset not staged: shows modal prompting user to stage dataset
  - Modal message: "This dataset will need to be staged before its files can be viewed. Would you like to stage this dataset?"
  - Checks if migration already in progress, shows toast if so
  - Triggers appropriate workflow when user confirms

- **Change:** Created `workers/workers/legacy_migration.py`
  - Helper functions for legacy migration operations in worker context
  - Functions: `has_reached_state()`, `is_legacy_dataset()`, `is_hydrated()`, `is_migrated()`, `get_migration_status()`
  - Enables workers to check migration status when needed

- **Change:** Added `stage_migrated` workflow to configuration:
  - `api/config/default.json`: Workflow definition with description and steps
  - `workers/workers/config/common.py`: Worker-side workflow configuration
  - `api/src/constants.js`: Added STAGE_MIGRATED constant and migration states
  - `workers/workers/constants/workflow.py`: Added STAGE_MIGRATED constant

- **Decision:** Workflow reuses existing tasks (stage, validate, setup_download) where possible
  - Only new tasks are migration-specific: begin_migration, retrieve_archive, populate_metadata, end_migration
  - Maintains consistency with existing workflow patterns

- **Decision:** `retrieve_archive` step separate from `stage` step
  - retrieve_archive: downloads bundle to staging location, verifies checksum
  - stage: extracts bundle (reuses existing stage_dataset task)
  - Separation allows for cleaner state tracking and error handling

- **Decision:** Bundle metadata population isolated in dedicated step
  - `populate_metadata` step specifically for legacy dataset hydration
  - Designed to be extensible for other metadata population needs
  - Core bundle logic in separate `populate_bundle_metadata()` function

---

## 2026-01-26

### Poller Data Pollution & Field Update Cleanup

- **Problem:** All pollers were storing `cmg_sync_state: { cmg_updated_at, last_sync_time }` in entity `metadata` JSON fields
  - Polluted application data with infrastructure concerns
  - Overwrote any Bioloop-specific metadata
  - Mixed sync tracking with domain data

- **Decision:** Removed all `metadata` field updates from pollers
  - Sync tracking handled at poller level via `cmg_sync_cursor` table
  - Entity `metadata` fields now available exclusively for Bioloop application data
  - No per-entity sync state needed (cursor-based approach sufficient)

- **Change:** Stopped updating immutable and Bioloop-managed fields
  - `dataset.origin_path`: Immutable after bigbang (removed from dataset_activity_poller)
  - `dataset.file_type`: Immutable after bigbang (removed from dataset_metadata_poller)
  - `genome_browser_session.access_count`: Bioloop-managed, incremented on session access (removed from session_metadata_poller)

- **Change:** Deprecated `dataset_activity_poller.js`
  - Poller now does nothing (all dataset fields immutable or Bioloop-managed)
  - Kept for backward compatibility, but should be removed in future cleanup

- **Change:** Deleted `session_metadata_poller.js` entirely
  - File: `data_sync/src/sync/pollers/session_metadata_poller.js`
  - Updated: `data_sync/src/poller_sync.js` (removed import and instantiation)
  - Rationale: All session fields it updated were either Bioloop-managed (`access_count`) or unused (`staging_*`)

- **Change:** Removed unused `staging_*` fields from schema
  - Fields removed: `staging_requested`, `staging_completed`, `staging_requested_by`
  - Table: `genome_browser_session`
  - Rationale: CMG-specific fields, not used anywhere in Bioloop code
  - Migration required: `npx prisma migrate dev --name remove_session_staging_fields`

- **Change:** Removed `/sessions/:id/stage-datasets` API endpoint
  - File: `api/src/routes/sessions.js`
  - Rationale: Relied on removed `staging_*` fields, not part of Bioloop workflow

- **Clarification:** Current poller field updates (after cleanup)
  - `user_roles_poller`: `is_deleted`, user roles (via user_role table)
  - `project_acl_poller`: `description`, `browser_enabled`, project associations
  - `project_metadata_poller`: `name`, `description`, `browser_enabled`, `funding`
  - `dataset_metadata_poller`: `description` only (user-editable field)
  - `dataset_activity_poller`: Nothing (deprecated, consider removal)

- **Decision:** Sync tracking strategy remains cursor-based at poller level
  - `cmg_sync_cursor` table tracks: `last_updated_at`, `last_cmg_objectid` per poller
  - No per-entity tracking needed (all changed entities captured by cursor query)
  - Simpler, cleaner architecture without entity table pollution

---

## 2026-01-27

### Sessions Migration Filtering

- **Change:** Only migrate CMG sessions with non-empty tracks array
  - Check added in `sync_sessions.js`: skip if `tracks` is null, not an array, or empty
  - Rationale: Sessions without tracks are not useful in Bioloop (tracks are required for viewing)
  - Result: Reduces unnecessary session migrations and database records
  - Logged as: `"Skipping session {id}: no tracks"`

### Sessions Migration API - Hydration Status

- **Change:** `/legacy/migrations/sessions/:id` endpoint reports hydration status from metadata field
  - `is_hydrated` returns `true` if `genome_browser_session.metadata.is_hydrated` is `true`
  - `is_hydrated` returns `false` otherwise (if metadata is null, undefined, or is_hydrated is false)
  - Added `metadata` JSON field to `genome_browser_session` schema
  - Updated service: `api/src/services/legacyMigration.js`
  - Updated route comments: `api/src/routes/legacy/migrations.js`
  - Updated schema: `api/prisma/schema.prisma`
  - Note: Setting `metadata.is_hydrated` is handled separately (not part of this change)

### Session Hydration Workflow Implementation (2026-01-27)

- **Change:** Created `hydrate_session` workflow for hydrating legacy CMG sessions with tracks
  - Workflow added to both API and worker configs
  - Two-step workflow: `hydrate_tracks` → `finish_hydration`
  - Configuration: `api/config/default.json` and `workers/workers/config/common.py`
  
- **Change:** Workflow tasks created in `workers/workers/tasks/`
  - `hydrate_tracks.py`: Retrieves legacy tracks from CMG API, finds corresponding dataset_files and existing tracks in Bioloop, associates tracks with session
    - NOTE: Does NOT create tracks - tracks must already exist in database
    - Retrieves CMG session data using `cmg_api.get_session(cmg_id)`
    - Maps CMG dataproduct IDs to Bioloop datasets via `cmg_id` field
    - Finds dataset_files by filename within datasets
    - Finds existing tracks by dataset_file_id
    - Associates found tracks with session via `session_tracks` table
  - `finish_hydration.py`: Sets `session.metadata.is_hydrated = true`
  - Tasks registered in `workers/workers/tasks/declarations.py`
  
- **Change:** API endpoints for legacy migration added to `/api/src/routes/legacy/migrations.js`
  - `GET /legacy/migrations/datasets/by-cmg-id/:cmgId`: Get dataset by CMG ID
  - Existing: `GET /legacy/migrations/datasets/:id`: Get dataset migration status
  - Existing: `GET /legacy/migrations/sessions/:id`: Get session hydration status
  
- **Change:** Tracks API enhanced (`api/src/routes/tracks.js`)
  - Added `dataset_file_id` query parameter to GET `/tracks` endpoint
  - Allows filtering tracks by specific dataset file
  
- **Change:** Worker API helper functions added/updated in `workers/workers/api.py`
  - `get_session()`: Get session by ID
  - `update_session()`: Update session data
  - `update_session_tracks()`: Update tracks associated with session
  - `get_dataset_by_cmg_id()`: Find dataset by CMG ID (calls `/legacy/migrations/datasets/by-cmg-id/:cmgId`)
  - `get_dataset_file_by_name_and_dataset()`: Find dataset_file by filename and dataset
  - `get_track_by_dataset_file_id()`: Find track by dataset_file ID
  
- **Change:** Workflow constants added
  - `HYDRATE_SESSION` added to `api/src/constants.js`
  - `HYDRATE_SESSION` added to `workers/workers/constants/workflow.py`
  
- **Change:** Prisma schema updated with new `session_workflow` table
  - Created NEW `session_workflow` table (did NOT modify existing `workflow` table)
  - Fields: `session_id`, `workflow_id` (composite primary key), `initiator_id`, `created_at`
  - Relations: links sessions to workflows and users (initiators)
  - Added `session_workflows` relation to `genome_browser_session` table
  - Added `initiated_session_workflows` relation to `user` table
  - Migration required: `npx prisma migrate dev --name add_session_workflow_table`
  
- **Change:** Session workflow API endpoint added (`api/src/routes/sessions.js`)
  - `POST /sessions/:id/workflows/:wf`: Trigger workflow on session
  - Currently supports `hydrate_session` workflow
  - Validates user is session owner
  - Creates `session_workflow` record
  - Calls workflow service to create and start workflow
  - Returns workflow response
  
- **Change:** UI legacy migration service enhanced (`ui/src/services/legacyMigration.js`)
  - Fixed `getSessionMigrationStatus()` to call correct endpoint: `/legacy/migrations/sessions/:id`
  - Added `isLegacySession()`: Check if session has cmg_id
  - Renamed `sessionNeedsHydration()` to `isSessionHydrated()`: Check if session is hydrated (returns boolean)
  
- **Change:** UI session service extended (`ui/src/services/session.js`)
  - Added `hydrateSession(id)`: Trigger hydration workflow for a session
  - Calls `POST /sessions/:id/workflows/hydrate_session`
  
- **Change:** Sessions view UI updated (`ui/src/pages/sessions/[id].vue`)
  - Enhanced `handleViewInBrowser()` to check:
    1. If datasets are staged → show UnstagedDatasetsModal if not
    2. If session is hydrated (for legacy sessions) → show HydrationModal if not
    3. Show browser selection modal if everything is ready
  - Added hydration confirmation modal
  - Added `handleHydrateSession()` function to trigger hydration workflow
  - Updated `handleStagingRequested()` to also check hydration status after staging
  - Imports `legacyMigrationService` for hydration status checks
  
- **Decision:** Hydration workflow assumes tracks already exist
  - Tracks must be created before hydration workflow runs
  - Hydration only associates existing tracks with sessions
  - If no track found for a dataset_file, it's skipped with warning

---

## 2026-02-22

### metadata.origin Column Added to Business Object Tables

- **Change:** Added `metadata Json?` column to tables: `dataset`, `analysis_type`, `dataset_import_log`, `workflow`, `project`, `genome_browser_session`, `conversion`
  - Migration files created under `api/prisma/migrations/`
  - All new columns are nullable JSONB

- **Decision:** `metadata.origin` values for rows created by bigbang vs. poller scripts:
  - `'legacy'` — row was created by the bigbang migration (one-time historical CMG → Bioloop migration)
  - `'sync'` — row was created by a poller script representing a purely Bioloop-native sync event (e.g. project/user ACL sync)

- **Clarification:** The `'legacy'` origin also applies to poller-created datasets and conversions that originate from CMG:
  - When a new Upload/Import happens in CMG and the poller registers the corresponding dataset in Bioloop → `metadata.origin = 'legacy'`
  - When a new Conversion happens in CMG and the poller creates the corresponding `conversion` row in Bioloop → `metadata.origin = 'legacy'`
  - Rationale: these rows represent CMG-originated data regardless of whether they were created by bigbang or the ongoing poller

- **Change:** All `cmg_id`-based legacy detection checks replaced with `metadata.origin === 'legacy'` checks
  - Centralized in service helpers: `api/src/services/legacyMigration.js` (`isLegacyDataset`, `isLegacySession`, `isLegacyConversion`), `ui/src/services/legacyMigration.js` (`isLegacyDataset`, `isLegacySession`, `isLegacyProject`), `workers/workers/legacy_migration.py` (`is_legacy_dataset`)
  - All UI components and API routes now call these service helpers instead of checking `metadata.origin` directly

### metadata.origin Rules (Full Decision)

The `metadata.origin` field encodes how/where a business object was created:

| Value | Meaning | Set by |
|---|---|---|
| `'legacy'` | Row represents CMG-originated data (bigbang migration, watch.py on non-legacy-app paths while CMG is active, derive_data_products while CMG is active) | bigbang sync, watch.py, derive_data_products.py, future poller CMG-upload importer |
| `'bioloop'` | Row was created natively in Bioloop while CMG is still running | `datasetService.create()` (API) — set automatically when `legacy_application_active=true` and no origin is already present |
| `'sync'` | Row was created by poller sync scripts for non-CMG-data-origin events | TBD — future poller ACL/project sync rows |
| not set | CMG is no longer active (`legacy_application_active=false`) — origin tracking not needed | any code path when flag is false |

**`legacy_application_active` config flag:**
- Workers: `config['legacy_application_active']` (boolean, default `False` when key absent)
- API: `config.get('legacy_application_active')` via node-config (currently `true` in `api/config/default.json`)
- Replaces the old `legacy_migration.completed` pattern — semantically equivalent: `legacy_application_active=True` ↔ `legacy_migration.completed=False`

**watch.py origin logic:**
- `legacy_application_active=True` AND candidate path IS under an obs6-obs12 path → `origin='legacy'` (dataset originates in CMG-managed infrastructure)
- `legacy_application_active=True` AND candidate path is NOT under any obs6-obs12 path → no origin set (API will assign `'bioloop'`)
- `legacy_application_active=False` → no origin set

### TODO: Poller Script Updates Required

The following poller scripts must be updated to set `metadata.origin` on rows they create:

- [ ] **dataset poller / CMG Upload importer** — when a new CMG Upload is detected and a Bioloop dataset is created to register it, set `metadata = { origin: 'legacy' }` (confirmed: NOT yet implemented — no poller currently creates datasets from CMG Uploads)
- [ ] **conversion poller** — when creating a new `conversion` row from a CMG Conversion, set `metadata = { origin: 'legacy' }`
- [ ] **Any other poller** that creates rows in `analysis_type`, `dataset_import_log`, `workflow`, `project`, or `genome_browser_session` — set `metadata = { origin: 'sync' }` (or `'legacy'` if the row represents CMG-originated data)

---

## 2026-03-11

- Decision: CMG clear operation is source-scoped by default in data-sync workflows.
- Change: CMG bigbang moved to source-scoped clear behavior (CMG-originated rows/cursors/retries only), preserving xenium-originated data.
- Decision: Legacy clear-flag transitions were completed and later superseded by unified clear semantics.
- Change: Central orchestrator `data_sync/bin/init.sh` adopted explicit app/action flags and source-aware clear handling instead of full-database truncation semantics.

---

## 2026-03-12

- Change: Added explicit CMG poller lifecycle controls in `data_sync/bin/init.sh`: `--cmg-start-pollers`, `--cmg-stop-pollers`, `--cmg-restart-pollers` (managed background mode with PID files under `data_sync/run/`).
- Change: Added symmetric Xenium lifecycle controls in the same orchestrator for parity and reduced operator branching confusion.
- Decision: Hybrid-row handling was removed from source-scoped clear logic; CMG and Xenium are treated as isolated migration domains with no cross-source contamination path.
- Change: Removed `--cmg-run-pollers` / `--xenium-run-pollers` from `init.sh`; poller lifecycle now uses only explicit per-app start/stop/restart flags.
- Change: Replaced per-app target-db flags with a shared `--target-db` in `init.sh`; all selected CMG/Xenium actions now run against the same target database.

---

## 2026-03-13

- Fix: CMG poller startup now checks for an active CMG bigbang lock and exits early to avoid concurrent write races.
- Fix: CMG bigbang progress logging now uses a consistent `[N/18]` step counter across all phases.
- Fix: `data_sync/bin/init.sh` help output now excludes the shebang line and CMG environment warnings now align with supported CMG inputs (`--cmg-uri`, `MONGO_URI`, `CMG_MONGO_HOST`) instead of warning only on missing `MONGO_URI`.
- Change: CMG poller base now supports optional Prisma transaction timeout overrides; CMG ACL poller defaults were hardened to `batchSize=50` and `transactionTimeoutMs=30000` to reduce timeout pressure from source reads inside target transactions.
- Change: CMG bigbang now seeds the `about` table from repository-root `about_cmg` content (plain text converted to escaped `<p>` HTML blocks) and only creates a new about row when content differs from the latest existing row.

---

## 2026-04-01

- Decision: Supersedes 2026-03-11 clear-flag split: `bigbang_cmg_sync.js`, `bigbang_xenium_sync.js`, and `init.sh` now use a single `--clear-target-db` that removes all CMG- and Xenium-originated migration rows and resets both sync lock tables (see `clear_legacy_target_data.js`, `forceReleaseAllSyncProcessLocks`).

---

## 2026-04-03

- Clarification: CMG bigbang import-log sync only reads/writes `dataset.create_method`; no `dataset_audit.create_method` fallback remains in bigbang code.

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

**Last Updated:** 2026-04-03

