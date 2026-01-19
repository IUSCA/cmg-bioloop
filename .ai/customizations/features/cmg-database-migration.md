# CMG Database Migration to Bioloop

**Feature Scope:** Migrating CMG's MongoDB database to Bioloop's PostgreSQL database with ongoing synchronization.

**Status:** In Progress

**Related Documentation:**
- `/CMG_BIOLOOP_DATABASE_SYNC---POLLING.md`
- `/data_sync/` directory
- `/CMG_SYNC_IMPLEMENTATION_PROGRESS.md`

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

<<<<<<< Updated upstream
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
=======
## 2026-01-19 (Legacy Dataset Hydration Workflow)

### New Workflow: stage_migrated

- **Change:** Implemented `stage_migrated` workflow for legacy CMG datasets
  - Purpose: Stage and hydrate legacy datasets that were migrated from MongoDB but lack complete file/track metadata
  - Steps: begin_migration → retrieve_archive → inspect → populate_metadata → stage → validate → setup_download → end_migration
  - Automatically triggered for legacy datasets (with `cmg_id`) that haven't been hydrated yet

### New Workflow Tasks

- **Change:** Created new workflow task files in `workers/workers/tasks/`:
  - `begin_migration.py`: Sets MIGRATION_INITIATED state
  - `retrieve_archive.py`: Downloads/copies archive bundle to staging location, verifies checksum, sets RETRIEVED state
  - `populate_metadata.py`: Populates bundle metadata from existing archive, sets METADATA_POPULATED state
  - `end_migration.py`: Sets final MIGRATED state
  - All tasks registered in `workers/workers/tasks/declarations.py`

### State Management

- **Change:** Added new dataset states for migration tracking:
  - `MIGRATION_INITIATED`: Migration workflow has started
  - `RETRIEVED`: Archive bundle has been retrieved to staging
  - `INSPECTED`: Dataset files have been inspected (added to existing inspect task)
  - `METADATA_POPULATED`: Bundle and file metadata populated (hydration complete)
  - `MIGRATED`: All migration steps completed successfully

- **Change:** Updated `inspect_dataset` task to add INSPECTED state
  - Enables tracking of inspection completion in stage_migrated workflow

### API Routes and Services

- **Change:** Created legacy migration API routes under `/legacy/`:
  - `GET /legacy/migrations/datasets/:id`: Returns migration status for a dataset
    - Fields: is_legacy, is_migration_initiated, is_retrieved, is_inspected, is_metadata_populated, is_hydrated, is_validated, is_migrated
  - `GET /legacy/sessions/:id`: Returns migration status for sessions (placeholder implementation)
  - Routes mounted in `api/src/routes/index.js`

- **Change:** Created `api/src/services/legacyMigration.js`
  - Provides `getDatasetMigrationStatus()`, `getSessionMigrationStatus()`, `isMigrationInProgress()`, `hasReachedState()`
  - Checks dataset states to determine hydration/migration progress

### Workflow Triggering Logic

- **Decision:** POST `/datasets/:id/workflow/stage` automatically selects correct workflow
  - If dataset has `cmg_id` AND not yet hydrated: triggers `stage_migrated` workflow
  - If dataset has `cmg_id` AND already hydrated: triggers standard `stage` workflow
  - If dataset has no `cmg_id`: triggers standard `stage` workflow
  - Returns 409 error if migration already in progress

### UI Implementation

- **Change:** Created `ui/src/services/legacyMigration.js`
  - Client-side service for checking dataset migration status
  - Functions: `getDatasetMigrationStatus()`, `isLegacyDataset()`, `needsHydration()`, `isMigrationInProgress()`

- **Change:** Updated "Browse Files" button behavior in `ui/src/components/dataset/Dataset.vue`
  - If dataset not staged: shows modal prompting user to stage dataset
  - Modal message: "This dataset will need to be staged before its files can be viewed. Would you like to stage this dataset?"
  - Checks if migration already in progress, shows toast if so
  - Triggers appropriate workflow when user confirms

### Worker Utilities

- **Change:** Created `workers/workers/legacy_migration.py`
  - Helper functions for legacy migration operations in worker context
  - Functions: `has_reached_state()`, `is_legacy_dataset()`, `is_hydrated()`, `is_migrated()`, `get_migration_status()`
  - Enables workers to check migration status when needed

### Configuration

- **Change:** Added `stage_migrated` workflow to configuration:
  - `api/config/default.json`: Workflow definition with description and steps
  - `workers/workers/config/common.py`: Worker-side workflow configuration
  - `api/src/constants.js`: Added STAGE_MIGRATED constant and migration states
  - `workers/workers/constants/workflow.py`: Added STAGE_MIGRATED constant

### Architecture Notes

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
>>>>>>> Stashed changes

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

**Last Updated:** 2026-01-19

