# Legacy Data Migration (CMG + Xenium)

This document describes the current, code-validated legacy migration behavior in `data_sync`, `api`, `ui`, and `workers` for:

- CMG legacy source (MongoDB -> Bioloop PostgreSQL)
- Xenium legacy source (PostgreSQL -> Bioloop PostgreSQL)
- Ongoing poller synchronization
- Legacy/bioloop concurrent registration and archival coordination

All behavior below is verified against implementation in the repository (not only `.ai` notes).

## Purpose and Boundaries

Legacy migration has two major phases:

- **Bigbang (one-time historical load):** Backfill legacy business objects into Bioloop.
- **Pollers (continuous incremental sync):** Keep selected mutable fields and ACL mappings in sync after bigbang.

Important boundary:

- Bigbang/pollers are **database sync processes**. They do not move or transform dataset files on disk.
- File staging, archival, hydration, and workflow execution happen in worker workflows (`workers/...`), not in `data_sync` bigbang modules.

## `metadata.origin` Semantics

`metadata.origin` is the source-of-truth for legacy provenance in API/UI/workers branching.

Values used by current code:

- `legacy`: CMG-origin business objects.
- `legacy_xenium`: Xenium-origin business objects.
- `bioloop`: Bioloop-created objects while legacy apps are still active (for example dataset creation paths in API).

Notes:

- `watch.py` tags auto-registered datasets under CMG legacy source directories as `legacy`, and Xenium source directories as `legacy_xenium`.
- Worker archive logic uses this field to decide whether to defer archival to legacy source apps.
- `sync` is documented in memory notes but is not currently set by code paths in this repo.

## Bigbang Runtime Model

## Containers and Compose Files

Bigbang uses its own `data_sync` compose files (separate from main app compose files):

- `data_sync/docker-compose.localhost.yml`
- `data_sync/docker-compose.prod.yml`

Wrapper scripts auto-select compose file via `APP_ENV` / `NODE_ENV` (or values read from `data_sync/.env`):

- production -> `docker-compose.prod.yml`
- otherwise -> `docker-compose.localhost.yml`

## Target Database Modes

Bigbang and pollers accept `--target-db`:

- `sandbox` (default): uses sandbox Postgres in `db_sandbox`.
- `app`: reads `api/.env` and points to app Postgres.
- `custom`: uses `DATABASE_URL` environment value.

Implementation: `data_sync/src/utils/db_config.js`.

## Wrapper vs Node Scripts

Shell wrappers are host-side launchers; Node scripts execute in `db_sandbox`:

- `bin/bigbang.sh` -> runs CMG then Xenium bigbang Node scripts in container.
- `bin/bigbang_cmg.sh` -> runs `src/bigbang_cmg_sync.js` in container.
- `bin/bigbang_xenium.sh` -> runs `src/bigbang_xenium_sync.js` in container.

Pollers can be run either:

- Direct wrappers (foreground): `start_pollers_cmg.sh`, `start_pollers_xenium.sh`.
- Managed orchestrator mode (background + PID files): `bin/init.sh --*-start-pollers`.

## CMG Bigbang: What It Populates

Driver: `data_sync/src/bigbang_cmg_sync.js`

Execution order (18 runtime steps):

1. Roles (`role`)
2. CMG system user + role link (`user`, `user_role`)
3. Pipeline definitions (`cmd_line_program`, `conversion_definition`, `argument`)
4. Analysis types (`analysis_type`, `metadata.origin='legacy'`)
5. Import sources + About content (`import_source`, `about`)
6. Bootstrap prod users from API JSON
7. Users (`user`, `user_role`)
8. Datasets (`dataset`, `dataset_genomic_attributes`, `metadata.origin='legacy'`)
9. Dataset audit logs (`dataset_audit`)
10. Historic stage/download events (`data_access_log`, `stage_request_log`, `dataset_state`)
11. Events collection download-copy logs (`data_access_log`)
12. Import logs (`dataset_import_log`, plus `dataset.create_method='IMPORT'` when missing)
13. Conversions (`conversion`, `argument_value`, `metadata.origin='legacy'`)
14. Projects + ACL links (`project`, `project_user`, `project_dataset`, `metadata.origin='legacy'`)
15. Dataset hierarchies (`dataset_hierarchy`, includes conversion linkage metadata)
16. Conversion logs from filesystem (`workflow`, `worker_process`, `log`, updates `conversion.workflow_id`)
17. Genome browser sessions (`genome_browser_session`, `metadata.origin='legacy'`, datasets list)
18. Cursor initialization (`cmg_sync_cursor`)

Key object-level details:

- Sessions are migrated only if they have non-empty track arrays; tracks themselves are not migrated in bigbang.
- Conversion logs step reads existing log files; it does not run conversions.
- Bigbang is idempotent: checks by `cmg_id`, unique constraints, and upserts where applicable.

## Xenium Bigbang: What It Populates

Driver: `data_sync/src/bigbang_xenium_sync.js`

Execution order (9 runtime steps):

1. Seed constants (`role`, xenium system `user`, `user_role`, `analysis_type`, `import_source`)
2. Users (`user`, `user_role`, with `xenium_id`)
3. Datasets (`dataset`, with `xenium_id`, preserves metadata including Xenium analysis-summary metadata)
4. Dataset audit logs (`dataset_audit`)
5. Dataset import logs (`dataset_import_log`, when source table exists)
6. Dataset hierarchies (`dataset_hierarchy`)
7. Projects + ACL links (`project`, `project_user`, `project_dataset`)
8. Cursor initialization (`xenium_sync_cursor`)
9. Bootstrap prod users from API JSON

Key differences vs CMG:

- No sessions migration.
- No conversions migration.
- No conversion-logs migration.
- Uses `metadata.origin='legacy_xenium'` (or `bioloop` if Xenium legacy source is configured inactive in helper logic).

## File-System Effects During Bigbang

Bigbang does not relocate dataset files. File interactions are limited to:

- Reading text file `about_cmg` to seed `about` table (CMG).
- Reading historical conversion log files from configured logs directory (CMG conversion logs step).
- Writing process logs under `/tmp/...` inside `db_sandbox` (mounted to host paths per compose mode).

No dataset directory rename/move/copy occurs inside bigbang sync modules.

## Pollers: What They Sync

## CMG Pollers

Driver: `data_sync/src/poller_cmg_sync.js`

Runtime pollers started:

- `user_roles`
- `project_acl`
- `dataset_activity` (deprecated no-op; still instantiated)
- `dataset_metadata`
- `project_metadata`

Field-level behavior:

- `user_roles`: syncs `user.is_deleted` and `user_role` memberships.
- `project_acl`: syncs project ACL links (`project_user`, `project_dataset`) and some metadata fields (`description`, `browser_enabled`), does not touch slug.
- `dataset_metadata`: syncs `dataset.description` only.
- `project_metadata`: syncs `project.name`, `description`, `browser_enabled`, `funding`.
- `dataset_activity`: intentionally no updates.

Cursor/lock behavior:

- Cursor: `cmg_sync_cursor` (`last_updated_at`, `last_cmg_objectid`).
- Retry queue: `cmg_sync_retry`.
- Process lock: `cmg_sync_process_lock` (`poller`).
- Startup refuses to run if CMG bigbang lock is active.

## Xenium Pollers

Driver: `data_sync/src/poller_xenium_sync.js`

Runtime pollers started:

- `xenium_user_roles`
- `xenium_project_acl`
- `xenium_dataset_metadata`
- `xenium_project_metadata`

Field-level behavior mirrors CMG intent:

- user delete/role mappings
- project ACL associations
- dataset description
- project name/description/browser/funding

Cursor/lock behavior:

- Cursor: `xenium_sync_cursor` (`last_updated_at`, `last_xenium_id`).
- Retry queue: `xenium_sync_retry`.
- Process lock: `xenium_sync_process_lock` (`xenium_poller`).
- Startup refuses to run if Xenium bigbang lock is active.

## Orchestrator (`data_sync/bin/init.sh`)

`init.sh` is explicit per-app/per-action. No implicit app selection.

Canonical action flags:

- `--cmg-run-bigbang`
- `--cmg-start-pollers`
- `--cmg-stop-pollers`
- `--cmg-restart-pollers`
- `--xenium-run-bigbang`
- `--xenium-start-pollers`
- `--xenium-stop-pollers`
- `--xenium-restart-pollers`

Shared/general flags:

- `--target-db sandbox|app|custom`
- `--clear-target-db` (only valid when at least one bigbang action is selected)
- `--dry-run`

App-specific lock flags:

- `--cmg-clear-locks`
- `--xenium-clear-locks`

Managed poller lifecycle details:

- PID files:
  - `data_sync/run/cmg_poller.pid`
  - `data_sync/run/xenium_poller.pid`
- Managed start writes logs under `data_sync/logs/` with timestamped filenames.

Deprecated/ambiguous flags are rejected by design (`--cmg`, `--xenium`, `--all`, `--bigbang`, etc.).

## Running Bigbang/Pollers (Examples)

## Common Wrapper Examples (`.sh`)

From `data_sync/`:

```bash
# Preflight: show resolved actions only
./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --target-db app --dry-run

# Run both bigbangs against app DB
./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --target-db app

# CMG-only bigbang, skip sessions and conversion logs
./bin/init.sh --cmg-run-bigbang --cmg-skip-sessions --cmg-skip-conversion-logs

# Xenium-only bigbang into sandbox
./bin/init.sh --xenium-run-bigbang --target-db sandbox

# Start both pollers in managed background mode
./bin/init.sh --cmg-start-pollers --xenium-start-pollers --target-db app
```

Direct wrappers:

```bash
./bin/bigbang_cmg.sh --target-db app --clear-locks
./bin/bigbang_xenium.sh --target-db app --clear-locks
./bin/bigbang.sh --target-db app --clear-target-db

./bin/start_pollers_cmg.sh --target-db app
./bin/start_pollers_xenium.sh --target-db app
```

## Node Script Examples (`.js`)

Run inside `db_sandbox`:

```bash
node src/bigbang_cmg_sync.js --target-db=sandbox --skip-sessions --skip-conversion-logs
node src/bigbang_xenium_sync.js --target-db=sandbox
node src/poller_cmg_sync.js --target-db=app --clear-locks
node src/poller_xenium_sync.js --target-db=app --clear-locks
```

## Clear/Lock Semantics

- `--clear-locks` releases process locks before attempting run.
- `--clear-target-db` behavior depends on entry point:
  - Via orchestrator / xenium bigbang path: clears both CMG+Xenium migrated business rows and sync infra rows.
  - CMG bigbang script directly: clears CMG-originated rows/cursors/retries only.

## Deployment Notes for Bigbang/Pollers

`data_sync/.env` provides key properties used by `data_sync` scripts/config:

- CMG source:
  - `CMG_MONGO_HOST`
  - `CMG_MONGO_PORT`
  - `CMG_MONGO_DB`
  - `CMG_MONGO_USERNAME`
  - `CMG_MONGO_PASSWORD`
- Xenium source:
  - `XENIUM_PG_HOST`
  - `XENIUM_PG_PORT`
  - `XENIUM_PG_DATABASE`
  - `XENIUM_PG_USERNAME`
  - `XENIUM_PG_PASSWORD`
- Sandbox target defaults:
  - `SYNC_PG_HOST`
  - `SYNC_PG_PORT`
  - `SYNC_PG_USER`
  - `SYNC_PG_PASSWORD`
  - `SYNC_PG_DATABASE`
- CMG conversion logs source:
  - `CMG_LEGACY_CONVERSIONS_LOGS_DIR`
- Base data root:
  - `DATA_ROOT`
- Legacy source activeness toggles (via config env mapping):
  - `LEGACY_APPLICATION_ACTIVE_CMG`
  - `LEGACY_APPLICATION_ACTIVE_XENIUM`

## Production Logs

## Bigbang/Poller (`data_sync`) Logs

- Node logger writes to `/tmp/<script_name>_<timestamp>.log` inside container.
- In prod compose, `/tmp` is mounted to host `/tmp/data_sync_logs`, so host-visible logs are in:
  - `/tmp/data_sync_logs/`

Managed pollers started by `init.sh` also create host-side logs in:

- `data_sync/logs/` (timestamped `cmg_poller_*` / `xenium_poller_*` files).

## Worker Script Logs (including concurrent-registration maintenance)

PM2 config writes worker logs under:

- `workers/../logs/workers/...`

On deployed host this maps under the workers install root (for example `/opt/sca/cmg/workers/logs/workers/...`), including:

- `manage_legacy_concurrent_registrations.log`
- `manage_legacy_concurrent_registrations.err`

## Legacy Concurrent Registrations and SDA Safety

## Why This Exists

When legacy-origin datasets are being registered in Bioloop while legacy apps are still active, both sides could otherwise attempt archival/upload behavior concurrently.

## How Overwrite/Duplicate Upload to SDA Is Avoided

In `workers/tasks/archive.py`, if dataset origin is:

- `legacy` and CMG legacy source is active, or
- `legacy_xenium` and Xenium legacy source is active,

then Bioloop defers archival upload to the legacy source app and does not upload a second bundle.

Flow:

1. Wait for legacy source archival completion (`wait_for_cmg_archival` or `wait_for_xenium_archival`).
2. Verify archive exists in SDA.
3. Read checksum and size from SDA metadata.
4. Save bundle metadata to Bioloop dataset/bundle records.

If origin is not an active legacy source, Bioloop performs standard tar + upload archival flow.

## Maintenance Script for Failed Concurrent Registrations

Script:

- `workers/workers/scripts/manage_legacy_concurrent_registrations.py`

Behavior:

- Scans latest `integrated` and `intake_integrated` workflows per dataset.
- Filters to datasets with `metadata.origin in {'legacy', 'legacy_xenium'}`.
- Targets workflows whose `archive` step is `FAILED`.
- Resumes only if workflow status is resumable (`FAILED` or `PAUSED`), via:
  - `workers.api.list_workflows()`
  - `workers.api.resume_workflow()`

Scheduled daily in PM2 (`fetch.ecosystem.config.js`).

## UI Functional Differences for Legacy Objects

Current UI behavior (implemented):

- Legacy dataset edit modal shows historical-data alert and makes analysis type read-only (`EditDatasetModal.vue`).
- Legacy dataset page disables Delete Archive action (`Dataset.vue`).
- Browse Files on CMG legacy dataset checks migration-in-progress and blocks duplicate staging launch (`Dataset.vue`).
- Session page requires staged datasets and hydration workflow before opening browser for legacy sessions (`sessions/[id].vue`).
- Legacy project disables Delete Project action (`projects/[projectId]/index.vue`).
- Legacy user edit modal shows historical-data alert; identity fields are read-only; delete option hidden for legacy users (`users.vue`).
- Conversion table marks legacy-origin conversions as complete badge (`DatasetConversions.vue`).

Xenium UI note:

- Xenium service helpers exist under `ui/src/services/legacyMigration/xenium`.
- Most current page components still import the CMG compatibility default service (`@/services/legacyMigration`), so many legacy UI restrictions are currently CMG-focused unless page code explicitly uses Xenium helpers.

## Utility: `reclaim_bigbang_container_resources.sh`

Script:

- `data_sync/bin/reclaim_bigbang_container_resources.sh`

Purpose:

- Destructive reset of both app DB and sandbox DB resources used by bigbang.

What it does:

- Stops main app `api`, `ui`, `postgres` services.
- Removes app postgres container (recreate with current env next start).
- Stops/removes `db_sandbox`.
- Deletes `bioloop_sandbox_pgdata` volume.
- Clears app Postgres host data directory.
- Prunes unused Docker builder/image/container/volume resources.
- Restarts fresh `db_sandbox`.

Use when:

- You need a hard reset of migration test state across app + sandbox targets.
- You need to reclaim disk space before rerunning migration.

Warning:

- This is intentionally destructive for migration-related DB state and should be used as an operator reset tool, not routine operation.
