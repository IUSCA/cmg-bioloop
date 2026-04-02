# Legacy Apps Migrations

## Overview

This project supports legacy data ingestion from two upstream systems:

- **CMG** (legacy MongoDB-based app)
- **Xenium** (legacy PostgreSQL-based app)

The one-time backfill process is referred to as **bigbang**. After bigbang, pollers can keep Bioloop in sync incrementally.

At a high level:

- CMG bigbang: `MongoDB -> Bioloop PostgreSQL`
- Xenium bigbang: `PostgreSQL -> Bioloop PostgreSQL`

## Core scripts and responsibilities

### `data_sync/bin/bigbang.sh` (central wrapper)

Runs both bigbang phases sequentially:

1. CMG (`bigbang_cmg_sync.js`)
2. Xenium (`bigbang_xenium_sync.js`)

Important behavior:

- If CMG fails, Xenium does not run.
- When `--target-db=app`, it runs Prisma migration preflight against the app database before starting.
- It forwards most flags to both phases, but intentionally removes `--clear-target-db` from Xenium to avoid double-clear overhead in one central run.

### `data_sync/bin/bigbang_cmg.sh`

CMG-only wrapper for `src/bigbang_cmg_sync.js`.

Important behavior:

- Runs migration preflight when target is `app`.
- Useful for CMG-only reruns and troubleshooting.

### `data_sync/bin/bigbang_xenium.sh`

Xenium-only wrapper for `src/bigbang_xenium_sync.js`.

Important behavior:

- Runs migration preflight when target is `app`.
- Useful for Xenium-only reruns and troubleshooting.

### `data_sync/src/bigbang_cmg_sync.js`

CMG migration implementation. Major phases include:

- role/system-user/bootstrap constants
- users, datasets, audit logs, import logs, hierarchies, projects, conversions
- conversion logs (optional skip)
- legacy QC/MultiQC report copy
- sessions/tracks migration (optional skip)
- cursor initialization and lock handling

### `data_sync/src/bigbang_xenium_sync.js`

Xenium migration implementation. Major phases include:

- constants seed
- users, datasets, dataset audit/import logs, hierarchies, projects
- cursor initialization and lock handling

Note: Xenium bigbang does **not** migrate CMG-specific sessions/tracks or CMG conversion logs.

### `data_sync/bin/reclaim_bigbang_container_resources.sh`

Destructive reset/reclaim helper for low-disk or fresh-start scenarios.

By default it reclaims for **both** targets:

- main app DB storage (`target-db=app`)
- sandbox DB storage (`target-db=sandbox`)

Important side effects:

- stops `api`, `ui`, `postgres` in main compose stack
- removes main `postgres` container (to force recreation with current env)
- clears main app postgres host data directory
- removes sandbox volume (`bioloop_sandbox_pgdata`) and recreates `db_sandbox`
- aggressively prunes unused Docker cache/images/containers/volumes

This script intentionally avoids `docker compose down` to reduce shared-network disruption.

## Bigbang flags

## Central wrapper: `data_sync/bin/bigbang.sh`

- `--target-db <sandbox|app|custom>`
- `--clear-target-db`
- `--clear-locks`
- `--skip-sessions` (CMG phase only)

## CMG-only wrapper: `data_sync/bin/bigbang_cmg.sh`

- `--target-db <sandbox|app|custom>`
- `--clear-target-db`
- `--clear-locks`
- `--skip-sessions`
- `--skip-conversion-logs`

## Xenium-only wrapper: `data_sync/bin/bigbang_xenium.sh`

- `--target-db <sandbox|app|custom>`
- `--clear-target-db`
- `--clear-locks`

## Target DB semantics

- `sandbox`: uses data_sync local DB in `db_sandbox`
- `app`: targets main app Postgres (`docker-compose-prod.yml` stack)
- `custom`: uses `DATABASE_URL` for custom target

## Setup and required configuration

## 1) Main stack env (`/opt/sca/cmg/.env`)

Must include at minimum:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Also ensure filesystem mount vars used by API are set (for upload and conversion report paths).

## 2) API env (`/opt/sca/cmg/api/.env`)

Bigbang `target-db=app` resolves app DB credentials from API env values:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- optional `POSTGRES_SCHEMA` (default `public`)

These must match the real app Postgres target.

## 3) data_sync env (`/opt/sca/cmg/data_sync/.env`)

CMG source (Mongo):

- `CMG_MONGO_HOST`
- `CMG_MONGO_PORT`
- `CMG_MONGO_DB`
- `CMG_MONGO_USERNAME`
- `CMG_MONGO_PASSWORD`

Xenium source (Postgres):

- `XENIUM_PG_HOST`
- `XENIUM_PG_PORT`
- `XENIUM_PG_DATABASE`
- `XENIUM_PG_USERNAME`
- `XENIUM_PG_PASSWORD`

Optional CMG historical artifacts:

- `CMG_LEGACY_CONVERSIONS_LOGS_DIR`
- `CMG_LEGACY_QC_REPORTS_DIR`
- `CMG_QC_REPORTS_TARGET_DIR`

## 4) Run order for fresh/recovered migrations

Recommended sequence:

1. `./data_sync/bin/reclaim_bigbang_container_resources.sh` (if reclaim/reset needed)
2. `./bin/deploy.sh`
3. `./data_sync/bin/bigbang.sh --target-db=app --clear-target-db --clear-locks`

## Path mapping (container vs host)

These are important for QC/MultiQC and conversion-log ingestion.

## CMG legacy conversion logs

- container path: `/opt/sca/project/ingestion_source_dir/CMG-SCA/production/runlogs`
- host path (via bind mount): `/N/project/CMG-SCA/production/runlogs`
- config key / env:
  - `cmg.legacyConversionsLogsDir`
  - `CMG_LEGACY_CONVERSIONS_LOGS_DIR`

## CMG legacy QC source

- container path: `/opt/sca/project/ingestion_source_dir/CMG-SCA/production/qc`
- host path (via bind mount): `/N/project/CMG-SCA/production/qc`
- config key / env:
  - `cmg.legacyQcReportsDir`
  - `CMG_LEGACY_QC_REPORTS_DIR`

## CMG QC target

- container target (bigbang copy destination): `/opt/sca/data/conversion_reports/qc_reports`
- configured by:
  - `cmg.qcReportsTargetDir`
  - `CMG_QC_REPORTS_TARGET_DIR`
- host-side equivalent depends on runtime mount configuration for that path.
  - In the main API stack, conversion reports are mounted using:
    - host: `${CONVERSION_REPORTS_HOST_DIR}`
    - container: `${CONVERSION_REPORTS_MOUNT_DIR}`
  - Ensure this mapping and the QC target strategy are aligned for your environment.

## Legacy app "enabled" settings

Per-app legacy-active settings are present in backend/worker/data_sync config:

- API: `api/config/default.json`
  - `legacy_application_active.cmg`
  - `legacy_application_active.xenium`
- Workers: `workers/workers/config/common.py`
  - `legacy_application_active['cmg']`
  - `legacy_application_active['xenium']`
- data_sync config:
  - `data_sync/config/default.json`
  - `data_sync/config/custom-environment-variables.json`
  - `legacy_application_active.cmg/xenium`

UI currently has only a single toggle:

- `ui/src/config.js`: `legacy.enabled`

UI does not currently expose separate CMG vs Xenium legacy-enable flags.

## Troubleshooting

## `No space left on device` (Postgres / WAL)

Symptoms:

- Prisma errors with Postgres code `53100`
- messages like `pg_wal/xlogtemp... No space left on device`

Resolution:

1. Reclaim/reset:
   - `./data_sync/bin/reclaim_bigbang_container_resources.sh`
2. Recreate stack:
   - `./bin/deploy.sh`
3. Re-run bigbang.

## Missing lock tables after fresh DB reset

Symptoms:

- `public.cmg_sync_process_lock does not exist`

Resolution:

- Bigbang wrappers now run migration preflight for `--target-db=app`.
- If needed manually:
  - run `npx prisma migrate deploy` against app DB before bigbang.

## Editing migration code while bigbang is running

If you edit `data_sync` JS files while a bigbang run is already in progress:

- the currently running Node process generally continues with the code already loaded in memory
- edits usually affect the next run, not the current one
- for reliability and reproducibility, avoid editing migration files mid-run

