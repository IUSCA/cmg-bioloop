# Xenium Legacy Migration

**Feature Scope:** Migrating data from the Xenium fork of bioloop (PostgreSQL source) into cmg-bioloop's PostgreSQL, with ongoing synchronization pollers, and supporting cmg-bioloop infrastructure to process and serve xenium datasets.

**Status:** In Progress

---

## Documentation Checklist Pointer

Upcoming migration-doc checklist location:
- `.ai/features/migration-documentation-checklists.md` (see Xenium and Cross-App sections)

---

## Design Decisions

### Source Database
- CMG migration: MongoDB → PostgreSQL
- Xenium migration: PostgreSQL → PostgreSQL (xenium uses standard bioloop schema)
- Xenium pollers connect to xenium's PostgreSQL using a separate Prisma client instance, using timestamp-based cursor queries instead of MongoDB ObjectID cursors

### `metadata.origin` for Xenium Business Objects
- Xenium-migrated rows use `metadata.origin = 'legacy_xenium'` (distinct from CMG's `'legacy'`)
- Applies to: dataset, project, user, dataset_import_log (and conversion/genome_browser_session if ever added)
- Pollers that create rows from xenium uploads/changes set `metadata.origin = 'legacy_xenium'`

### `xenium_id` Field
- All tables with `cmg_id` get a parallel `xenium_id` column (same nullable `String?` type, same semantics)
- Affected tables: `dataset`, `dataset_import_log`, `user`, `project`, `genome_browser_session`, `conversion`
- Infrastructure tables (`cmg_sync_*`) do not get `xenium_id` — xenium has its own infrastructure tables (`xenium_sync_*`)

### Xenium Sync Infrastructure Tables
- `xenium_sync_cursor`: mirrors `cmg_sync_cursor`, tracks poller cursor positions against xenium's PostgreSQL
- `xenium_sync_retry`: mirrors `cmg_sync_retry`, failed-document retry queue for xenium pollers
- `xenium_sync_process_lock`: mirrors `cmg_sync_process_lock`, prevents concurrent xenium bigbang/poller runs

### Process Lock Manager — Generic
- `data_sync/src/sync/shared/process_lock_manager.js` is now source-agnostic
- All functions accept an optional `lockTableModel` parameter (default `'cmg_sync_process_lock'`)
- CMG scripts pass `'cmg_sync_process_lock'`; xenium scripts pass `'xenium_sync_process_lock'`

### Cursor Manager — Generic
- `data_sync/src/sync/shared/cursor_manager.js` is source-agnostic
- All functions accept a `cursorTableModel` parameter (default `'cmg_sync_cursor'`)
- Error logger similarly accepts `retryTableModel` parameter (default `'cmg_sync_retry'`)

### Directory Restructuring (data_sync)
CMG-specific sync code moved from flat `src/sync/{bigbang,pollers}/` into `src/sync/cmg/{bigbang,pollers}/`.
Xenium-specific code lives in `src/sync/xenium/{bigbang,pollers}/`.
Shared utilities (cursor manager, error logger, process lock manager) live in `src/sync/shared/`.

```
data_sync/src/
├── bigbang_cmg_sync.js        (renamed from bigbang_sync.js)
├── bigbang_xenium_sync.js     (new)
├── poller_cmg_sync.js         (renamed from poller_sync.js)
├── poller_xenium_sync.js      (new)
└── sync/
    ├── shared/
    │   ├── cursor_manager.js
    │   ├── error_logger.js
    │   └── process_lock_manager.js
    ├── cmg/
    │   ├── constants.js
    │   ├── bigbang/
    │   │   ├── seed_constants.js
    │   │   ├── populate_bioloop_users.js
    │   │   ├── sync_users.js
    │   │   ├── sync_datasets.js
    │   │   ├── sync_audit_logs.js
    │   │   ├── sync_download_stage_logs.js
    │   │   ├── sync_events_collection.js
    │   │   ├── sync_import_logs.js
    │   │   ├── sync_conversions.js
    │   │   ├── sync_projects.js
    │   │   ├── sync_dataset_hierarchies.js
    │   │   ├── sync_conversion_logs.js
    │   │   ├── sync_sessions.js
    │   │   └── initialize_cursors.js
    │   ├── pollers/
    │   │   ├── base_poller.js
    │   │   ├── user_roles_poller.js
    │   │   ├── project_acl_poller.js
    │   │   ├── dataset_activity_poller.js
    │   │   ├── dataset_metadata_poller.js
    │   │   └── project_metadata_poller.js
    │   └── utils/
    │       ├── cmg_helpers.js
    │       ├── duplicate_handler.js
    │       ├── event_parser.js
    │       ├── role_mapper.js
    │       └── state_mapper.js
    └── xenium/
        ├── constants.js
        ├── bigbang/
        │   ├── seed_constants.js
        │   ├── sync_users.js
        │   ├── sync_datasets.js
        │   ├── sync_audit_logs.js
        │   ├── sync_import_logs.js
        │   ├── sync_projects.js
        │   ├── sync_dataset_hierarchies.js
        │   └── initialize_cursors.js
        └── pollers/
            ├── base_poller.js
            ├── user_roles_poller.js
            ├── project_acl_poller.js
            ├── dataset_metadata_poller.js
            └── project_metadata_poller.js
```

### Shell Script Naming
- `bin/bigbang_cmg.sh` — runs CMG bigbang (renamed from `bin/bigbang.sh`)
- `bin/start_pollers_cmg.sh` — runs CMG pollers (renamed from `bin/start_pollers.sh`)
- `bin/bigbang_xenium.sh` — runs xenium bigbang
- `bin/start_pollers_xenium.sh` — runs xenium pollers
- `bin/init.sh` — central entry point; `--source cmg|xenium|all` selects which migration to run

### API Legacy Migration Route Structure
Existing routes under `/legacy/migrations/` are reorganized:

```
api/src/routes/legacyMigration/
├── index.js            (mounts cmg and xenium sub-routers)
├── cmg/
│   └── migrations.js   (moved from routes/legacy/migrations.js)
└── xenium/
    └── migrations.js   (new)
```

URL paths remain unchanged:
- `/legacy/migrations/datasets/by-cmg-id/:cmgId` — CMG dataset lookup by CMG ID
- `/legacy/migrations/datasets/by-xenium-id/:xeniumId` — Xenium dataset lookup by xenium ID (new)
- `/legacy/migrations/datasets/:id` — dataset migration status (CMG + xenium aware)
- `/legacy/migrations/sessions/:id` — session migration status (CMG only; xenium has no sessions)

### API Service Structure
```
api/src/services/legacyMigration/
├── index.js    (re-exports both)
├── cmg.js      (moved from services/legacyMigration.js)
└── xenium.js   (new)
```

`isLegacyDataset` (CMG) checks `metadata.origin === 'legacy'`.
`isXeniumDataset` checks `metadata.origin === 'legacy_xenium'`.
`isLegacyOrXeniumDataset` returns true for either origin (used in staging decision).

### UI Service Structure
```
ui/src/services/legacyMigration/
├── index.js
├── cmg.js      (moved from services/legacyMigration.js)
└── xenium.js   (new)
```

### Workers Legacy Migration Structure
```
workers/workers/legacy_migration/
├── __init__.py     (re-exports cmg + xenium helpers)
├── cmg.py          (moved from legacy_migration.py)
└── xenium.py       (new)
```

Old import path `workers.legacy_migration` still works via `__init__.py` re-exports.

### Worker Queue Constants
```python
# workers/workers/constants/queue.py  (new file)
XENIUM_ARCHIVE_QUEUE = 'archive.xenium.sca.iu.edu.q'
XENIUM_FETCH_QUEUE = 'fetch.xenium.sca.iu.edu.q'
```

Referenced in workflow registry config (`archive.xenium.q` pattern), worker PM2 config, and any direct task routing.

### Xenium Bigbang Steps
No sessions, no conversions, no conversion logs (xenium has none).
Estimated 8 steps:
1. Seed constants (roles, xenium system user, analysis types, import sources)
2. Sync users
3. Sync datasets (RAW_DATA and DATA_PRODUCT)
4. Sync dataset audit logs
5. Sync import logs
6. Sync dataset hierarchies (RAW_DATA → DATA_PRODUCT)
7. Sync projects
8. Initialize xenium cursors

### Xenium Pollers
Source is xenium's PostgreSQL. Cursors track `updated_at` timestamps + integer primary keys.
4 concurrent pollers (xenium has no sessions/conversions to poll):
1. `xenium_user_roles` — sync user role changes
2. `xenium_project_acl` — sync project user + dataset ACL
3. `xenium_dataset_metadata` — sync dataset description changes
4. `xenium_project_metadata` — sync project name/description/browser_enabled

### Dataset Staging for Xenium (Worker workflow)
`stage_migrated_xenium` workflow:
1. `begin_migration` — sets MIGRATION_INITIATED state
2. `retrieve_archive` — fetches bundle from xenium's SDA archive path
3. `inspect` — inspect staged files
4. `populate_metadata` — populate bundle/file metadata
5. `parse_analysis_data` — parse analysis_summary.html from staged path (xenium-specific)
6. `stage` — extract bundle
7. `validate` — validate staged data
8. `setup_download` — create download symlinks
9. `upload_static_content` — upload analysis_summary.html to API (xenium-specific)
10. `end_migration` — sets MIGRATED state

Staging decision in `POST /datasets/:id/workflows/stage`:
- `metadata.origin === 'legacy_xenium'` AND not yet hydrated → triggers `stage_migrated_xenium`
- `metadata.origin === 'legacy_xenium'` AND already hydrated → triggers standard `stage`
- `metadata.origin === 'legacy'` AND not yet hydrated → triggers `stage_migrated` (existing CMG flow)

### API Route: PUT /api/datasets/:id/report
Must handle both metadata key patterns:
- `metadata.analysis_summary_file_dir` (xenium) → store as `reports/{uuid}/analysis_summary.html`
- `metadata.report_id` (CMG/bioloop) → store as `reports/{uuid}/multiqc_report.html`

### Watch.py Observer for Xenium
A new `obs_xenium` observer watches `/zpool/xenium/` and registers RAW_DATA datasets with `default_wf_name='subdir_wf_initiator'`.

### `integrated` Workflow — Conditional Xenium Steps
The existing `integrated` workflow in cmg-bioloop gets conditional step execution:
- `parse_analysis_data` — only runs if dataset has `metadata.origin === 'legacy_xenium'` or is a xenium DATA_PRODUCT
- `upload_static_content` — same condition

`initiate_subdir_workflows` task is xenium-specific and only runs in `subdir_wf_initiator` workflow.

### `intake_integrated` Workflow
If cmg-bioloop has an `intake_integrated` workflow, it too gets the same conditional xenium steps.

---

## Changes Required in Xenium App (TODO for separate work)

The following changes must be made in the Xenium application at `/Users/ripandey/dev/xenium`:

- [ ] `GET /api/datasets/by-xenium-id/:xeniumId` — add endpoint to look up a dataset by its xenium PostgreSQL ID (the cmg-bioloop bigbang needs this to verify migration idempotency; alternatively, bigbang can use the xenium DB directly via direct PG query and this endpoint is not strictly needed — confirm approach)
- [ ] Verify `GET /api/alerts` is registered in xenium's router (noted as absent in xenium-fork-explained.md)
- [ ] Ensure `analysis_summary_file_dir` metadata is preserved on all DATA_PRODUCT rows (required for report serving after migration)
- [ ] Coordinate any changes to xenium's SDA archive path conventions with cmg-bioloop's `retrieve_archive` task

---

## 2026-03-06

### Initial Design

- Decision: Xenium migrated objects use `metadata.origin = 'legacy_xenium'` (not `'legacy'` — distinguishes xenium from CMG).
- Decision: All tables with `cmg_id` get a parallel `xenium_id` column.
- Decision: New Prisma sync infrastructure tables prefixed `xenium_sync_*` mirror the CMG `cmg_sync_*` tables.
- Decision: CMG bigbang/poller code moved to `src/sync/cmg/` subdirectory; shared utilities extracted to `src/sync/shared/`; xenium lives in `src/sync/xenium/`.
- Decision: Shell scripts named consistently: `bigbang_cmg.sh`, `bigbang_xenium.sh`, `start_pollers_cmg.sh`, `start_pollers_xenium.sh`, `init.sh` (central).
- Decision: Xenium poller is PostgreSQL-to-PostgreSQL using timestamp cursor queries.
- Decision: `process_lock_manager.js` and `cursor_manager.js` made generic via optional table model name parameter.
- Decision: `stage_migrated_xenium` is a new workflow distinct from `stage_migrated` (CMG); it includes xenium-specific steps `parse_analysis_data` and `upload_static_content`.
- Decision: `PUT /api/datasets/:id/report` updated to handle both `analysis_summary_file_dir` (xenium) and `report_id` (CMG) metadata keys.
- Decision: The `integrated` and `intake_integrated` workflows get conditional xenium steps rather than separate named xenium workflows — cleaner than proliferating workflow names.
- Constraint: Xenium bigbang does NOT migrate sessions, conversions, or conversion logs (xenium has none of these features).
- Constraint: Xenium pollers do NOT include session or conversion pollers.
- Change: API service and route code for legacy migration reorganized into `legacyMigration/{cmg,xenium}` subdirectory structure.
- Change: UI `legacyMigration` service reorganized into `legacyMigration/{cmg,xenium}` subdirectory structure.
- Change: Workers `legacy_migration.py` reorganized into `legacy_migration/{cmg,xenium}.py` package structure; old import path preserved via `__init__.py`.

---

## Chat Breakdown

This feature is divided across multiple chat sessions. Each chat has a defined scope.

| Chat | Scope |
|---|---|
| Chat 1 (this) | Planning doc + DB schema + data_sync/ restructure + shell scripts + API/UI/workers restructure |
| Chat 2 | Implement xenium bigbang sync modules (`src/sync/xenium/bigbang/*.js`) + `bigbang_xenium_sync.js` |
| Chat 3 | Implement xenium poller classes (`src/sync/xenium/pollers/*.js`) + `poller_xenium_sync.js` |
| Chat 4 | Worker changes: `parse_analysis_data`, `upload_static_content`, `initiate_subdir_workflows` tasks; `stage_migrated_xenium` workflow config; `watch.py` observer; worker production config paths/queues |
| Chat 5 | API changes: `PUT /api/datasets/:id/report` dual-key handling; xenium migration routes; dataset staging logic for `legacy_xenium` datasets; `GET /datasets/by-xenium-id` |

---

## Critical Handoff Instructions (Chats A/B/C)

These instructions are mandatory for continuation chats and should be treated as implementation constraints.

### Config Key Naming (JS/API)

- Use **per-source booleans** under the existing key namespace:
  - `legacy_application_active.cmg`
  - `legacy_application_active.xenium`
- Do **not** rely on a single global on/off boolean for all legacy sources.
- Existing JS usages of `legacy_application_active` must be updated to source-aware reads (for example, CMG-specific logic reads `.cmg`, Xenium-specific logic reads `.xenium`).
- Requirement: CMG and Xenium must be independently retire-able on different dates.

### Chat A — Origin-Retirement Safety Refactor

- Implement independent retirement toggles for CMG and Xenium using:
  - `legacy_application_active.cmg` (boolean)
  - `legacy_application_active.xenium` (boolean)
- Audit and refactor all origin checks/usages across UI/API/workers/data_sync so future records created for a retired source can safely use `metadata.origin = 'bioloop'` without breaking:
  - workflow selection
  - hydration/migration status APIs
  - UI legacy badges/actions
  - worker branching logic
- Preserve provenance for already-migrated rows:
  - existing `legacy`/`legacy_xenium` values are immutable and must not be backfilled to `bioloop`.

### Chat B — Xenium Multi-Node Queue/Routing Completion

- Do not reuse Xenium fork queue names; use cmg-bioloop-managed dedicated Xenium queues.
- Ensure each Xenium workflow step is routed to the correct node-specific queue (archive-side vs fetch-side) while preserving current cmg-bioloop custom behavior:
  - separate bundle-generation paths by ingestion context
  - conditional bundle deletion after successful archival
- Align workflow definitions and queue routing consistently across API and workers configs (remove drift).
- Ensure PM2/worker consumers and task registration are wired so configured Xenium queues are actually consumed.

### Chat C — Xenium Sync/Task Implementation

- Implement Xenium bigbang modules and Xenium pollers (PostgreSQL-to-PostgreSQL) fully and idempotently.
- Implement worker task bodies:
  - `parse_analysis_data`
  - `upload_static_content`
  - `initiate_subdir_workflows`
- Ensure `metadata.origin` assignment follows source-aware retirement rules introduced in Chat A.

### Engineering Quality Requirements (All Chats)

- Code must be modular, decoupled, reusable, readable, and well-organized.
- Prefer small composable helpers over cross-cutting conditionals.
- Keep CMG and Xenium source-specific logic explicit where behavior meaningfully diverges.
- Avoid hidden coupling between API/UI/workers/data_sync; update interfaces/contracts in tandem.

---

## 2026-03-11

- Decision: `legacy_application_active` now supports per-source toggles (`{ cmg, xenium }`) with backward compatibility for legacy boolean config reads in API and workers.
- Change: `POST /datasets/:id/workflow/stage` now routes Xenium datasets to `stage_migrated_xenium` when hydration is pending and keeps CMG behavior unchanged.
- Change: Xenium queue routing is explicit and node-scoped (`cmg-bioloop-xenium-archive.*.q`, `cmg-bioloop-xenium-fetch.*.q`) in API workflow config, worker workflow config, and PM2 queue consumers.
- Change: Implemented worker task bodies for `parse_analysis_data`, `upload_static_content`, and `initiate_subdir_workflows`, including best-effort file handling and idempotent dataset hierarchy/workflow initiation behavior.
- Change: Enabled conditional Xenium RAW_DATA observer activation in `watch.py` when `registration.RAW_DATA.source_dir_xenium` is configured.
- Change: Xenium poller runner now instantiates and starts all four Xenium pollers; poller class stubs for role/ACL/metadata sync now execute concrete row-processing logic.
- Change: Implemented all Xenium bigbang modules (`seed_constants`, `sync_users`, `sync_datasets`, `sync_audit_logs`, `sync_import_logs`, `sync_dataset_hierarchies`, `sync_projects`, `initialize_cursors`) with idempotent create-or-update behavior and deterministic mapping by `xenium_id`.
- Change: Wired `bigbang_xenium_sync.js` to execute all 8 real migration phases (removed stubs), including optional xenium-scoped target cleanup before rerun.
- Change: Added reusable Xenium sync helpers (`bigbang/helpers.js`, `utils/role_mapper.js`) to centralize source-retirement origin policy, integer ID coercion, chunking, and project slug generation; poller role mapping now uses the shared utility.
- Change: Added `legacy_application_active.{cmg,xenium}` support in `data_sync` config files so data-sync-created records follow the same source-aware retirement policy as API/workers.
- Change: Refactored `data_sync/bin/init.sh` to explicit per-app action flags and controls for target DB, lock clearing, and source-scoped clear-target behavior.
- Change: Added `--dry-run` mode in `init.sh` to print fully resolved per-app commands/flags before execution for safer review in multi-branch migration scenarios.
- Decision: Orchestrator flags are explicit per app and per action; ambiguous aggregate flags are rejected (`--all`, `--both`, `--cmg`, `--xenium`, `--bigbang`, `--pollers`).
- Decision: `init.sh` requires at least one explicit action flag and never infers app selection by default (supersedes prior default-`bigbang` behavior).
- Change: CMG clear behavior is now source-scoped; CMG bigbang supports `--clear-cmg-target-data` and deletes CMG-originated rows/cursors/retries without truncating xenium data.
- Change: Orchestrator clear flags are now source-consistent and symmetric: `--cmg-clear-target-data` and `--xenium-clear-target-data`.
- Change: Xenium bigbang now exposes explicit source-scoped clear flag `--clear-xenium-target-data`; init forwards the explicit form.
- Decision: Deprecated clear alias `--clear-target-db` removed from Xenium bigbang CLI; only explicit `--clear-xenium-target-data` is supported.
- Clarification: Scenario matrix validated in dry-run mode for these cases: single-app initial bigbang, second-app follow-up bigbang + pollers, one-app rerun after both migrated, and dual-app rerun with explicit per-app action/clear/lock flags.

---

## 2026-03-12

- Change: Added explicit Xenium poller lifecycle controls in `data_sync/bin/init.sh`: `--xenium-start-pollers`, `--xenium-stop-pollers`, `--xenium-restart-pollers` (managed background mode with PID files under `data_sync/run/`).
- Change: Added symmetric CMG lifecycle controls in the same orchestrator for operational parity.
- Decision: Hybrid-row handling was removed from source-scoped clear logic; Xenium and CMG are treated as isolated migration domains with no cross-source contamination path.
- Change: Removed `--cmg-run-pollers` / `--xenium-run-pollers` from `init.sh`; poller lifecycle now uses only explicit per-app start/stop/restart flags.
- Change: Replaced per-app target-db flags with a shared `--target-db` in `init.sh`; all selected CMG/Xenium actions now run against the same target database.

---

## 2026-03-13

- Fix: Xenium origin retirement helper no longer falls back to `legacy_application_active.cmg` when `legacy_application_active.xenium` is absent; Xenium now defaults to active unless explicitly disabled.
- Fix: Xenium clear flow now mirrors CMG semantics for stale-lock recovery during reset operations (`--clear-xenium-target-data` clears rows/cursors/retries and then clears Xenium process locks before lock acquisition).
- Fix: Xenium poller startup now rejects launch when Xenium bigbang lock is active to avoid concurrent write races.
- Fix: Xenium bigbang clear operation now runs sequential deletes (instead of one interactive transaction) to reduce timeout risk on large cascades.
- Change: Xenium metrics reporter now includes `lastSuccessTime`; reserved usernames skipped by Xenium user sync now emit warning logs.
- Fix: Xenium unique-name/slug generators now include bounded attempt guards and descriptive failures.
- Change: Xenium pollers now write structured failure entries to `xenium_sync_errors.jsonl`; full source-row payload snapshots are gated behind `XENIUM_SYNC_LOG_FULL_PAYLOAD=true|1|yes`.
- Decision: Xenium bigbang timestamp fallback is deterministic when source timestamps are null/invalid (`1970-01-01T00:00:00.000Z`) to preserve rerun-stable history.
- Change: Xenium poller base now supports optional Prisma transaction timeout overrides; Xenium ACL poller defaults were hardened to `batchSize=50` and `transactionTimeoutMs=30000` to reduce timeout pressure from source reads inside target transactions.

---

**Last Updated:** 2026-03-13
