# Genomic Conversions

## Overview

Genomic Conversions run pipeline-style transformations on staged datasets (for example, sequencing-run style input to derived data products) using the conversion workflow stack in API + workers.

The workflow behind this feature is `genomic_conversion`, whose first step is `convert_genomic` in `workers/workers/tasks/convert_genomic.py`.

Platform-based execution (for example, SLURM) is optional. Core conversion functionality is not tightly coupled to SLURM or any external scheduler, and conversions run fully in local worker mode when platform execution is disabled.

## Architecture

### Components

- **UI** (`/conversions` list and `/conversions/:id` detail) creates conversions, polls workflow status, renders run metadata, shows logs, and opens reports.
- **API** (`api/src/routes/conversions/index.js`) validates conversion requests, creates conversion rows, starts workflows through Rhythm, and serves conversion-specific read endpoints (`/logs`, `/derived_datasets`, `/reports`).
- **Worker conversion app** (`workers/workers/conversions_app.py`) registers conversion tasks (`convert_genomic`, `copy_conversion_reports`, `derive_data_products`) on the conversion queue.
- **Workflow orchestration** (Rhythm) runs the `genomic_conversion` step sequence from config (`api/config/default.json`, `workers/workers/config/common.py`).
- **PostgreSQL** stores conversion records, argument values, worker process records, and structured logs.

### Objectives

- Execute conversion programs with validated, typed arguments tied to conversion definitions.
- Keep conversion execution asynchronous and observable through workflow status and logs.
- Materialize conversion outputs as first-class data products and link them back to the source dataset and conversion.
- Keep report access tokenized through secure_download-compatible URLs.

## Workflow Selection and Registry

When a conversion is created (`POST /conversions` or `POST /conversions/bulk`), the API chooses the workflow type using the conversion program name:

- If program name is in `genomic_conversion_programs`, workflow type is `genomic_conversion`.
- Otherwise workflow type is `conversion`.

For genomic programs, the workflow steps are:

1. `convert_genomic`
2. `copy_conversion_reports`
3. `derive_data_products`

All three steps are configured on the conversion queue (`cmg-bioloop-conversion...q`) in both API and worker config.

## Request Flow

### 1) Create Conversion

`POST /conversions` validates:

- conversion definition exists
- input dataset exists
- dataset type is allowed by `conversion_definition.dataset_types`
- argument values conform to each argument schema (`STRING`/`NUMBER`/`BOOLEAN`, allowed values, min/max, required/positional rules)

Then API:

- creates `conversion` + `argument_value` rows
- stores `metadata.origin` (defaults to `bioloop`, mergeable from request metadata)
- optionally creates `process_request` / `process_artifact` rows (only when `platform_based_execution` is enabled)
- creates Rhythm workflow
- stores workflow association in `workflow` table
- writes `conversion.workflow_id`

`POST /conversions/bulk` uses the same validation/creation logic in batches.

### 2) Execute `convert_genomic`

`convert_genomic` performs:

- fetch conversion with dataset + definition
- enforce 0 or 1 process requests
- resolve conversion output path with:
  - run dir: `definition.output_directory / conversion.id`
  - output dir: `run_dir / dataset.name`
- require input dataset to already be staged (`dataset.is_staged`)
- build command args from stored argument list, then append:
  - `--runfolder-dir <dataset.staged_path>`
  - `--output-dir <conversion_output_dir>`

Special argument handling:

- `--sample-sheet` content is written to `<dataset.staged_path>/<dataset_id>_samplesheet.csv`, and the command gets that file path.
- unsupported flag `--delete-undetermined` is dropped.
- flags with `None` values are skipped.

Execution path:

- **Local path:** if no process request (or `platform_based_execution` feature is off), execute locally.
- **Platform path:** if process request exists and feature is enabled, task attempts external execution flow (SLURM path currently implemented in code).

This split is intentional: external platform execution is an optional extension, not a core requirement of conversion workflows.

### 3) Copy Reports (`copy_conversion_reports`)

After conversion, worker copies `Reports` from:

- source: `<conversion_output_dir>/Reports`
- destination: `<workers config paths.conversion.reports>/<conversion_id>/<dataset_name>/Reports`

If destination exists, it is replaced.

### 4) Derive Data Products (`derive_data_products`)

Worker scans conversion output and creates data products via bulk dataset API.

Key behavior:

- if `*.fastq.gz` exist at output root, worker creates `Conversion-<conversion_id>-<dataset_name>` and moves those files into it
- otherwise it uses subdirectories containing `_` in name as product directories
- each product dataset is created as:
  - `type: DATA_PRODUCT`
  - `create_method: CONVERSION`
  - `origin_path: <output_dir>`
- `metadata.origin` is propagated from conversion origin (`legacy` vs `bioloop`)
- `dataset_hierarchy` links are created with `metadata.conversion_id = <conversion_id>`
- integrated workflows are started for derived products

## Data Model

Core models:

- `conversion_definition` - pipeline metadata, allowed dataset types, output/log directory metadata, and `capture_logs`.
- `cmd_line_program` + `argument` - executable metadata and argument schema.
- `conversion` - conversion instance, input dataset, initiator, workflow link, metadata.
- `argument_value` - persisted argument values for a conversion.
- `process_request` + `process_artifact` - optional platform-execution metadata.
- `workflow` - workflow association row for Rhythm workflow ID.
- `worker_process` + `log` - process and line-level logs used by conversion log viewing.

## Logging: Storage and UI Visibility

### Where Conversion Logs Are Stored

Runtime conversion logs shown in conversion detail are stored in PostgreSQL, not read from `conversion_definition.logs_directory`.

Path in code:

1. `convert_genomic` checks `conversion_definition.capture_logs`.
2. If `capture_logs=true` and execution is local, it calls `cmd.execute_with_log_tracking(...)`.
3. `execute_with_log_tracking`:
   - registers a `worker_process` (`POST /workflows/processes`)
   - streams stdout/stderr lines
   - writes log batches (`POST /workflows/processes/:process_id/logs`)
4. API persists these in:
   - `worker_process` table (workflow/process metadata)
   - `log` table (timestamp, level, message, worker_process_id)

What determines whether logs exist:

- `conversion_definition.capture_logs` must be true
- conversion must have a `workflow_id`
- worker process registration/log posting must succeed
- local execution path is used (non-local paths do not use `execute_with_log_tracking` in `convert_genomic`)

About `logs_directory`:

- `conversion_definition.logs_directory` exists in schema and is displayed in UI forms/details.
- current conversion log viewer (`GET /conversions/:id/logs`) does not read files from that path; it reads DB logs via `workflow_id -> worker_process -> log`.

### How Logs Become Viewable in UI

UI flow in `ConversionView.vue`:

1. On mount, UI calls in parallel:
   - `GET /conversions/:id?include_dataset=true&include_definition=true`
   - `GET /conversions/:id/logs`
2. Logs endpoint resolves:
   - conversion by `id` to get `workflow_id`
   - all `worker_process` rows for that workflow
   - all `log` rows for those process IDs (ordered by timestamp asc)
3. UI renders `log.message` lines in the Run Info card and in an expand modal.
4. Logs section is shown only when `logs.length > 0`.

## Reports Access Flow

`GET /conversions/:id/reports` returns tokenized report URLs used by UI "View Reports".

- API determines `conversionIdentifier` (`cmg_id` for legacy, `id` otherwise) and dataset name.
- API builds absolute reports path:
  - `${CONVERSION_OUTPUT_DIR}/${conversionIdentifier}/${dataset_name}/Reports`
  - `CONVERSION_OUTPUT_DIR` comes from env or `config.conversion.output_dir`.
- API issues token and returns URL paths with `?access_token=...`.
- UI prefixes with `VITE_UPLOAD_API_BASE_PATH` and opens in new tab.

## Access Control

- API conversion routes use `accessControl('conversion')`.
- Role grants currently allow conversion access for admin/operator; user role has no conversion resource grants.
- UI conversion pages are role-guarded with `requiresRoles: ["operator", "admin"]`.

---

## Deployment Notes

### 1) Conversion Worker Must Run the Conversion App

Ensure a worker process is running:

- Celery app: `workers.conversions_app`
- queue: `cmg-bioloop-conversion.<app_id>.q`
- tasks available: `convert_genomic`, `copy_conversion_reports`, `derive_data_products`

If this worker is down or listening on the wrong queue, genomic conversion workflows will stall at queued/running states without task progress.

### 2) Keep API and Worker Workflow Registry in Sync

`genomic_conversion` must match between:

- `api/config/default.json` (workflow creation source)
- `workers/workers/config/common.py` (task execution registry)

Step names can differ cosmetically, but task names and order must stay aligned.

### 3) Validate Runtime Paths and Program Binaries

Required path dependencies:

- each genomic `conversion_definition.output_directory` must exist and be writable by conversion workers
- executable references must be valid:
  - `program.executable_directory` (if set) must exist
  - `program.executable_path` must resolve to a file
- source datasets must be staged before conversion (`dataset.is_staged=true`)

### 4) Report URL Path Configuration

API report URL generation uses:

- env `CONVERSION_OUTPUT_DIR` (mapped in `api/config/custom-environment-variables.json`)
- fallback `config.conversion.output_dir` (default `/opt/sca/data/conversions`)

UI opens returned paths under:

- `VITE_UPLOAD_API_BASE_PATH` (secure_download base URL)

### 5) Copy-Reports Path vs Report-URL Path

Current code has two report location concepts:

- copy step destination: `workers config paths.conversion.reports/.../Reports`
- report URL base: `CONVERSION_OUTPUT_DIR/.../Reports`

Deployments should verify these map to the same real report tree (or intentionally accept divergence). If not aligned, "View Reports" can succeed at API layer but point to a directory that does not contain copied reports.

### 6) Platform-Based Execution Flag

External platform execution is feature-gated and disabled by default:

- API: `enabled_features.platform_based_execution`
- UI: `enabledFeatures.platformBasedExecution`
- Workers: `enabled_features.platform_based_execution`

With flag disabled, conversions execute locally regardless of `process_request` payloads.
This means conversion feature availability does not depend on SLURM being configured or reachable.

---

## Post-Deploy Smoke Test

1. Create a genomic conversion for a staged dataset.
2. Verify conversion appears in `/conversions` and receives a workflow ID.
3. Open conversion detail and confirm:
   - output directory resolves as expected
   - logs appear (if `capture_logs=true` for definition)
4. Confirm derived data products are created and linked in "Derived Datasets".
5. Click "View Reports" and verify report HTML loads with nested assets.

