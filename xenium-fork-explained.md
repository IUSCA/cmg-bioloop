# Xenium Fork — Integration Reference for cmg-bioloop

This document describes what is **unique to xenium** and what a receiving bioloop fork needs to handle differently when xenium's data and worker code are merged into it. It covers worker workflows, filesystem paths, artifacts, API changes, and migration considerations.

---

## Background

Xenium is a deployment of bioloop tailored for the [10x Genomics Xenium](https://www.10xgenomics.com/instruments/xenium-analyzer) spatial transcriptomics instrument at IU/CMG. It shares the same database schema, frontend, and core API as bioloop but differs in:

- How datasets are discovered and registered (two-tier RAW_DATA → DATA_PRODUCT structure)
- The instrument-specific analysis summary artifact it surfaces
- Its two-queue worker architecture (archive node + fetch node)
- The specific workflow steps in its `integrated` pipeline

When xenium data is migrated into cmg-bioloop, the receiving fork must support all of the above.

---

## Dataset Structure

Xenium data arrives as a **two-tier hierarchy**:

```
RAW_DATA/
└── {run_name}/              ← registered as RAW_DATA dataset
    ├── {cell_type_A}/       ← registered as DATA_PRODUCT dataset
    ├── {cell_type_B}/       ← registered as DATA_PRODUCT dataset
    └── ...
```

- `watch.py` observes `/zpool/xenium/` and registers new top-level directories as **RAW_DATA** datasets.
- RAW_DATA datasets are not processed individually — they immediately trigger `subdir_wf_initiator`.
- `subdir_wf_initiator` inspects the RAW_DATA `origin_path`, registers each subdirectory as a **DATA_PRODUCT**, and launches the `integrated` workflow for each.
- **The `integrated` workflow is the main processing pipeline and runs only on DATA_PRODUCT datasets.**

### Dataset `origin_path` values

| Dataset type | `origin_path` |
|---|---|
| RAW_DATA | `/zpool/xenium/{run_name}` |
| DATA_PRODUCT | `/zpool/xenium/{run_name}/{subdir_name}` |

---

## Workflows

### `subdir_wf_initiator`

Triggered on RAW_DATA. Runs on the archive queue.

| Step | Task | Queue |
|---|---|---|
| await stability | `await_stability` | `archive.*.q` |
| initiate dataproduct workflows | `initiate_subdir_workflows` | `archive.*.q` |

### `integrated` (DATA_PRODUCT pipeline)

The core processing workflow. Steps span both queues.

| Step | Task | Queue | Filesystem |
|---|---|---|---|
| await stability | `await_stability` | archive | `origin_path` (zpool, source) |
| inspect | `inspect_dataset` | archive | `origin_path` (zpool, source) |
| parse analysis data | `parse_analysis_data` | archive | `origin_path/analysis_summary.html` (zpool, source) |
| archive | `archive_dataset` | archive | `origin_path` → bundle at `/zpool/users/cmguser/xenium/{name}.tar` → SDA |
| stage | `stage_dataset` | fetch | SDA → `/N/scratch/…/stage/data_products/{alias}/{name}` |
| validate | `validate_dataset` | fetch | Slate scratch (staged path) |
| setup download | `setup_dataset_download` | fetch | Creates symlinks in `/N/scratch/…/download/` |
| upload static content | `upload_static_content` | fetch | `staged_path/analysis_summary.html` → `PUT /api/datasets/{id}/report` |

### `stage` (on-demand re-stage)

| Step | Task | Queue |
|---|---|---|
| stage | `stage_dataset` | fetch |
| validate | `validate_dataset` | fetch |
| setup download | `setup_dataset_download` | fetch |

### `delete`

Runs on archive queue via `delete_dataset` task (SDA deletion).

### `upload_integrated` / `process_dataset_upload` / `cancel_dataset_upload`

Standard upload workflows, same as core bioloop. Run on fetch queue.

---

## Filesystem Paths (Production)

Both the archive node and fetch node have access to the same filesystem spaces via mounts.

```python
'paths': {
    'archive_scratch': '/zpool/users/cmguser/xenium',    # tar bundle generation
    'scratch':         '/N/scratch/cmguser/xenium/production/scratch',
    'RAW_DATA': {
        'archive': 'archive/{YEAR}/raw_data',            # SDA path
        'stage':   '/N/scratch/cmguser/xenium/production/stage/raw_data',
    },
    'DATA_PRODUCT': {
        'archive': 'archive/{YEAR}/data_products',       # SDA path
        'stage':   '/N/scratch/cmguser/xenium/production/stage/data_products',
        'bundle': {
            'generate': '/zpool/users/cmguser/xenium',
            'stage':    '/N/scratch/cmguser/xenium/production/stage/data_products/bundles',
        },
    },
    'download_dir': '/N/scratch/cmguser/xenium/production/download',
    'root':         '/N/scratch/cmguser/xenium',
},
'registration': {
    'RAW_DATA': {
        'source_dir': '/zpool/xenium/',
    },
},
```

### Which tasks touch which filesystem

| Task | Needs zpool (`/zpool/`) | Needs Slate scratch (`/N/scratch/`) | Needs SDA |
|---|---|---|---|
| `await_stability` | ✅ (reads `origin_path`) | | |
| `inspect_dataset` | ✅ (reads `origin_path`) | | |
| `parse_analysis_data` | ✅ (reads `origin_path/analysis_summary.html`) | | |
| `archive_dataset` | ✅ (reads source + writes bundle) | | ✅ (upload) |
| `stage_dataset` | | ✅ (write staged path) | ✅ (download) |
| `validate_dataset` | | ✅ (reads staged path) | |
| `setup_dataset_download` | | ✅ (creates symlinks in download_dir) | |
| `upload_static_content` | | ✅ (reads `staged_path/analysis_summary.html`) | |
| `initiate_subdir_workflows` | ✅ (reads `origin_path` subdirs) | | |

---

## Two-Queue Worker Architecture

Xenium uses two separate Celery apps pointed at two separate queues, running on two nodes:

| App | Queue name | Node role | Tasks registered |
|---|---|---|---|
| `archive_celery_app` | `archive.xenium.sca.iu.edu.q` | Archive node (zpool access) | `archive_dataset`, `delete_dataset`, `inspect_dataset`, `await_stability`, `initiate_subdir_workflows`, `parse_analysis_data`, `mark_archived_and_delete`, `delete_source` |
| `fetch_celery_app` | `fetch.xenium.sca.iu.edu.q` | Fetch node (Slate scratch access) | `stage_dataset`, `validate_dataset`, `setup_dataset_download`, `upload_static_content`, `archive_dataset`, `inspect_dataset`, `await_stability`, `process_dataset_upload`, `cancel_dataset_upload`, `parse_analysis_data` |

Note that some tasks (`archive_dataset`, `inspect_dataset`, `await_stability`, `parse_analysis_data`) are registered in **both** apps. This is intentional — the workflow registry routes each step to the correct queue explicitly, and both nodes need to be able to execute the full task set for different workflow types (e.g., `upload_integrated` runs entirely on the fetch queue).

The receiving fork already supports multi-node worker architecture, so the queue names and PM2 ecosystem configs need to be extended, not redesigned.

---

## `parse_analysis_data` — Xenium-Specific Task

Reads the `analysis_summary.html` from the DATA_PRODUCT's `origin_path` (on the source zpool, **before archival**), parses embedded JSON from a `<script>` tag using BeautifulSoup, and stores the result in `dataset.metadata.analysis`:

```python
{
    'metadata': {
        'analysis': {
            'region_info': { ... },   # parsed from analysis_summary.html
            'run_info':    { ... },
        }
    }
}
```

This is optional/best-effort — if `analysis_summary.html` does not exist, the task silently proceeds.

**Python dependencies required:** `beautifulsoup4`, `python-slugify`

---

## `upload_static_content` — Xenium-Specific Task

Reads `analysis_summary.html` from the **staged** DATA_PRODUCT path (on Slate scratch, **after staging**), then uploads it to the API server via `PUT /api/datasets/{id}/report`.

The upload creates or reuses a UUID (`analysis_summary_file_dir`) stored in `dataset.metadata.analysis_summary_file_dir`. This UUID is the subdirectory name used on the API server, obfuscating the real file path.

Flow:
```
staged_path/analysis_summary.html
    → PUT /api/datasets/{id}/report (multipart)
    → API stores at:  reports/{analysis_summary_file_dir}/analysis_summary.html
    → Served at URL:  /reports/{analysis_summary_file_dir}/analysis_summary.html
```

This is also best-effort — if `analysis_summary.html` is not present in the staged path, the task silently proceeds.

---

## `initiate_subdir_workflows` — Xenium-Specific Task

Reads subdirectories of a RAW_DATA dataset's `origin_path` and for each:

1. Registers it as a DATA_PRODUCT dataset (idempotent — skips if already exists)
2. Creates a source→derived association between the RAW_DATA and the DATA_PRODUCT
3. Launches the `integrated` workflow on the new DATA_PRODUCT

This is the only way DATA_PRODUCT datasets are created in xenium — the DATA_PRODUCT observer in `watch.py` is commented out.

---

## The Analysis Summary Artifact

### `analysis_summary_file_dir` (xenium) vs `report_id` (core bioloop)

These are parallel mechanisms for the same concept — a UUID stored in `dataset.metadata` that names a directory on the API server under which a report HTML file is uploaded and served.

| | Xenium | Core bioloop |
|---|---|---|
| Metadata key | `dataset.metadata.analysis_summary_file_dir` | `dataset.metadata.report_id` |
| Filename stored | `analysis_summary.html` | `multiqc_report.html` |
| Set by | `upload_static_content` task | `generate_qc` task |
| Source | Xenium instrument output (embedded JSON in HTML) | FastQC + MultiQC on sequencing reads |

They **do not conflict** at the database level — both keys live in the `dataset.metadata` JSONB column and apply to different datasets. A xenium DATA_PRODUCT will have `analysis_summary_file_dir`; a sequencing dataset will have `report_id`.

### The API route conflict

The `PUT /api/datasets/:id/report` endpoint checks for the relevant metadata key and sets the storage path and filename. **Each fork currently handles only its own key.** After merging, this route must handle both:

```
if dataset.metadata.analysis_summary_file_dir:
    → store as reports/{analysis_summary_file_dir}/analysis_summary.html

if dataset.metadata.report_id:
    → store as reports/{report_id}/multiqc_report.html
```

Without this change, the `upload_static_content` worker step will fail when called against the merged API (it will get "report_id is not set").

### Migrating existing report files

The already-uploaded `analysis_summary.html` files live on the **xenium API server's local filesystem** at `reports/{uuid}/analysis_summary.html`, not on any shared NFS. These need to be copied to the same path on cmg-bioloop's API server. The DB metadata (`analysis_summary_file_dir` values) will be preserved during the row-level data migration, and the `/reports` static-serve route is identical in both forks, so the URLs will resolve correctly once the files are in place.

---

## API Changes Required in the Receiving Fork

| Area | Change needed |
|---|---|
| `PUT /api/datasets/:id/report` | Handle `analysis_summary_file_dir` metadata key and `analysis_summary.html` filename alongside existing `report_id` / `multiqc_report.html` logic |
| `GET /api/alerts` | This route exists but is **not registered** in xenium's router — verify it is registered in cmg-bioloop's router (it should already be) |
| `GET /api/datasets` | Xenium doesn't pass `include_audit_logs` or `include_projects` — no breaking change, but these params will now work on xenium datasets too once in the merged app |
| Dataset `description` field | Xenium doesn't use it; bioloop does. No conflict — xenium datasets will just have a null description |

---

## `watch.py` Changes for cmg-bioloop

Add an observer for xenium RAW_DATA alongside any existing observers:

```python
obs_xenium = Observer(
    name='xenium_raw_data_obs',
    dir_path='/zpool/xenium/',
    callback=Register('RAW_DATA', default_wf_name='subdir_wf_initiator').register,
    interval=config['registration']['poll_interval_seconds'],
    full_scan_every_n_scans=config['registration']['full_scan_every_n_scans'],
)
poller.register(obs_xenium)
```

The `registration.RAW_DATA.rejects` list should exclude `.snapshots`, `temp`, and any other xenium-specific noise directories (review `common.py`).

---

## Workflow Registry Entries Required

The following workflow definitions must exist in cmg-bioloop's `workflow_registry` config with the correct queue assignments for xenium:

```json
"subdir_wf_initiator": {
  "name": "SubDir Workflow Initiator",
  "steps": [
    { "name": "await stability",              "task": "await_stability",            "queue": "archive.*.q" },
    { "name": "initiate dataproduct workflows","task": "initiate_subdir_workflows",  "queue": "archive.*.q" }
  ]
},
"integrated": {
  "steps": [
    { "name": "await stability",    "task": "await_stability",        "queue": "archive.*.q" },
    { "name": "inspect",            "task": "inspect_dataset",        "queue": "archive.*.q" },
    { "name": "parse_analysis_data","task": "parse_analysis_data",    "queue": "archive.*.q" },
    { "name": "archive",            "task": "archive_dataset",        "queue": "archive.*.q" },
    { "name": "stage",              "task": "stage_dataset",          "queue": "fetch.*.q"   },
    { "name": "validate",           "task": "validate_dataset",       "queue": "fetch.*.q"   },
    { "name": "setup_download",     "task": "setup_dataset_download", "queue": "fetch.*.q"   },
    { "name": "upload_static_content","task":"upload_static_content", "queue": "fetch.*.q"   }
  ]
}
```

If cmg-bioloop's `integrated` workflow differs (e.g., includes `generate_qc`), xenium's version must either be a separate named workflow (e.g., `xenium_integrated`) or the step list must be made conditional. The cleanest approach is a separate named workflow launched by `initiate_subdir_workflows`.

---

## Binaries Required on Worker Nodes

| Binary | Used by | Notes |
|---|---|---|
| `tar` (GNU, `--sparse` support) | `archive_dataset` | Standard, must support sparse files |
| `du` | `inspect_dataset` | Standard |
| SDA client (`hsi`/`htar`) | `archive_dataset`, `stage_dataset` | IU SDA access required |
| `quota` | `scripts/metrics.py` | IU HPC-specific, requires `module load quota` |
| `lfs` | `scripts/metrics.py` (via `hpfs.py`) | Lustre FS client for `/N/scratch` quota |
| `fastqc` | `tasks/qc.py` (`generate_qc` task) | **Not used in any xenium workflow** — already present in cmg-bioloop for sequencing |
| `multiqc` | `tasks/qc.py` | Same — not used for xenium data |

---

## Python Dependencies to Add

```toml
beautifulsoup4 = "^4.12.2"   # parse_analysis_data task
python-slugify = "^8.0.1"    # utility, used in xenium worker code
```

---

## Data Migration Checklist

- [ ] Migrate all xenium PostgreSQL table rows into cmg-bioloop's corresponding tables
- [ ] Verify `dataset.metadata.analysis_summary_file_dir` values are preserved on all xenium DATA_PRODUCT rows
- [ ] Copy `reports/` directory contents from xenium API server → cmg-bioloop API server (preserving UUID subdirectory structure)
- [ ] Migrate MongoDB workflow documents (xenium workflows → cmg-bioloop MongoDB instance)
- [ ] Add `xenium/analyse.py` and `hpfs.py` to cmg-bioloop workers
- [ ] Add `initiate_subdir_workflows`, `parse_analysis_data`, `upload_static_content` tasks to the appropriate Celery app(s)
- [ ] Add `subdir_wf_initiator` and xenium-flavored `integrated` workflow definitions to config
- [ ] Add xenium observer to `watch.py`
- [ ] Update `PUT /api/datasets/:id/report` to handle both `analysis_summary_file_dir` and `report_id`
- [ ] Add `beautifulsoup4` and `python-slugify` to `pyproject.toml`
- [ ] Update worker production config paths to include xenium path entries
- [ ] Register `/alerts` route in API router if not already (missing in xenium, should be present in cmg-bioloop)
- [ ] Verify xenium's authentication config (IU CAS as primary provider, signup disabled)
