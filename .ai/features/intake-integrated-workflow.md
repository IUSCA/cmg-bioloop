# intake_integrated Workflow Feature Changelog

## Overview

The `intake_integrated` workflow handles end-to-end processing for datasets ingested via `watch.py` (instrument ingestion on the archive node). It is defined in `api/config/default.json` under `workflow_registry.intake_integrated`.

---

## 2026-03-05

- Fix: `intake_integrated` workflow did not show the linked dataset on the `/workflows` page and did not appear in the workflows list on the `/datasets/:id` page.
  - Root cause: `intake_integrated` was missing from `WORKFLOW_SUBJECT_CONFIGS` in `ui/src/constants.js`, so `resolveWorkflowSubject()` returned `null` and no dataset link was rendered.
  - Additionally, `INTAKE_INTEGRATED` was absent from `CONSTANTS.WORKFLOWS` in `api/src/constants.js`, and `intake_integrated` was not in the allowed list for `POST /datasets/:id/workflow/:wf`, preventing the worker from registering the workflow association through the standard API path.
  - Affected files: `ui/src/constants.js`, `api/src/constants.js`, `api/src/routes/datasets/index.js`, `ui/src/components/dataset/Dataset.vue`.
  - Trigger: `intake_integrated` workflow created by watcher worker on the archive node.
- Decision: `intake_integrated` uses `dataset_id` as its subject key (same pattern as `integrated`, `stage`, `stage_migrated`, `delete`).
- Decision: `handleBrowseFilesClick` in `Dataset.vue` now also blocks staging/browsing when an `intake_integrated` workflow is active, consistent with the behavior for `integrated`.
- Constraint: For `intake_integrated` workflows to appear on `/datasets/:id`, the worker must create the DB association — either by passing `workflow_id` when calling `POST /datasets` or by calling `POST /datasets/:id/workflows` after creating the workflow in Rhythm. The API now supports both paths.
