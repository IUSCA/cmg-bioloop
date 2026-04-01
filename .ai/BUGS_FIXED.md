# Bugs Fixed

Structured log of bugs found and fixed by AI agents in this repo.
Load this file when writing e2e tests — filter by `E2E candidate: yes`.

Format per entry:
- **Symptom** — what the user/system observed
- **Root cause** — why it happened
- **Fix** — what changed
- **Files** — affected files
- **E2E candidate** — yes/no + reasoning

---

## BUG-001 · 2026-03-01 · conversions

**Symptom:** All conversions against the same input dataset show the same list of derived datasets, regardless of which conversion produced them.

**Root cause:** `GET /conversions/:id/derived_datasets` filtered `dataset_hierarchy` by `source_id = conversion.dataset_id` only. No per-conversion discriminator existed in the data, so all conversions sharing the same input dataset returned the same rows.

**Fix:** Store `conversion_id` in `dataset_hierarchy.metadata` when creating hierarchy entries. Filter by `metadata.conversion_id` instead of `source_id`. Removed the now-redundant `metadata.derivation_method` field entirely, replacing its use in the UI chip with `dataset.create_method`.

**Files:**
- `workers/workers/tasks/derive_data_products.py`
- `api/src/routes/conversions/index.js`
- `api/src/services/dataset.js`
- `api/src/routes/datasets/index.js`
- `data_sync/src/sync/bigbang/sync_dataset_hierarchies.js`
- `data_sync/src/bigbang_cmg_sync.js`
- `ui/src/components/dataset/AssocDatasetList.vue`

**E2E candidate:** yes
**Why:** Easy to reproduce: run two conversions against the same input dataset, verify each `/conversions/:id` view shows only its own derived datasets. Brittle because the query looks correct at a glance but silently returns wrong scope.

---

## BUG-002 · 2026-02-27 · conversions

**Symptom:** Derived data products created from Bioloop-UI conversions had `metadata.origin = 'legacy'`, causing the integrated workflow's inspect step to fail — it tried to read from a legacy extracted archive path that does not exist for Bioloop-native data products.

**Root cause:** `derive_data_products.py` propagated `origin: 'legacy'` to all derived data products whenever `config.legacy_application_active` was `True`, regardless of whether the conversion was submitted via Bioloop UI or the legacy CMG sync scripts.

**Fix:** Read `conversion.metadata.origin` and propagate it to derived data products. Default to `'bioloop'` if absent. API layer now sets `metadata.origin = 'bioloop'` on all conversions created via `POST /conversions`, with an override hook for future legacy poller scripts.

**Files:**
- `workers/workers/tasks/derive_data_products.py`
- `api/src/routes/conversions/index.js`

**E2E candidate:** yes
**Why:** Run a conversion via Bioloop UI, verify derived data products have `metadata.origin = 'bioloop'` and their integrated workflow completes without inspect step failure.

---

## BUG-003 · 2026-01-27 · conversions

**Symptom:** "View Reports" button opened a broken page — redirected to the core API instead of the secure_download microservice, and authentication failed (401) because the bearer token wasn't included in the URL.

**Root cause:** UI was calling a legacy endpoint that served files directly from the core API. Path construction also used `dataset.id` instead of `dataset.name`, producing wrong filesystem paths.

**Fix:** UI calls `GET /conversions/:id/reports` which generates a JWT-scoped token and returns a fully-formed URL (with token embedded as `?access_token=`). Path now uses `dataset.name` to match the actual filesystem layout.

**Files:**
- `api/src/routes/conversions/index.js`
- `api/src/routes/reports/conversions.js`
- `ui/src/components/conversion/ConversionView.vue`
- `ui/src/services/conversion/api.js`
- `secure_download/src/routes/reports.js`

**E2E candidate:** yes
**Why:** Click "View Reports" on a conversion, verify the page opens in a new tab and the HTML report loads (not a 401 or 404).

---

## BUG-004 · 2026-01-27 · conversions

**Symptom:** The report HTML page loaded but appeared unstyled and broken — CSS, JS, and nested HTML frames returned 401 errors.

**Root cause:** The initial `index.html` request carried the auth token as a query parameter. When the browser then fetched nested resources (`style.css`, `tree.html`, etc.), it made plain requests without the token. Cookie-based auth couldn't be used because the secure_download service runs on a different domain.

**Fix:** `secure_download` intercepts HTML responses and rewrites all relative `href` and `src` attribute values in-memory to append `?access_token=<token>` before sending to the browser.

**Files:**
- `secure_download/src/routes/reports.js`

**E2E candidate:** yes
**Why:** Open a conversion report, verify the page is fully styled and all sub-resources load without auth errors. Brittle because it only fails cross-domain — easy to miss in local dev where same-origin doesn't expose the issue.

---

## BUG-005 · 2026-01-16 · cmg-database-migration (bigbang)

**Symptom:** Bigbang migration crashed at runtime on `cmg_id` lookups.

**Root cause:** `findUnique()` was used on `cmg_id` fields, which have no unique constraint in the Prisma schema. Prisma 5 throws if `findUnique()` is called on a non-unique field.

**Fix:** Changed all `cmg_id` lookups to `findFirst()` across 12+ bigbang/poller files.

**Files:** Multiple files in `data_sync/src/sync/`

**E2E candidate:** no
**Why:** Internal bigbang infrastructure. Not user-observable in the application UI.

---

## BUG-006 · 2026-01-16 · cmg-database-migration (bigbang)

**Symptom:** Bigbang created 0 import log records.

**Root cause:** Logic looked up dataproducts by iterating uploads and searching for a matching dataproduct — wrong direction. In CMG, uploads *create* dataproducts (one-to-many), so the correct lookup is: find dataproducts where `dataproduct.upload == cmgUpload._id`.

**Fix:** Reversed the lookup direction.

**Files:** `data_sync/src/sync/bigbang/sync_import_logs.js`

**E2E candidate:** no
**Why:** Bigbang internal. Observable only by checking import log count in DB after migration.

---

## BUG-007 · 2026-01-16 · cmg-database-migration (bigbang)

**Symptom:** Bigbang created 0 dataset hierarchy records.

**Root cause:** Skip logic was too aggressive — detailed logging revealed all records were being skipped due to missing parent/child lookups that were themselves failing silently.

**Fix:** Added granular skip-reason tracking; fixed underlying lookup failures. Result: 5,279 hierarchies created.

**Files:** `data_sync/src/sync/bigbang/sync_dataset_hierarchies.js`

**E2E candidate:** no
**Why:** Bigbang internal. Detectable only by inspecting `dataset_hierarchy` table row count post-migration.

---

## BUG-008 · 2026-01-16 · cmg-database-migration (bigbang)

**Symptom:** Documents updated in CMG *during* a bigbang run were silently skipped by pollers after migration completed (data loss).

**Root cause:** Cursor initialization set `last_cmg_objectid = null`. Pollers queried `updatedAt > cursor`, but documents updated at exactly the same timestamp as the cursor could be missed. Setting cursor to null made the problem worse.

**Fix:** Cursor initialization now retrieves and stores the actual `_id` of the latest document per collection. Poller query uses `updatedAt > cursor OR (updatedAt = cursor AND _id > objectId)`.

**Files:** `data_sync/src/` (cursor init logic)

**E2E candidate:** no
**Why:** Race condition in sync infrastructure. Not testable via UI e2e.

---

## BUG-009 · 2026-01-16 · cmg-database-migration (bigbang)

**Symptom:** Bigbang crashed with "JavaScript heap out of memory" on large collections.

**Root cause:** Collections were loaded entirely into memory via `.toArray()` before processing. Datasets (~2000 items) and audit logs (~7000+ events) exhausted the default Node.js heap.

**Fix:** Replaced `.toArray()` with MongoDB cursor streaming (`for await (const doc of cursor)`). Added batch processing (50 datasets/batch) and `global.gc()` hints.

**Files:** Multiple bigbang sync files in `data_sync/src/sync/bigbang/`

**E2E candidate:** no
**Why:** Infrastructure performance issue. Not user-observable in application UI.

---

## BUG-011 · 2026-03-01 · sessions / genome browser

**Symptom:** "Some legacy datasets are still being migrated. Please wait for migration to complete." toast appeared even for sessions that were already hydrated (had tracks associated), blocking access to the genome browser.

**Root cause:** Two-layer failure:
1. `PATCH /sessions/:id` never read or applied the `metadata` field from the request body. When `finish_session_hydration` (worker task) called `api.update_session(session_id, { metadata: { is_hydrated: True } })`, the PATCH handler silently discarded it — `metadata.is_hydrated` was never written to the database.
2. Because `metadata.is_hydrated` was always `undefined`, `isSessionHydrated()` (which calls `GET /legacy/migrations/sessions/:id` and checks `session.metadata.is_hydrated === true`) always returned `false`, even for fully-hydrated sessions.
3. With `isHydrated = false`, `handleViewInBrowser()` entered the migration-check branch. The sessions API — which uses the `hydrate_session` workflow status to determine hydration — correctly served datasets from `session_tracks` without `migration_status` attached. Without `migration_status`, `allMigrated` evaluated to `false`, triggering the toast.

**Fix:** Added `metadata` field support to `PATCH /sessions/:id`. Uses a shallow merge (`{ ...existingMetadata, ...incomingMetadata }`) so that `origin`, `datasets`, and other existing keys are preserved while `is_hydrated: true` is correctly written through.

**Files:**
- `api/src/routes/sessions.js`

**E2E candidate:** yes
**Why:** Open a legacy session whose hydration workflow already completed, click "View in Genome Browser" — should open the browser-selection modal, not show the migration warning toast.

---

## BUG-010 · 2026-01-26 · cmg-database-migration (pollers)

**Symptom:** Entity `metadata` fields (on datasets, sessions, etc.) contained `cmg_sync_state: { cmg_updated_at, last_sync_time }` — sync infrastructure data polluting application-level metadata.

**Root cause:** All pollers were writing their sync tracking state directly into entity `metadata` JSON fields, overwriting any Bioloop-native metadata that was already there.

**Fix:** Removed all `metadata` field updates from pollers. Sync state is tracked exclusively in the `cmg_sync_cursor` table.

**Files:** All poller files in `data_sync/src/sync/pollers/`

**E2E candidate:** no
**Why:** Not directly visible in the UI. Could assert via API that dataset/session metadata doesn't contain sync-state keys, but low value as a UI e2e test.

---
