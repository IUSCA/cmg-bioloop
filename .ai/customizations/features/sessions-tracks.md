# Sessions & Tracks Feature

**Feature Scope:** Genome browser sessions and tracks for visualizing genomic data in IGV and WashU browsers.

**Status:** Implemented

**Related Documentation:**
- `/docs/features/genome-browser-sessions.md` - Detailed architectural walkthrough

---

## 2026-01-16

### Initial State Documentation

**Context:** This feature enables users to create genome browser sessions that visualize genomic tracks (from dataset files) in IGV or WashU browsers.

**Key Architecture Decisions:**
- Decision: Sessions and tracks are separate but related entities
- Decision: One session can have multiple tracks (many-to-many via session_track)
- Decision: One dataset file can have at most one track
- Decision: Tracks reference dataset files, not datasets directly
- Decision: Genome information comes from dataset.genomic_details, not track table
- Constraint: Files must be staged before they can be accessed by genome browsers
- Constraint: All tracks in a session should have compatible genome types

**Database Schema:**
- `genome_browser_session`: Browser session configuration
  - Includes: title, description, genome_type, genome_value, browser_type
  - References: user (owner)
  - Has many: session_tracks (join table)
  
- `track`: Genome browser track configuration
  - Includes: name, track_type, color, track_specific_data (JSON)
  - References: dataset_file (one-to-one)
  - Has many: session_tracks (join table)
  - Note: Genome info accessed via track.dataset_file.dataset.genomic_details
  
- `session_track`: Join table linking sessions and tracks
  - Includes: session_id, track_id, display_order
  - Unique constraint: (session_id, track_id)

**File Serving Pattern:**
- Files served via `/api/sessions/:id/files/expose/*` routes
- Cookie-based authentication for genome browser access
- Compression DISABLED for binary genome files (critical)
- Range request support for efficient browser loading
- File paths constructed: `stage_alias + dataset_file.path`

**Browser Integration:**
- IGV: JavaScript library embedded in Vue
- WashU: React component embedded in Vue via React-in-Vue wrapper
- Both browsers support: BAM, BIGWIG, VCF_GZ, BED file types
- Browser selection: User chooses IGV or WashU when opening session

**Key Code Locations:**
- API Routes: `/api/src/routes/sessions.js`, `/api/src/routes/tracks.js`
- API Services: `/api/src/services/session.js`, `/api/src/services/track.js`
- File Exposure Router: `/api/src/routes/sessions.js` (fileExposureRouter)
- UI Components: `/ui/src/components/sessions/`, `/ui/src/components/tracks/`
- Browser Components: `/ui/src/components/genome-browser/`
- Prisma Schema: `/api/prisma/schema.prisma` (models: genome_browser_session, track, session_track)

**Critical Implementation Notes:**
1. **Router Mounting Order:** File exposure router MUST be mounted BEFORE global auth middleware
2. **Compression:** Disabled for `/files/expose/` routes (binary files break if compressed)
3. **Range Requests:** Supported for efficient file streaming
4. **Auto-Population:** Preserve manual user selections when auto-populating genome fields
5. **Track Selection:** Multiple track selection supported via async autocomplete
6. **File Restrictions:** Removed browser-compatible file restrictions - all files can be added

**Recent Changes (from memory):**
- Removed browserCompatibleExtensions concept (all files now allowed in sessions)
- Fixed genome field access (track doesn't have genome fields, accessed via relations)
- Enhanced track selection UI with multiple selection and display list
- Session validation allows empty genome values (flexible validation)
- Config consolidation: moved file type arrays to service config files

---

## 2026-03-01

- Fix (attempt 1): `handleViewInBrowser()` in `ui/src/pages/sessions/[id].vue` incorrectly showed "Some legacy datasets are still being migrated" toast even after all workflows completed.
  - Root cause: The migration check (`ds.migration_status.is_migrated`) ran against datasets returned from `session_tracks` after hydration. The API only attaches `migration_status` to datasets fetched from `metadata.datasets` (the pre-hydration path). Once the session is hydrated the API uses the `session_tracks`-only path, which does not attach `migration_status`, so `allMigrated` evaluated to `false`.
  - Fix: Check `isSessionHydrated` first. If the session is already hydrated, skip the migration-status check entirely and proceed to browser selection. The migration check is now guarded inside the `!isHydrated` branch, where it is meaningful as a prerequisite gate before showing the hydration modal.
- Fix (attempt 2, root cause): `PATCH /sessions/:id` in `api/src/routes/sessions.js` never read or applied the `metadata` field from the request body, so `finish_session_hydration` (worker task) could never persist `metadata.is_hydrated = true`. Because of this `isSessionHydrated()` always returned `false` for every session, making attempt 1's `isHydrated` check ineffective.
  - Fix: Added `metadata` body validator and shallow-merge logic to `PATCH /sessions/:id`: `{ ...existingMetadata, ...incomingMetadata }`. Existing keys (`origin`, `datasets`, etc.) are preserved; `is_hydrated: true` is now correctly written to the DB.

## 2026-03-01 (continued)

- Change: `GET /sessions/:id` now returns full Rhythm workflow objects in `session_workflows` (instead of partial `{ workflow_id, created_at, status, name }`). The enrichment now spreads the entire `enrichedWf` object from Rhythm, giving the UI `id`, `steps`, `steps_done`, `total_steps`, `updated_at`, etc. needed by `WorkflowCompact` and `Workflow` components.
- Change: Added a "WORKFLOWS" section to `ui/src/pages/sessions/[id].vue`. It is rendered only for legacy sessions (`session.metadata.origin === 'legacy'`) and displays all workflows from `session.session_workflows` using the same `collapsible` + `WorkflowCompact` + `Workflow` pattern used in the dataset detail view. Active workflows expand by default; auto-polling at `config.dataset_polling_interval` is started/stopped based on whether any session workflow is still running.

## 2026-03-01 (continued)

- Change: Added `dataset_id` query filter to `GET /conversions` (API) and `GET /sessions` (API) so both endpoints can be scoped to a specific dataset.
- Change: `GET /conversions` now accepts an `include_workflow_status` boolean query parameter. When `true`, the response enriches each conversion with a `workflow_status` field (batch-fetched from Rhythm by `workflow_id`).
- Change: `GET /sessions` `dataset_id` filter resolves via `session_tracks -> track -> dataset_file -> dataset_id` nested relation.
- Change: Added `DatasetConversions.vue` component that shows an "Associated Conversions" table on `/datasets/:id` for `RAW_DATA` type datasets. Status column: legacy conversions (detected via `conversion.metadata?.origin`) show a static green checkmark; non-legacy conversions fetch workflow statuses in a batch call to `/workflows` and render them via `WorkflowStatusIcon`.
- Change: Added `DatasetSessions.vue` component that shows an "Associated Sessions" table on `/datasets/:id` for `DATA_PRODUCT` type datasets. No Status column.
- Change: `Dataset.vue` renders Associated Conversions after the existing Associated Datasets section, gated on `dataset.type === 'RAW_DATA'`; renders Associated Sessions gated on `dataset.type === 'DATA_PRODUCT'`.

## 2026-03-01 (continued)

- Change: Removed the "Delete Track" feature entirely. The `DELETE /tracks/:id` API route has been removed. The `DeleteTrackModal.vue` component has been deleted. All delete UI (button in `TrackList.vue` actions column, Actions card in `pages/tracks/[id].vue`), the `deleteTrack` store action, and the `delete()` service method have been removed. Tracks are no longer deletable from the DB through the UI or API.
- Constraint: Tracks can still be associated with or disassociated from sessions (session creation flow is unchanged). Only the concept of permanently deleting a track record from the database has been removed.

## 2026-03-03

- Decision: Sessions and Tracks features are now accessible to the `user` role, not only `operator`/`admin`.
- Decision: Access control for user role is enforced at the DB query level in route handlers, not via the ACL layer alone. ACL layer grants `read:any`, `create:any`, `update:any`, `delete:any` to user role for sessions and tracks; route handlers use `userCanAccessAll(req.user)` (checks if `admin` or `operator` role) to decide between full access vs. user-scoped access.
- Constraint: User role can only access Sessions they own (`user_id === req.user.id`). Admin/operator can access all sessions.
- Constraint: User role can only access Tracks from datasets in projects they are a member of. Admin/operator see all tracks.
- Constraint: Track selection when creating or updating a session is filtered by project membership for user role.
- Constraint: `session_tracks` in `GET /tracks/:id` response is filtered to sessions owned by the requesting user when the user has user role.
- Constraint: `GET /sessions?dataset_id=X` (used by DatasetSessions component) returns only the user's own sessions for user role, so the Associated Sessions table in `/datasets/:id` is automatically user-scoped.
- Change: `req.permission.granted` replaced by explicit `userCanAccessAll(req.user)` helper throughout `sessions.js` and `tracks.js` to correctly distinguish admin/operator from user role (previously user role got 403 from middleware and the `req.permission.granted` else-branch was dead code).
- Change: Added `isPermittedTo('delete')` middleware to `DELETE /sessions/:id` (was unguarded before).
- Change: Added `isPermittedTo('read')` middleware and ownership check to `GET /sessions/:id/tracks` (was unguarded before).
- Change: Sessions and Tracks sidebar items moved from `operator_items` to `user_items` in `ui/src/constants.js`, gated on `feature_key: 'genome_browser'`.
- Change: `requiresRoles: ['operator', 'admin']` removed from route metadata in all Sessions and Tracks pages (`sessions/index.vue`, `sessions/new.vue`, `sessions/[id].vue`, `tracks/index.vue`, `tracks/[id].vue`).
- Change: `canEditSession` and `canDeleteSession` in `sessions/[id].vue` now also allow admin/operator in addition to session owner.
- Change: `canDeleteSession` in `SessionList.vue` now also allows admin/operator.
- Change: `userCanAccessAll` helper added at top of both `sessions.js` and `tracks.js` route files for reuse.

## 2026-03-03 (tracks ACL correction — read:own + track_access_check)

- Fix: Changed tracks user ACL from `read:any` to `read:own`. This correctly models "user can read their own tracks" where ownership is defined by project membership (user → project → dataset → dataset_file → track), matching the same indirect relationship that `datasets: read:own` uses.
- Added `track_access_check` middleware to `tracks.js`, mirroring `datasetService.dataset_access_check` exactly: admin/operator pass immediately; user role is checked against project membership via a Prisma query through the track → dataset_file → dataset → project → user chain; 403 if no access.
- `GET /tracks/:id` now uses `track_access_check` instead of `isPermittedTo('read')`. This matches the `GET /datasets/:id` pattern (no `isPermittedTo` — dedicated access-check middleware handles both role and possession checks).
- `GET /tracks` (general list): `isPermittedTo('read')` → `readAny` → user with `read:own` → 403. User role must use `GET /tracks/:username/all` (already uses `checkOwnership: true`). ✓
- UI: `useTracksStore.fetchTracks()` now routes to `GET /tracks/:username/all` for user role and `GET /tracks` for admin/operator, same pattern as sessions store.
- Design note: `read:own` for tracks is semantically correct because the library states "own requires you to also check for the actual possession" — ownership verification is done by `track_access_check` (item) and by handler-level project-membership filtering (`/:username/all`), not by the library itself.

## 2026-03-03 (ACL semantic correction)

- Fix: Replaced `:any` grants for sessions user role with semantically correct `:own` grants (`read:own`, `update:own`, `delete:own`; `create:any` kept because `POST /sessions` has no `:username` path param, matching the same pattern used for `datasets: create:any`).
- Fix: Removed `create:any` and `update:any` for tracks from user role entirely — user role should only READ tracks, not create/update them (operator/admin responsibility).
- Added `sessionOwnerFn` to `sessions.js` that resolves a session's owning username from the DB, used as the `resourceOwnerFn` argument to `isPermittedTo` on all `/:id` routes.
- Updated all session `/:id` routes to use `isPermittedTo('read'/'update'/'delete', { checkOwnership: true }, sessionOwnerFn)` — ownership enforcement is now in the middleware layer, consistent with the dataset/tracks pattern.
- Updated `GET /sessions/:username/all` to use `isPermittedTo('read', { checkOwnership: true })` — matches the `GET /tracks/:username/all` and `GET /datasets/:username/all` pattern exactly.
- Removed all manual `if (!userCanAccessAll && session.user_id !== req.user.id) return 403` ownership checks from handlers — they were redundant once the middleware enforces ownership.
- Added `dataset_id` query param support to `GET /sessions/:username/all` (mirroring `GET /sessions`) so that `DatasetSessions.vue` can filter by dataset when calling the user-scoped endpoint.
- UI: `useSessionsStore.fetchSessions()` now routes to `GET /sessions/:username/all` for user role and `GET /sessions` for admin/operator.
- UI: `DatasetSessions.vue` applies the same routing logic for its dataset-scoped session fetch.

## 2026-03-03 (security audit follow-up)

- Fix: `POST /tracks` — `if (!req.permission.granted)` replaced with `if (!userCanAccessAll(req.user))`. The project membership check was being silently bypassed for user role because granting `create:any` made `req.permission.granted` always `true`. User role can now only create tracks for dataset files in projects they are a member of.
- Fix: `GET /tracks/:username/all` — added ownership guard (`if (!userCanAccessAll(req.user) && user.id !== req.user.id) → 403`). With user role having `read:any`, the `checkOwnership: true` middleware was no longer effective (it calls `readAny` when requester ≠ resourceOwner, which now passes). Without the guard, user role could call `/tracks/other_user/all` and see that user's project-scoped tracks.
- Clarification: `GET /sessions/:username/all` was correctly protected in the initial change (manual ownership guard already added). `GET /tracks/:username/all` missed the equivalent guard, now fixed.
- Clarification: Both `sessionService.getByUsername()` and `trackService.getByUsername()` exist in the UI service layer but are dead code — no UI component calls them. The UI always uses the general `GET /sessions` and `GET /tracks` endpoints, which are now correctly server-side filtered by user role. The `/:username/all` endpoints remain available for future use and are now properly secured.

## 2026-03-04 (UI enhancements — stage/download buttons, search modal improvements, genome/dataset type standardization)

### Stage/Download Columns in Dataset Tables
- Change: Added "Stage" and "Download" columns to `SessionDatasetsTable.vue`, `AssocDatasetList.vue`, and `ConversionDerivedDatasets.vue`, mirroring the existing implementation in `ProjectDatasetsTable.vue`. Includes `StageDatasetModal`, `DatasetDownloadModal`, `HalfCircleSpinner` for workflow status, and polling via `useIntervalFn`.

### Associated Tracks Table in `/sessions/:id`
- Change: Renamed 'Dataset Name' column to 'Dataset' in the tracks table on the session detail page.
- Change: Dataset link in the 'Dataset' column is hidden for 'user' role (plain text shown instead).
- Change: Added "Stage", "Download Dataset", and "Download File" action columns to the Associated Tracks table. Stage/Download use the same modal-based logic as `ProjectDatasetsTable.vue`. "Download File" directly downloads the `dataset_file` associated with the track.

### Track Detail Page (`/tracks/:id`)
- Change: Dataset link in 'Dataset Information' card is hidden for 'user' role (plain text shown instead).
- Change: Added 'Actions' card with "Stage Dataset", "Download Dataset", and "Download File" buttons.
- Change: Replaced inline dataset type `va-chip` with the `DatasetType` component.
- Change: Replaced inline genome `va-chip` with the new `GenomeDisplay` component in both the 'Track Details' card and the 'Associated Sessions' table.

### GenomeDisplay Component
- Change: Created `ui/src/components/genome/GenomeDisplay.vue` as a reusable component for displaying genome type and value consistently. Replaced all inline genome chip usages in `tracks/[id].vue`, `TrackList.vue`, and `DatasetSessions.vue`.

### Track List (`/tracks`)
- Change: Dataset link in the 'Dataset' column is hidden for 'user' role (plain text shown instead).
- Change: 'Genome' column now uses `GenomeDisplay` component.

### Track Search Modal
- Change: Replaced 'Project ID' text input with `ProjectAsyncAutoComplete` component (single-select). Selected project filters tracks via the project → dataset → dataset_file → track relation.
- Change: Added 'Dataset' search field using `DatasetSelectAutoComplete`, filtered by selected project when a project is chosen.
- Constraint: When 'Project' changes, 'Dataset' is cleared. When 'Project' is cleared, 'Dataset' is preserved (user intent: search by dataset only).
- Constraint: When both Project and Dataset are selected, `project_id` is omitted from the API call — `dataset_id` alone is sent because a dataset uniquely implies its project.
- Change: 'File Type' options are now loaded from the `analysis_type` API table (not hardcoded).
- Change: 'Genome Type' and 'Genome Value' options use `constants.GENOME_TYPES` for consistency with the rest of the app. Changing genome type clears genome value.
- Change: All form fields reset when the modal is closed or submitted.
- Constraint: 'user' role uses the `/tracks/:username/all` endpoint which enforces project membership, ensuring users only see their own datasets and projects in the autocomplete fields.

### API: `dataset_id` Filter for Tracks
- Change: Added `dataset_id` query parameter to both `GET /tracks` and `GET /tracks/:username/all`. When `dataset_id` is provided, it takes priority over `project_id`. For user role, the specified dataset must belong to a project the user is a member of (access check enforced server-side).

### DatasetSelectAutoComplete
- Change: Added `projectId` prop. When set, datasets are fetched via `projectService.getDatasets` (project-scoped) instead of `datasetService.getAll`, ensuring correct scoping.

## 2026-03-04 (bug fixes — workflow enrichment and bundle data)

### Bug: Stage column loading indicator not shown in Source/Derived Dataset tables (`/datasets/:id`)
- Fix: `GET /datasets` list endpoint (`api/src/routes/datasets/index.js`) was returning raw Prisma workflow records `[{id}]` with no `name` or `status`. `wfService.is_staging_workflow_active()` checks `wf.name`, so it always returned `false`, keeping the stage button enabled even when a workflow was running.
- Root cause: Only `GET /projects/:username/:id/datasets` had Rhythm workflow enrichment (`wfService.getAll` loop). All other list endpoints were missing it.
- Affected: `GET /datasets` and `GET /datasets/:username/all` (both handlers in `datasets/index.js`); feeds `AssocDatasetList.vue` (Source/Derived Datasets in `/datasets/:id`).
- Fix: Added `wfService.getAll` enrichment loop after the Prisma `findMany` in both handlers. Also added `INCLUDE_WORKFLOWS` to `GET /datasets/:username/all` which was missing it entirely.
- Trigger: Any dataset with an active `stage`, `stage_migrated`, or `integrated` workflow.

### Bug: "Download Archive" option missing from download modal in `/datasets/:id`, `/sessions/:id` (both tables), and `/tracks/:id`
- Fix: `DatasetDownloadModal.vue` condition `v-if="props.dataset.bundle"` requires the `bundle` relation to be present. Every API endpoint that returns datasets was missing `bundle: true` in its Prisma include.
- Root cause: Only `GET /projects/:username/:id/datasets` included `bundle: true`. Every other endpoint omitted it, silently sending `bundle: undefined`, which hid the option.
- Affected endpoints:
  - `GET /sessions/:id` (`sessions.js` line ~691) — session_tracks dataset include; feeds Associated Tracks table
  - `GET /sessions/:id/datasets` (`sessions.js` line ~1456) — session_tracks path; feeds `SessionDatasetsTable.vue`
  - `GET /sessions/:id/datasets` (`sessions.js` line ~1529) — legacy `findFirst` path; same endpoint
  - `GET /datasets` (`datasets/index.js`) — feeds `AssocDatasetList.vue` (Source/Derived Datasets)
- Trigger: Any dataset that has a bundle (i.e., a `.tar` archive has been generated for it).

---

## E2E Test Scenarios

> These scenarios are derived from expected feature behaviors and edge cases that are easy for a developer to omit. Implementation mistakes that were typos or mechanical errors (wrong field names, missing imports, pixel vs. percent widths) are excluded — only behaviors observable by a user or meaningful API contracts are listed.

### Stage/Download buttons in dataset tables
> Applies to: Associated Datasets in `/sessions/:id`, Source/Derived Datasets in `/datasets/:id`, Derived Datasets in `/conversions/:id`, and Datasets in `/projects/:id`.

- [ ] Stage button is enabled (clickable cloud_sync icon) when dataset is not staged and no staging/archival workflow is active
- [ ] Stage button shows disabled cloud_sync icon (not spinner) when dataset is already staged
- [ ] Stage button is replaced by a spinner (not a button) when a `stage`, `stage_migrated`, or `integrated` workflow is actively running on the dataset
- [ ] Stage button is replaced by a spinner (different color) when an archival workflow is actively running
- [ ] Download button is disabled when dataset is not staged
- [ ] Download button is enabled when dataset is staged
- [ ] Download modal shows all 3 options ("Direct Download", "Download Archive", "IU Storage") when the dataset has a bundle
- [ ] Download modal shows only 2 options ("Direct Download", "IU Storage") — no "Download Archive" — when the dataset has no bundle

### Associated Tracks table in `/sessions/:id`

- [ ] Column header is "Dataset", not "Dataset Name"
- [ ] Dataset column shows plain text (no link) for `user` role
- [ ] Dataset column shows a clickable router-link for `operator`/`admin` role
- [ ] Stage column follows the same enabled/disabled/spinner logic as dataset tables (listed above)
- [ ] "Download Dataset" button opens the download modal for the dataset the track belongs to
- [ ] "Download Dataset" button follows the same enabled/disabled logic as the download button in dataset tables (disabled when not staged)
- [ ] "Download File" button initiates a direct browser download of the individual `dataset_file` associated with the track
- [ ] No columns besides Stage, Download Dataset, and Download File action columns are present as action columns

### Track detail page (`/tracks/:id`)

- [ ] Dataset name in "Dataset Information" card is a clickable link for `operator`/`admin` role
- [ ] Dataset name in "Dataset Information" card is plain text (no link) for `user` role
- [ ] "Dataset Information" card does NOT show a "Staged" field
- [ ] Dataset Type field shows a colored chip via the `DatasetType` component, not plain text
- [ ] Genome field in "Track Details" card uses the `GenomeDisplay` component (consistent chip display)
- [ ] "Associated Sessions" table genome column uses the `GenomeDisplay` component
- [ ] An "Actions" card is present with exactly 3 buttons: "Stage Dataset", "Download Dataset", "Download File"
- [ ] All 3 action buttons are disabled when a staging workflow is actively running
- [ ] All 3 action buttons are disabled when an archival workflow is actively running
- [ ] When staging is pending: a spinner and a `va-alert` with text "Actions are disabled while Dataset is being staged" appear below the buttons
- [ ] "Stage Dataset" button text remains visible and readable when the button is disabled due to pending staging (text is NOT replaced by a spinner inside the button)
- [ ] "Download Dataset" button opens the download modal (same 2-or-3 option logic as other tables)
- [ ] "Download File" button initiates a direct browser download of the track's `dataset_file`

### Track list (`/tracks` view)

- [ ] Dataset column shows plain text (no link) for `user` role
- [ ] Dataset column shows a clickable link for `operator`/`admin` role
- [ ] Genome column uses the `GenomeDisplay` component (consistent chip, not inline plain text)

### Track Search modal

- [ ] "File Type" dropdown values are loaded from the `analysis_type` table (not hardcoded); values match what other parts of the app use
- [ ] "Genome Type" options are sourced from `constants.GENOME_TYPES`, consistent with the rest of the app
- [ ] "Genome Value" options are sourced from the selected Genome Type's values in `constants.GENOME_TYPES`
- [ ] Changing Genome Type clears the Genome Value field
- [ ] "Project" field uses an async autocomplete (not a plain text ID input); shows project names
- [ ] "Dataset" field uses an async autocomplete filtered to the selected project's datasets when a project is selected
- [ ] Selecting a different project clears the Dataset field
- [ ] Clearing the Project field (setting it to null/empty) preserves the current Dataset field value
- [ ] Changing the Dataset field does NOT clear or change the Project field
- [ ] When both Project and Dataset are selected and the search is submitted, only `dataset_id` is sent — `project_id` is omitted from the API request
- [ ] When only Project is selected (no Dataset), `project_id` is sent in the API request
- [ ] Closing the modal without submitting resets all fields to their defaults
- [ ] Submitting the modal resets all fields to their defaults
- [ ] `user` role only sees projects they are a member of in the Project autocomplete
- [ ] `user` role only sees datasets belonging to their own projects in the Dataset autocomplete
- [ ] Dataset autocomplete is scoped to the selected project when a project is chosen (not all user datasets)

### Associated Sessions table in `/datasets/:id`

- [ ] Genome column uses the `GenomeDisplay` component (consistent with the rest of the app)

### Dataset Type display

- [ ] Every table that has a "Type" column for datasets uses the `DatasetType` component (colored chip), not plain text — this applies to Associated Datasets in `/sessions/:id`, Source/Derived Datasets in `/datasets/:id`, Derived Datasets in `/conversions/:id`

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
- Fix: [Short description]. Root cause: [what caused it]. Affected: [files/endpoints]. Trigger: [what condition exposed it].
```

---

**Last Updated:** 2026-03-04

