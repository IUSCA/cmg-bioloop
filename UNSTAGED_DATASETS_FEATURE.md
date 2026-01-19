# Unstaged Datasets Feature for Genome Browser Sessions

**Date:** 2026-01-12  
**Feature:** Check and stage legacy CMG datasets before viewing sessions in genome browser

---

## Overview

When a user visits a legacy CMG session that has been migrated into Bioloop and clicks "View in Genome Browser", the system now checks if any of the datasets whose files/tracks were used to create that session are currently unstaged. If unstaged datasets are found, a modal is displayed listing them with links to each dataset, and the user can choose to stage all of them before proceeding to view the session in the genome browser.

---

## Implementation Details

### 1. API Changes (`api/src/routes/sessions.js`)

#### New Endpoint: `GET /sessions/:id/datasets`
- **Purpose:** Get datasets for a session with optional staging filter
- **Authorization:** `isPermittedTo('read')`
- **Query Parameters:**
  - `staged` (boolean, optional) - Filter by staging status
    - `true` - Only return staged datasets
    - `false` - Only return unstaged datasets
    - omitted - Return all datasets
- **Response:**
  ```json
  {
    "count": 2,
    "datasets": [
      {
        "id": 123,
        "name": "Dataset Name",
        "type": "data_product",
        "is_staged": false,
        "genomic_details": {
          "genome_type": "human",
          "genome_value": "hg38"
        },
        "user": {
          "id": 1,
          "username": "user",
          "first_name": "First",
          "last_name": "Last"
        },
        "created_at": "2026-01-10T12:00:00Z"
      }
    ]
  }
  ```
- **Example Usage:**
  - `GET /sessions/123/datasets` - Get all datasets
  - `GET /sessions/123/datasets?staged=false` - Get only unstaged datasets
  - `GET /sessions/123/datasets?staged=true` - Get only staged datasets

#### New Endpoint: `POST /sessions/:id/stage-datasets`
- **Purpose:** Trigger staging workflows for all unstaged datasets in a session
- **Authorization:** `isPermittedTo('read')`
- **Response:**
  ```json
  {
    "success": true,
    "message": "Started staging for 2 dataset(s)",
    "results": [
      {
        "dataset_id": 123,
        "dataset_name": "Dataset Name",
        "success": true,
        "workflow_id": "wf_abc123"
      }
    ]
  }
  ```

#### Code Quality Improvements:
- Moved all `require()` imports to the top of the file (following new convention)
- `datasetService` is now imported at the top instead of inside route handler

---

### 2. UI Service Changes (`ui/src/services/session.js`)

#### New Methods:
- `getDatasets(id, params)` - Fetches datasets for a session with optional filters
  - `params.staged` (boolean, optional) - Filter by staging status
- `stageDatasets(id)` - Triggers staging workflows for all unstaged datasets

#### Example Usage:
```javascript
// Get all datasets
await sessionService.getDatasets(sessionId);

// Get only unstaged datasets
await sessionService.getDatasets(sessionId, { staged: false });

// Get only staged datasets
await sessionService.getDatasets(sessionId, { staged: true });
```

---

### 3. New Component: `UnstagedDatasetsModal.vue`

**Location:** `ui/src/components/sessions/UnstagedDatasetsModal.vue`

**Features:**
- Displays a table of unstaged datasets with:
  - Dataset name (hyperlinked to `/datasets/:id` in new tab)
  - Dataset type
  - Genome information (type and value)
  - Creation date
- "Stage All Datasets" button to trigger staging workflows
- Shows staging results after initiating workflows
- Loading states during data fetch and staging operations

**Props:**
- `sessionId` (Number, required) - The session ID to check for unstaged datasets

**Events:**
- `close` - Emitted when modal is closed
- `staging-complete` - Emitted when staging workflows have been successfully initiated

---

### 4. Session Detail Page Updates (`ui/src/pages/sessions/[id].vue`)

#### New State:
- `showUnstagedModal` - Controls visibility of the unstaged datasets modal

#### New Methods:

**`checkUnstagedDatasets()`**
- Called before opening genome browser
- Fetches unstaged datasets for the session
- If unstaged datasets exist, shows the modal and returns `true`
- If all datasets are staged, returns `false`

**`handleStagingComplete()`**
- Called when user completes staging from the modal
- Shows informational toast about staging progress
- Closes the modal

#### Modified Method:

**`handleBrowserSelection(browserType)`**
- Now checks for unstaged datasets before initializing the browser
- If unstaged datasets are found, shows the modal instead of proceeding
- Only initializes the browser if all datasets are staged

---

## User Flow

1. User navigates to `/sessions/:id` to view a session
2. User clicks "View in Genome Browser" button
3. User selects browser type (IGV or WashU) from the browser selection modal
4. **NEW:** System checks if any datasets used in the session are unstaged
5. **If unstaged datasets exist:**
   - Unstaged Datasets Modal is displayed
   - Modal shows a table with dataset details and hyperlinks to each dataset
   - User can click "Stage All Datasets" to initiate staging workflows
   - System starts staging workflows for each unstaged dataset
   - Modal shows results (success/failure for each dataset)
   - User is informed that datasets will be available once staging completes
   - Modal closes automatically after 2 seconds if all workflows started successfully
6. **If all datasets are staged:**
   - Genome browser initialization proceeds normally

---

## Technical Notes

### Dataset Staging
- Staging is handled by the existing Bioloop workflow system
- Each dataset triggers a separate "Stage" workflow
- Workflows are created via `datasetService.create_workflow(dataset, 'Stage', user_id)`
- The modal doesn't wait for staging to complete - it just initiates the workflows

### Authorization
- Uses existing `isPermittedTo('read')` middleware for session access
- Staging workflows respect existing dataset access controls

### Error Handling
- If staging fails for individual datasets, the modal shows which ones failed
- Failed staging attempts are logged on the server side
- User is shown a warning toast if some datasets failed to stage

---

## Database Schema

No database schema changes were required. The feature uses existing fields:
- `dataset.is_staged` - Boolean flag indicating if dataset is staged
- `dataset.genomic_details` - Relation to genomic attributes
- `session_track` - Join table linking sessions to tracks
- `track.dataset_file` - Relation to dataset files
- `dataset_file.dataset` - Relation to parent dataset

---

## API Conventions Followed

✅ Use `prisma` from `@/db` (not creating new instances)  
✅ Use `asyncHandler` for async route handlers  
✅ Use `express-validator` for input validation  
✅ Use `isPermittedTo` middleware for authorization  
✅ Use `createError` for HTTP errors  
✅ Use `logger` for server-side logging  
✅ Consistent API response structure with `{ count, datasets }` pattern

## UI Conventions Followed

✅ Use Vuestic components (`va-modal`, `va-data-table`, `va-chip`)  
✅ Import constants from `@/constants`  
✅ Use `datetime` service for date formatting  
✅ Show toasts only for API operations (not UI interactions)  
✅ Use router-link with `target="_blank"` for external navigation  
✅ No custom styling classes added (using Vuestic defaults)

---

## Testing Recommendations

1. **Test with fully staged session:**
   - Verify browser opens normally without showing unstaged modal

2. **Test with partially unstaged session:**
   - Verify modal appears with correct dataset list
   - Verify dataset hyperlinks open in new tabs
   - Verify "Stage All Datasets" initiates workflows
   - Verify staging results are displayed correctly

3. **Test with all unstaged session:**
   - Verify all datasets are listed in modal
   - Verify staging can be initiated for all

4. **Test error cases:**
   - Verify graceful handling if staging API fails
   - Verify user is informed of partial failures

5. **Test authorization:**
   - Verify only authorized users can view sessions
   - Verify staging respects dataset access controls

---

## Future Enhancements

Potential improvements for future iterations:

1. **Real-time staging status:**
   - Poll for staging progress and update modal in real-time
   - Show progress bars for each dataset being staged

2. **Selective staging:**
   - Allow users to select which datasets to stage (instead of all)
   - Add checkboxes to the dataset table

3. **Estimated staging time:**
   - Display estimated time based on dataset size
   - Show historical staging times for similar datasets

4. **Auto-refresh after staging:**
   - Automatically reload session and open browser once all datasets are staged
   - Add option to notify user when staging completes

---

## Files Modified

### API
- `api/src/routes/sessions.js` - Added 2 new endpoints, removed 1 old endpoint

### UI
- `ui/src/components/sessions/UnstagedDatasetsModal.vue` - New component
- `ui/src/services/session.js` - Added 2 new service methods
- `ui/src/pages/sessions/[id].vue` - Integrated unstaged check logic

### Documentation
- `UNSTAGED_DATASETS_FEATURE.md` - This file

---

**Implementation Complete:** All TODOs completed, no linter errors.

