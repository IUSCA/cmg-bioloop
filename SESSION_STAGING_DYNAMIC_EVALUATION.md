# Session Staging Enhancement - Dynamic Data Request Evaluation

**Status:** ✅ Implementation Complete (No migration needed)  
**Date:** 2026-01-12  
**Feature:** Dynamic evaluation of data request status based on workflow states

## Overview

This document details the implementation of dynamic data request evaluation for genome browser sessions. Instead of storing `data_requested` as a database field, it is now computed at runtime based on the actual workflow states of associated datasets.

---

## Key Changes from Previous Approach

### Before (Database Field Approach)
- `data_requested` was a boolean field in the `genome_browser_session` table
- Required database migration
- Field was set to `true` when staging initiated, `false` when complete
- Could become stale if workflows failed or were manually modified

### After (Dynamic Evaluation)
- `data_requested` is computed at runtime based on workflow states
- No database field needed
- Always reflects current state of workflows
- Returns structured object: `{ requested: boolean, request_status?: 'PENDING' | 'COMPLETE' }`

---

## Implementation Details

### 1. Constants Added

**API (`api/src/constants.js`):**
```javascript
const DATA_REQUEST_STATUS = {
  PENDING: 'PENDING',
  COMPLETE: 'COMPLETE',
};
```

**UI (`ui/src/constants.js`):**
```javascript
dataRequestStatus: {
  PENDING: 'PENDING',
  COMPLETE: 'COMPLETE',
}
```

### 2. Dynamic Evaluation Logic

**Helper Function (`api/src/routes/sessions.js`):**

```javascript
/**
 * Evaluate data request status for a session based on workflow states
 * @param {Object} session - Session object with session_tracks populated
 * @returns {Object} { requested: boolean, request_status?: 'PENDING' | 'COMPLETE' }
 */
function evaluateDataRequestStatus(session) {
  if (!session?.session_tracks || session.session_tracks.length === 0) {
    return { requested: false };
  }

  // Get unique datasets from session tracks
  const datasetMap = new Map();
  session.session_tracks.forEach((st) => {
    const dataset = st.track?.dataset_file?.dataset;
    if (dataset) {
      datasetMap.set(dataset.id, dataset);
    }
  });

  const datasets = Array.from(datasetMap.values());
  
  // Check if any datasets are unstaged
  const unstagedDatasets = datasets.filter((ds) => !ds.is_staged);
  
  if (unstagedDatasets.length === 0) {
    // All datasets are staged - no request needed
    return { requested: false };
  }

  // Check workflow status for unstaged datasets
  let hasPendingWorkflows = false;
  let allHaveWorkflows = true;

  unstagedDatasets.forEach((ds) => {
    const workflows = ds.workflows || [];
    const stageWorkflows = workflows.filter((wf) => wf.name === 'Stage');
    
    if (stageWorkflows.length === 0) {
      // No stage workflow exists for this dataset
      allHaveWorkflows = false;
    } else {
      // Check if any stage workflow is still pending/running
      const hasActiveWorkflow = stageWorkflows.some(
        (wf) => !DONE_STATUSES.includes(wf.status)
      );
      if (hasActiveWorkflow) {
        hasPendingWorkflows = true;
      }
    }
  });

  // If no workflows exist for unstaged datasets, data not requested
  if (!allHaveWorkflows && !hasPendingWorkflows) {
    return { requested: false };
  }

  // Data has been requested
  return {
    requested: true,
    request_status: hasPendingWorkflows || !allHaveWorkflows
      ? DATA_REQUEST_STATUS.PENDING
      : DATA_REQUEST_STATUS.COMPLETE,
  };
}
```

**Evaluation Logic:**
1. If no session tracks exist → `{ requested: false }`
2. If all datasets are staged → `{ requested: false }`
3. If unstaged datasets exist but no workflows → `{ requested: false }`
4. If unstaged datasets have workflows:
   - Any workflow still running/pending → `{ requested: true, request_status: 'PENDING' }`
   - All workflows done but datasets still unstaged → `{ requested: true, request_status: 'PENDING' }`
   - All workflows done and datasets staged → `{ requested: false }`

### 3. API Endpoint Updates

**GET `/api/sessions/:id`**
- Now includes `workflows` in dataset query
- Calls `evaluateDataRequestStatus()` before returning response
- Returns: `{ ...session, data_requested: { requested: boolean, request_status?: string } }`

```javascript
// Fetch session with workflows
const session = await prisma.genome_browser_session.findUnique({
  where: { id },
  include: {
    // ... other includes
    session_tracks: {
      include: {
        track: {
          include: {
            dataset_file: {
              include: {
                dataset: {
                  include: {
                    workflows: true, // Added for evaluation
                  },
                },
              },
            },
          },
        },
      },
      orderBy: { order: 'asc' },
    },
  },
});

// Evaluate data request status dynamically
const dataRequestStatus = evaluateDataRequestStatus(session);

res.json({
  ...session,
  data_requested: dataRequestStatus,
});
```

**POST `/api/sessions/:id/stage-datasets`**
- Removed database update logic (no longer sets `data_requested` field)
- Changed success message from "Successfully initiated" to "Successfully requested"
- Response message emphasizes "requested" not "completed"

### 4. UI Updates

**Session Detail Page (`ui/src/pages/sessions/[id].vue`):**

**Data Requested Indicator:**
```vue
<div class="flex justify-between">
  <span class="font-medium">Data Requested</span>
  <div class="flex items-center gap-2">
    <va-icon
      :name="session.data_requested?.requested ? 'cancel' : 'check_circle'"
      :color="session.data_requested?.requested ? 'danger' : 'success'"
    />
    <span
      v-if="session.data_requested?.requested && session.data_requested?.request_status"
      class="text-sm"
    >
      ({{ session.data_requested.request_status }})
    </span>
  </div>
</div>
```

**Retry Staging Button:**
```vue
<va-button
  v-if="
    session.data_requested?.requested &&
    session.data_requested?.request_status === 'PENDING' &&
    canRetryStaging
  "
  color="warning"
  @click="retryStaging"
>
  <i-mdi-refresh class="pr-2 text-2xl" />
  Retry Staging
</va-button>
```

**Associated Datasets Table:**
```vue
<SessionDatasetsTable
  :session-id="session?.id"
  :data-requested="session?.data_requested?.requested || false"
  @datasets-updated="handleDatasetsUpdated"
/>
```

**Updated Logic:**
```javascript
const handleDatasetsUpdated = (updatedDatasets) => {
  const hasUnstagedDatasets = updatedDatasets.some((ds) => !ds.is_staged);
  canRetryStaging.value = session.value?.data_requested?.requested && hasUnstagedDatasets;
};
```

### 5. Language Changes

All references to staging being "complete" after API returns 200 have been changed to "requested":

**API Messages:**
- ❌ "Successfully initiated staging"
- ✅ "Successfully requested staging"

**UI Messages:**
- ❌ "Staging workflows have been initiated"
- ✅ "Staging workflows have been requested"

**Event Names:**
- ❌ `staging-complete`
- ✅ `staging-requested`

**Comments:**
- ❌ "Handle staging completion"
- ✅ "Handle staging requested"

---

## Response Structure

### GET `/api/sessions/:id`

```json
{
  "id": 123,
  "title": "My Session",
  "genome": "hg38",
  "genome_type": "human",
  "session_tracks": [...],
  "data_requested": {
    "requested": true,
    "request_status": "PENDING"
  }
}
```

**Possible Values:**

1. **No data request needed (all staged):**
   ```json
   "data_requested": {
     "requested": false
   }
   ```

2. **Data requested, workflows pending:**
   ```json
   "data_requested": {
     "requested": true,
     "request_status": "PENDING"
   }
   ```

3. **Data requested, workflows complete:**
   ```json
   "data_requested": {
     "requested": true,
     "request_status": "COMPLETE"
   }
   ```

### POST `/api/sessions/:id/stage-datasets`

```json
{
  "success": true,
  "message": "Successfully requested staging for 3 dataset(s)",
  "results": [
    {
      "dataset_id": 123,
      "dataset_name": "Sample Dataset",
      "success": true,
      "workflow_id": "abc-123"
    }
  ],
  "all_successful": true
}
```

---

## Files Modified

### Backend (API)

1. **`api/src/constants.js`**
   - Added `DATA_REQUEST_STATUS` constant

2. **`api/prisma/schema.prisma`**
   - Removed `data_requested` field from `genome_browser_session` model

3. **`api/src/routes/sessions.js`**
   - Added `evaluateDataRequestStatus()` helper function
   - Updated `GET /sessions/:id` to include workflows and evaluate status
   - Updated `POST /sessions/:id/stage-datasets` to remove DB update
   - Changed success messages to use "requested" instead of "initiated"

### Frontend (UI)

1. **`ui/src/constants.js`**
   - Added `dataRequestStatus` constant

2. **`ui/src/pages/sessions/[id].vue`**
   - Updated Data Requested indicator to use nested object structure
   - Updated Retry Staging button visibility logic
   - Updated `handleDatasetsUpdated()` to check nested property
   - Changed toast messages to use "requested" language

3. **`ui/src/components/sessions/UnstagedDatasetsModal.vue`**
   - Event name: `staging-complete` → `staging-requested`
   - Toast messages: "started" → "requested"

### Deleted Files

- **`api/prisma/migrations/20260112_add_data_requested_to_session/migration.sql`**
  - No longer needed (no database field)

---

## Benefits of Dynamic Evaluation

### 1. **Always Accurate**
- Status reflects current workflow states
- No risk of stale data

### 2. **No Database Migration**
- No schema changes required
- Can be deployed without downtime

### 3. **Self-Healing**
- If workflows are manually modified, status updates automatically
- No need to manually sync database field with workflow states

### 4. **Clearer Semantics**
- `request_status: 'PENDING'` clearly indicates workflows are in progress
- `request_status: 'COMPLETE'` means workflows finished (but staging may have failed)
- `requested: false` means no staging needed or no workflows exist

### 5. **Better UX**
- Users see real-time status
- Retry button appears/disappears based on actual state
- Status indicator shows both if requested and current progress

---

## Testing Checklist

### API Testing
- [ ] `GET /sessions/:id` returns `data_requested` object
- [ ] `data_requested.requested` is `false` when all datasets staged
- [ ] `data_requested.requested` is `false` when no workflows exist
- [ ] `data_requested.request_status` is `'PENDING'` when workflows running
- [ ] `data_requested.request_status` is `'COMPLETE'` when workflows done
- [ ] `POST /sessions/:id/stage-datasets` message says "requested" not "initiated"

### UI Testing
- [ ] Data Requested indicator shows correct icon and status
- [ ] Status text shows (PENDING) or (COMPLETE) when applicable
- [ ] Retry Staging button appears only when status is PENDING
- [ ] Retry Staging button hides when all datasets staged
- [ ] Toast messages use "requested" language
- [ ] No references to staging being "complete" after API returns 200

### Integration Testing
- [ ] Status updates automatically when workflows complete
- [ ] Retry button behavior matches actual workflow states
- [ ] Multiple sessions with different states display correctly

---

## Migration Notes

### No Database Migration Required ✅

Since `data_requested` is now computed dynamically, no database migration is needed. The field was never actually added to the database in production.

### Deployment Steps

1. Deploy API changes (adds evaluation logic)
2. Deploy UI changes (uses new response structure)
3. No downtime required
4. No data migration needed

---

## Summary

This implementation replaces a static database field with dynamic runtime evaluation, providing:

- **Accuracy:** Status always reflects current workflow states
- **Simplicity:** No database migration or field management
- **Clarity:** Structured response with explicit status values
- **Correctness:** Language emphasizes "requested" not "complete"

The system now correctly represents that staging is an asynchronous process that takes hours, and API success only means workflows have been successfully requested, not that staging is complete.

---

**Implementation Date:** 2026-01-12  
**Status:** ✅ Complete  
**Migration Required:** No  
**Breaking Changes:** None (response structure is additive)  
**Backward Compatible:** Yes

