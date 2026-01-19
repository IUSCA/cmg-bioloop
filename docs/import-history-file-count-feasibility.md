# File Count Column Feasibility Analysis

**Date:** 2026-01-18  
**Feature:** Display file count in Import History table after Integrated workflow completion

---

## Summary

✅ **FEASIBLE** - File count can be displayed in the Import History table using the existing polling mechanism.

---

## Current Implementation Analysis

### 1. Where File Count is Stored

**Location:** `dataset.metadata.num_genome_files` (JSON field)

**Evidence:**
- [`ui/src/components/project/datasets/ProjectDatasetsTable.vue`](../ui/src/components/project/datasets/ProjectDatasetsTable.vue) (line 132):
  ```vue
  <template #cell(metadata)="{ rowData }">
    <maybe :data="rowData?.metadata?.num_genome_files" />
  </template>
  ```

**Schema:** [`api/prisma/schema.prisma`](../api/prisma/schema.prisma) (line 28):
```prisma
model dataset {
  metadata Json?
  // ... other fields
}
```

### 2. When File Count is Populated

**Workflow Step:** `inspect` (2nd step of Integrated workflow)

**Integrated Workflow Steps:**
1. `await_stability` - Wait for dataset to stabilize
2. **`inspect`** - Scan files and extract metadata ← File count populated here
3. `archive` - Move to tape storage
4. `stage` - Extract from tape to fast storage
5. `validate` - Verify staged files match archive
6. `setup_download` - Configure secure download access

**Note:** File count is available BEFORE workflow completion, not after. It's populated during the `inspect` step.

### 3. Current Polling Implementation

**Pattern Used:** [`ui/src/components/project/datasets/ProjectDatasetsTable.vue`](../ui/src/components/project/datasets/ProjectDatasetsTable.vue) (lines 343-389)

```javascript
// Track datasets with pending workflows
const tracking = computed(() => {
  return rows.value
    .filter((ds) => ds.is_staging_pending)
    .map((ds) => ds.id);
});

// Fetch and update individual dataset
function fetch_and_update_dataset(id) {
  DatasetService.getById({ id, include_projects: true, bundle: true })
    .then((res) => {
      _datasets.value[id] = res.data; // Includes metadata
    });
}

// Poll every 10 seconds
const poll = useIntervalFn(
  () => { tracking.value.forEach(fetch_and_update_dataset); },
  config.dataset_polling_interval, // 10000ms
  { immediate: false }
);
```

**Key Points:**
- Polls datasets with pending workflows
- Fetches full dataset object (includes `metadata`)
- Updates local state reactively
- Automatically starts/stops based on tracking array

---

## Implementation Approach

### Option 1: Show After Workflow Completion (User's Request)

**Condition:** Only show file count when `integrated` workflow status is `SUCCESS`

**Pros:**
- Matches user's explicit request
- Clear indicator that processing is complete
- No confusion about partial data

**Cons:**
- File count is actually available much earlier (after `inspect` step)
- Users must wait for full workflow to see file count

**Implementation:**
```vue
<template #cell(file_count)="{ rowData }">
  <span v-if="rowData.integration_completed && rowData.imported_dataset.metadata?.num_genome_files">
    {{ rowData.imported_dataset.metadata.num_genome_files }}
  </span>
</template>
```

### Option 2: Show After Inspect Step (Alternative)

**Condition:** Show file count as soon as it's available (after `inspect` step)

**Pros:**
- Shows data as soon as it's available
- More responsive UX
- Matches how ProjectDatasetsTable works

**Cons:**
- Doesn't match user's explicit request
- May confuse users if workflow later fails

**Implementation:**
```vue
<template #cell(file_count)="{ rowData }">
  <span v-if="rowData.imported_dataset.metadata?.num_genome_files">
    {{ rowData.imported_dataset.metadata.num_genome_files }}
  </span>
</template>
```

---

## Polling Strategy

### Single API Call for Both Status and File Count

✅ **YES - Already Implemented**

The existing polling mechanism in [`ui/src/pages/datasets/imports/index.vue`](../ui/src/pages/datasets/imports/index.vue) fetches the full dataset object:

```javascript
function fetch_and_update_dataset(id) {
  datasetService
    .getById({ id, include_projects: false, bundle: true })
    .then((res) => {
      _datasets.value[id] = res.data; // Includes workflows AND metadata
      // Update import row with latest data
      const importIndex = pastImports.value.findIndex(
        (imp) => imp.imported_dataset.id === id
      );
      if (importIndex !== -1) {
        pastImports.value[importIndex].imported_dataset = res.data;
        // Update workflow status
        pastImports.value[importIndex].is_integration_pending = ...;
        pastImports.value[importIndex].integration_completed = ...;
        // File count is now available in res.data.metadata.num_genome_files
      }
    });
}
```

**Key Insight:** The `bundle: true` parameter ensures all related data (workflows, metadata, etc.) is included in a single API call.

---

## Implementation Steps (When Ready)

### 1. Add Column Definition

```javascript
const columns = [
  // ... existing columns
  {
    key: 'file_count',
    label: 'Files',
    width: '8%',
    thAlign: 'center',
    tdAlign: 'center',
  },
];
```

### 2. Add Template Cell

```vue
<template #cell(file_count)="{ rowData }">
  <span v-if="rowData.integration_completed && rowData.imported_dataset.metadata?.num_genome_files">
    {{ rowData.imported_dataset.metadata.num_genome_files }}
  </span>
</template>
```

### 3. No Additional API Changes Needed

The existing polling already fetches `metadata`, so no backend changes required.

---

## Rhythm API/Worker Investigation (Optional)

**User mentioned:** "if needed, rhythm API code is in /Users/ripandey/dev/rhythm_api and rhythm code in /Users/ripandey/dev/rhythm"

**Analysis:** Not needed for this feature because:
1. File count is stored in Bioloop's PostgreSQL database (`dataset.metadata`)
2. Bioloop API already exposes this data via `GET /datasets/:id`
3. Polling mechanism already fetches this data
4. No direct Rhythm API calls needed from UI

**When Rhythm is Relevant:**
- Workflow execution (handled by workers)
- Task status updates (handled by workers polling Rhythm MongoDB)
- Workflow step progress (handled by workers)

**For UI Display:** Bioloop API is sufficient.

---

## Recommendation

**Implement Option 1** (show after workflow completion) as it matches the user's explicit request:
- Only show file count when `integration_completed` is true
- Use existing polling mechanism (already implemented)
- No additional API calls needed
- Simple template addition

**Estimated Effort:** ~15 minutes (just add column and template)

---

## Related Files

- **Polling Implementation:** [`ui/src/pages/datasets/imports/index.vue`](../ui/src/pages/datasets/imports/index.vue) (lines 260-310)
- **Reference Pattern:** [`ui/src/components/project/datasets/ProjectDatasetsTable.vue`](../ui/src/components/project/datasets/ProjectDatasetsTable.vue) (lines 343-389)
- **Workflow Service:** [`ui/src/services/workflow.js`](../ui/src/services/workflow.js)
- **Dataset Service:** [`ui/src/services/dataset.js`](../ui/src/services/dataset.js)
- **Config:** [`ui/src/config.js`](../ui/src/config.js) (line 20: `dataset_polling_interval: 10000`)

---

**Last Updated:** 2026-01-18

