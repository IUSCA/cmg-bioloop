# Import Feature with Genomic Details - Implementation Documentation

**Date:** 2026-01-09  
**Feature:** Dataset Import with History Tracking and Genomic Metadata Capture

---

## Overview

This document describes the implementation of the Import feature in Bioloop, which allows users to register datasets from remote filesystem locations. The feature includes:

1. **Genomic Details Capture**: File Type, Genome Type, Genome Assembly, and Notes fields
2. **Import History Tracking**: Similar to Upload History, with a searchable table interface
3. **Consistent UX Pattern**: Mirrors the existing Upload feature's UI/UX patterns

### What Import Means in Bioloop

In Bioloop's terminology:
- **Import**: User specifies a directory path on the remote filesystem; system registers the dataset from that location
- **Upload**: User uploads files through the web browser; system stores files and registers the dataset

Both methods ultimately register datasets in the system and kick off the "integrated" workflow.

---

## Database Schema Changes

### 1. New Table: `dataset_import_log`

**File:** [`api/prisma/schema.prisma`](../api/prisma/schema.prisma) (lines 117-133)

Created a new table to track import history with genomic metadata:

```prisma
model dataset_import_log {
  id            Int      @id @default(autoincrement())
  file_type     String?
  genome_type   String?
  genome_value  String?
  source_run    String?
  notes         String?
  cmg_id        String?  // For historical CMG imports
  metadata      Json?
  created_at    DateTime @default(now())
  updated_at    DateTime @default(now()) @updatedAt
  audit_log     dataset_audit @relation(fields: [audit_log_id], references: [id], onDelete: Cascade)
  audit_log_id  Int      @unique
}
```

**Design Decision:** Link to `dataset_audit` via `audit_log_id` with cascade delete to maintain referential integrity. When a dataset is deleted, its audit logs are deleted, which cascades to import logs.

### 2. Genomic Attributes Storage

**File:** [`api/prisma/schema.prisma`](../api/prisma/schema.prisma) (lines 135-142)

Genomic details (genome_type, genome_value) are stored in both:
- `dataset_genomic_attributes` table (for queryable fields)
- `dataset_import_log` or `dataset_upload_log` (for historical record of what was captured during import/upload)

**Design Decision:** Dual storage ensures we can:
1. Query datasets by genome type/assembly efficiently
2. Preserve the exact metadata captured at import/upload time

---

## API Implementation

### 1. Route Ordering Critical Fix

**File:** [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (lines 256-445)

**Problem Discovered:** Express routes are matched in order. The route `/:id` was defined before `/imports`, causing `/imports` to match as `id='imports'`.

**Solution:** Moved `/imports` and `/imports/:username` routes to come **before** the `/:id` route.

**Key Learning:** In Express, specific routes must always be defined before parameterized routes.

### 2. Import Log Endpoints

**Files:**
- [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (lines 256-351) - `/imports` route
- [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (lines 353-445) - `/imports/:username` route

**Design Pattern:** Mirrored the Upload endpoints pattern exactly:

```javascript
GET /datasets/imports                    // For operators/admins (all users' imports)
GET /datasets/imports/:username          // For specific user (with ownership check)
```

**Validation Pattern:** Used `.optional()` for query parameters to match Upload behavior:

```javascript
validate([
  query('dataset_name').optional().trim().isLength({ min: 1 }),
  query('limit').isInt({ min: 1 }).toInt().optional(),
  query('offset').isInt({ min: 0 }).toInt().optional(),
])
```

**Critical Fix:** Originally used `.default(50)` which made parameters non-optional, causing 400 errors when UI sent additional params like `forSelf` and `username`. Changed to `.optional()` and handle defaults in code using `Prisma.skip`.

### 3. Prisma Query Pattern

**File:** [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (lines 272-294, 368-390)

**Problem Discovered:** Cannot mix `select` and `include` on the same Prisma query level.

**Solution:** Use only `include` for nested relations, which returns all fields plus related data:

```javascript
// ❌ WRONG - Causes validation error
dataset: {
  select: { id: true, name: true },
  include: { source_datasets: true }
}

// ✅ CORRECT - Use include only
dataset: {
  include: { source_datasets: { include: { source_dataset: true } } }
}
```

### 4. Dataset Creation with Genomic Details

**File:** [`api/src/services/dataset.js`](../api/src/services/dataset.js) (lines 1191-1291)

The `buildDatasetCreateQuery` function was enhanced to:

1. Create `dataset_genomic_attributes` if genome_type or genome_value provided
2. Create `dataset_import_log` nested within `dataset_audit` if create_method is `IMPORT`
3. Store file_type, genome_type, genome_value, and notes in the import log

**Design Decision:** Use nested create within transaction to ensure atomicity. If dataset creation fails, no orphaned import logs are left.

---

## UI Implementation

### 1. Import History Page

**File:** [`ui/src/pages/datasets/imports/index.vue`](../ui/src/pages/datasets/imports/index.vue)

**Design Pattern:** Replicated the `/datasetUpload` page structure exactly:
- Searchable history table at the top
- "Import Dataset" button to open stepper
- Client-side search filtering
- Server-side pagination

**Key Components:**
- `va-data-table` for displaying import history
- `Pagination` component for page controls
- `useSearchKeyShortcut` for "/" keyboard shortcut
- Columns: Status, Dataset Name, Type, File Type, Genome, Source Data, User, Date, Notes

### 2. Import Stepper with Genomic Details

**File:** [`ui/src/components/dataset/import/ImportStepper.vue`](../ui/src/components/dataset/import/ImportStepper.vue)

**Added Step 3: "Genomic Details"** between General Info and Import steps.

**Fields Implemented:**
1. **File Type** - Optional dropdown (FASTQ, BAM, BigWig, VCF, BED, BigBed, Other)
2. **Genome Type** - Optional dropdown populated from Constants.GENOME_TYPES
3. **Genome Assembly** - Optional dropdown, auto-populates based on selected Genome Type
4. **Notes** - Optional textarea for additional information

### 3. Genome Assembly Auto-Population Bug Fix

**File:** [`ui/src/components/dataset/import/ImportStepper.vue`](../ui/src/components/dataset/import/ImportStepper.vue) (lines 519-530)

**Problem Discovered:** 
- `va-select` with `track-by="value"` binds the entire option object `{ text: "Human", value: "human" }`, not just the value
- Code was trying to use `selectedGenomeType.value` directly as a key: `Constants.GENOME_TYPES[selectedGenomeType.value]`
- This resulted in `[object Object]` as the key, returning empty array

**Solution:**
```javascript
const genomeTypeKey = selectedGenomeType.value.value || selectedGenomeType.value;
const genomes = Constants.GENOME_TYPES[genomeTypeKey]?.genomes || [];
```

**Watcher Added:** Clear genome value when genome type changes to force re-selection:
```javascript
watch(selectedGenomeType, () => {
  selectedGenomeValue.value = null;
});
```

### 4. Service Layer

**File:** [`ui/src/services/dataset.js`](../ui/src/services/dataset.js) (lines 221-238)

Added `getDatasetImportLogs` method mirroring `getDatasetUploadLogs`:
- Constructs path based on `forSelf` flag
- Only sends `dataset_name`, `offset`, `limit` as query params (not `forSelf` or `username`)

### 5. Constants and Navigation

**File:** [`ui/src/constants.js`](../ui/src/constants.js)

Updated sidebar navigation:
- Changed "Import" path from `/datasets/import` to `/datasets/imports`
- This matches the pattern: `/datasets/uploads` for uploads, `/datasets/imports` for imports

---

## Upload Feature Enhancement

Applied the same Genomic Details step to Upload feature for consistency.

**File:** [`ui/src/components/dataset/upload/UploadDatasetStepper.vue`](../ui/src/components/dataset/upload/UploadDatasetStepper.vue)

**Changes:**
1. Added Step 3: "Genomic Details" (same fields as Import, except no Notes field)
2. Updated step keys and validation
3. Added genomic fields to upload form data
4. Updated API endpoint to accept genomic fields

**Note:** Unlike Import, Upload doesn't have a `notes` field in `dataset_upload_log` schema, so we only capture File Type, Genome Type, and Genome Assembly.

**API File:** [`api/src/routes/datasets/uploads.js`](../api/src/routes/datasets/uploads.js) (lines 147-170)

---

## Key Design Decisions & Rationale

### 1. Why Mirror Upload Pattern Exactly?

**Decision:** Import History UI/UX matches Upload History exactly.

**Rationale:**
- Users familiar with Upload will immediately understand Import
- Reduces cognitive load
- Easier maintenance (same patterns, same components)
- Follows DRY principle for UX patterns

### 2. Why Separate Import Log Table?

**Decision:** Created `dataset_import_log` separate from `dataset_upload_log`.

**Rationale:**
- Different metadata needed (import_space vs upload chunks)
- Import has `notes` field, Upload doesn't need it
- Clearer separation of concerns
- Easier to query import-specific data

### 3. Why Optional Genomic Fields?

**Decision:** All genomic fields are optional (File Type, Genome Type, Genome Assembly, Notes).

**Rationale:**
- Not all datasets are genomic (could be proteomics, metabolomics, etc.)
- Users may not know genome details at import time
- Flexibility > strict requirements
- Can be populated later if needed

### 4. Why Link to dataset_audit?

**Decision:** Both `dataset_import_log` and `dataset_upload_log` link to `dataset_audit` via `audit_log_id`.

**Rationale:**
- Maintains single source of truth for "who created what when"
- Cascade delete ensures no orphaned logs
- Can track full lifecycle of dataset from creation through all operations
- Enables comprehensive audit trails

### 5. Why Store Genomic Details in Two Places?

**Decision:** Store genome_type/genome_value in both `dataset_genomic_attributes` AND `dataset_import_log`.

**Rationale:**
- `dataset_genomic_attributes`: Queryable, current state, can be updated
- `dataset_import_log`: Historical record, immutable, what was captured at import time
- Audit trail: Can see if genomic details changed after import

---

## Important Learnings & Gotchas

### 1. Express Route Ordering Matters

**Issue:** `/imports` was being caught by `/:id` route.

**Solution:** Always define specific routes before parameterized routes.

**Code Location:** [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (line 256)

### 2. Prisma Validation: No Mixing select and include

**Issue:** `PrismaClientValidationError: Please either use 'include' or 'select', but not both at the same time.`

**Solution:** Use only `include` when you need relations. If you need to limit fields, use `select` everywhere (including nested relations).

**Code Location:** [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (lines 297-332)

### 3. Vue Reactive Proxies and Console Logging

**Issue:** Console logging Vue reactive objects shows lazy-expanded proxies, making debugging difficult.

**Solution:** Use `JSON.parse(JSON.stringify(toRaw(value)))` to log actual values:
```javascript
import { toRaw } from 'vue';
console.log(JSON.stringify(toRaw(myObject), null, 2));
```

**Note:** This was only used for debugging and removed in final code.

### 4. va-select Binding Behavior

**Issue:** `va-select` with `track-by="value"` binds the whole option object, not just the value.

**Solution:** Extract the value property: `selectedGenomeType.value.value`

**Code Location:** [`ui/src/components/dataset/import/ImportStepper.vue`](../ui/src/components/dataset/import/ImportStepper.vue) (line 525)

### 5. Express Validator Optional vs Default

**Issue:** Using `.default(50)` before `.optional()` made the parameter non-optional, causing 400 errors when UI sent extra params.

**Solution:** Use `.optional()` and handle defaults in code with `?? Prisma.skip` or with default parameter values.

**Code Location:** [`api/src/routes/datasets/index.js`](../api/src/routes/datasets/index.js) (lines 262-264)

---

## Testing Checklist

### API
- [ ] GET `/datasets/imports` returns all import logs for operators
- [ ] GET `/datasets/imports/:username` returns user-specific logs with ownership check
- [ ] POST `/datasets` with genomic fields creates `dataset_genomic_attributes` entry
- [ ] POST `/datasets` with create_method=IMPORT creates `dataset_import_log` entry
- [ ] Pagination works correctly (offset, limit)
- [ ] Search by dataset name works (case-insensitive)
- [ ] Routes don't conflict with `/:id` route

### UI
- [ ] Import History page displays correctly at `/datasets/imports`
- [ ] "Import Dataset" button opens stepper
- [ ] Search bar filters results (keyboard shortcut "/" works)
- [ ] Pagination controls work
- [ ] Genomic Details step appears between General Info and Import
- [ ] Genome Type dropdown populates from constants
- [ ] Genome Assembly dropdown auto-populates when Genome Type selected
- [ ] Genome Assembly clears when Genome Type changes
- [ ] Can submit import without genomic fields (all optional)
- [ ] Import with genomic fields saves correctly

---

## Future Enhancements

1. **Edit Genomic Details**: Allow users to update genomic details after import
2. **Bulk Import**: Import multiple datasets at once with same genomic details
3. **Import Templates**: Save commonly-used genomic configurations as templates
4. **Validation**: Add genome-specific file validation (e.g., BAM files should have genome assembly)
5. **CMG Historical Imports**: Populate `cmg_id` field when syncing historical data from legacy CMG system

---

## Related Documentation

- [Genome Browser Sessions, Tracks, and Conversions](./genome-browser-sessions-tracks-conversions-2026-01-03.md)
- [Genome Browser IGV and WashU Implementation](./genome-browser-igv-washu-implementation-2026-01-03.md)
- [Project Conventions](./.cursorrules)

---

## Summary

The Import feature with Genomic Details has been implemented following these key principles:

1. **Consistency**: Mirrors Upload feature patterns for familiar UX
2. **Flexibility**: All genomic fields optional to accommodate diverse data types
3. **Audit Trail**: Complete tracking via `dataset_audit` and `dataset_import_log`
4. **Data Integrity**: Cascade deletes prevent orphaned records
5. **User Experience**: Auto-population, validation, and clear error messages

The implementation demonstrates how to add complex features to an existing system while maintaining consistency with established patterns and avoiding common pitfalls like route conflicts and Prisma validation errors.

---

## Recent Updates (2026-01-18)

### Removed Notes Field
- **Removed from schema:** `dataset_import_log.notes` field
- **Removed from UI:** Notes column and textarea in Import History and ImportStepper
- **Removed from API:** `import_notes` parameter handling in dataset service

### Added Workflow Status Tracking
- **Status Column:** Shows spinner for pending integration, checkmark for completed
- **Polling Mechanism:** Uses `useIntervalFn` to poll datasets with pending workflows every 10 seconds
- **Pattern:** Matches ProjectDatasetsTable implementation exactly
- **Workflow Detection:** Checks for `integrated` workflow with `VALIDATE` step pending

### UI Improvements
- **No Empty Placeholders:** Removed `-` for missing values; shows nothing instead
- **Conditional Rendering:** All template cells use `v-if` to only render when data exists
- **Workflow Status:** Visual indicators (spinner/checkmark) for integration progress

### File Count Column (Feasibility Analysis)
- **Status:** Research completed, implementation on hold per user request
- **Feasibility:** ✅ Fully feasible using existing polling mechanism
- **Data Source:** `dataset.metadata.num_genome_files`
- **Documentation:** See [`docs/import-history-file-count-feasibility.md`](./import-history-file-count-feasibility.md)


