# BigBang and Stats Page Changes - Implementation Summary

## Overview
This document summarizes the changes made to add historic download/stage event tracking to BigBang sync and enhance the stats page with SLATE_PROJECT metrics.

---

## 1. BigBang Changes

### New Module: `sync_download_stage_logs.js`
**Location:** `data_sync/src/sync/bigbang/sync_download_stage_logs.js`

**Purpose:** Processes historic CMG "Stage - finish" events to create:
1. `data_access_log` entries with `access_type='SLATE_PROJECT'`
2. `stage_request_log` entries
3. `dataset_state` entries (FETCHED and STAGED)

**Key Features:**
- Searches for "Stage - finish" events in CMG datasets and dataproducts
- Creates data_access_log with SLATE_PROJECT access type (CMG staging = downloading to filesystem)
- Creates stage_request_log for each stage attempt
- Creates FETCHED state (timestamp - 1ms) and STAGED state (timestamp) for dataset_state tracking
- Handles duplicates gracefully
- Progress logging every 500 datasets

### Updated: `bigbang_sync.js`
**Location:** `data_sync/src/bigbang_sync.js`

**Changes:**
1. Added import for new `syncDownloadStageLogs` module
2. Updated execution order (now 16 steps instead of 15):
   - Step 9 (NEW): Convert stage/download logs
   - Steps 10-16: Previous steps 9-15 renumbered
3. Updated header documentation to reflect new step

**Execution Flow:**
```
8. Convert dataset audit logs (from events)
9. Convert stage/download logs (NEW) ← Creates data_access_log, stage_request_log, dataset_state
10. Convert dataset import logs (CMG upload history)
11. Convert dataset hierarchies
... (rest of steps)
```

---

## 2. Database Schema

### Prisma Schema: `access_type` Enum
**Location:** `api/prisma/schema.prisma` (line 646)

**Status:** ✅ Already exists - No changes needed

```prisma
enum access_type {
  BROWSER
  SLATE_SCRATCH
  SLATE_PROJECT  ← Already defined
}
```

### Tables Used:
- `data_access_log`: Records when datasets/files are accessed (downloaded/staged)
- `stage_request_log`: Records when staging is requested
- `dataset_state`: Tracks dataset state transitions (FETCHED, STAGED, etc.)

---

## 3. UI Changes

### A. Updated: `DataAccessCountByTimeChart.vue`
**Location:** `ui/src/components/statistics/DataAccessCountByTimeChart.vue`

**Changes:**
1. **Line Chart Legend:** Added "Number of Slate-Project Data Accesses"
2. **Series Data:** Added 4th line series for SLATE_PROJECT counts
3. **Data Processing:** 
   - Extracts `slateProjectData` from API response
   - Calculates `totalSlateProjectCount`
   - Includes in total count calculation
4. **Pie Chart:** Added SLATE_PROJECT section to pie chart data

**Before:**
- 3 lines: Total, Browser, Slate-Scratch
- 2 pie sections: Browser, Slate-Scratch

**After:**
- 4 lines: Total, Browser, Slate-Scratch, Slate-Project
- 3 pie sections: Browser, Slate-Scratch, Slate-Project

### B. Updated: `MetricCountByTimeChart.vue`
**Location:** `ui/src/components/statistics/MetricCountByTimeChart.vue`

**Changes:**
1. **Prop Validator:** Added `SLATE_PROJECT` to accepted measurements
2. **currentUsage Computed:** Added case for SLATE_PROJECT ("Current Slate-Project Space Usage")
3. **chartTitleCallBack:** Added case for SLATE_PROJECT ("Slate-Project Space Utilization")
4. **getDatasetLabel:** Added case for SLATE_PROJECT ("Slate-Project Usage")

### C. Updated: `config.js`
**Location:** `ui/src/config.js`

**Changes:**
```javascript
metric_measurements: {
  SDA: 'sda',
  SLATE_SCRATCH: '/N/scratch',
  SLATE_SCRATCH_FILES: '/N/scratch files',
  SLATE_PROJECT: '/N/project',  // ← NEW
},
```

### D. Updated: `stats.vue`
**Location:** `ui/src/pages/stats.vue`

**Changes:**
Added new chart section for Slate-Project Space Utilization:
```vue
<div class="flex flex-row gap-20 flex-wrap">
  <div class="flex-1">
    <MetricCountByTimeChart
      :measurement="config.metric_measurements.SLATE_PROJECT"
    ></MetricCountByTimeChart>
  </div>
</div>
```

**Chart Order (Top to Bottom):**
1. Data Access Requests Per Day (line + pie)
2. Stage Requests Per Day (line)
3. SDA Space Utilization (line)
4. Slate-Scratch Space Utilization (line)
5. **Slate-Project Space Utilization (line)** ← NEW
6. Users by Bandwidth Consumption (bar)
7. Most Frequently Staged Datasets (bar)
8. Most Frequently Accessed Files/Datasets (bar)
9. Total Users Registered (line)

---

## 4. API Changes

### Status: ✅ No Changes Needed

**Why:** The existing API routes already handle the new data correctly:

1. **`GET /statistics/data-access-count-by-date`**
   - Groups by `access_type` automatically
   - Will include SLATE_PROJECT in results once data exists

2. **`GET /metrics/space-utilization-by-timestamp`**
   - Filters by `measurement` parameter
   - Will return SLATE_PROJECT metrics when queried with `measurement='/N/project'`

---

## 5. What Data Gets Created

### For Each Historic "Stage - finish" Event:

**Example CMG Event:**
```json
{
  "stamp": "2024-01-15T10:30:00Z",
  "description": "Stage - finish"
}
```

**Creates in Bioloop:**

1. **data_access_log:**
```sql
INSERT INTO data_access_log (timestamp, access_type, dataset_id, user_id, file_id)
VALUES ('2024-01-15 10:30:00', 'SLATE_PROJECT', 123, 1, NULL);
```

2. **stage_request_log:**
```sql
INSERT INTO stage_request_log (timestamp, dataset_id, user_id)
VALUES ('2024-01-15 10:30:00', 123, 1);
```

3. **dataset_state (FETCHED):**
```sql
INSERT INTO dataset_state (state, timestamp, dataset_id)
VALUES ('FETCHED', '2024-01-15 10:29:59.999', 123);
```

4. **dataset_state (STAGED):**
```sql
INSERT INTO dataset_state (state, timestamp, dataset_id)
VALUES ('STAGED', '2024-01-15 10:30:00', 123);
```

---

## 6. Testing & Verification

### After Running BigBang:

**1. Check Data Creation:**
```sql
-- Check SLATE_PROJECT access logs
SELECT COUNT(*) FROM data_access_log WHERE access_type = 'SLATE_PROJECT';

-- Check stage request logs
SELECT COUNT(*) FROM stage_request_log;

-- Check dataset states
SELECT state, COUNT(*) FROM dataset_state GROUP BY state;
```

**2. Verify Stats Page:**
- Navigate to `/stats` page
- **Data Access Requests Per Day chart:**
  - Should show 4 lines (Total, Browser, Slate-Scratch, Slate-Project)
  - Pie chart should show 3 sections
- **Slate-Project Space Utilization chart:**
  - Should appear below Slate-Scratch chart
  - Will show "No Data Found" if no metrics exist yet

**3. Test API Endpoints:**
```bash
# Get data access counts (should include SLATE_PROJECT)
curl "http://localhost:3001/api/statistics/data-access-count-by-date?start_date=2024-01-01&end_date=2024-12-31&by_access_type=true"

# Get Slate-Project metrics (if they exist)
curl "http://localhost:3001/api/metrics/space-utilization-by-timestamp?measurement=/N/project"
```

---

## 7. Running BigBang with New Changes

### Command:
```bash
cd data_sync
node src/bigbang_sync.js --target-db=app --skip-sessions --clear-locks
```

### Expected Output:
```
[8/16] Converting dataset audit logs...
[9/16] Converting historic stage/download events to logs...
[BIGBANG] Processing stage events for RAW_DATA
[BIGBANG] Found 1500 datasets to process for stage/download logs
[BIGBANG] Processed 500/1500 datasets (33%) - Found 350 with stage events
[BIGBANG] Processed 1000/1500 datasets (67%) - Found 720 with stage events
[BIGBANG] Processed 1500 RAW_DATA datasets, found 980 with stage events
[BIGBANG] Processing stage events for DATA_PRODUCT
...
[10/16] Converting CMG upload history to import logs...
```

---

## 8. Notes & Considerations

### Historic Data:
- Only creates logs for datasets with "Stage - finish" events in CMG
- Uses CMG event timestamps (preserves historic timing)
- FETCHED state is 1ms before STAGED (satisfies timing constraint)

### Idempotency:
- Script handles duplicate insertions gracefully
- Can be run multiple times safely
- Uses try-catch to skip existing records

### Performance:
- Processes 100 datasets per batch
- Progress logging every 500 datasets
- Minimal memory footprint

### Future Metrics:
- Slate-Project Space Utilization chart will show "No Data Found" until metrics are populated
- Metrics are populated by separate metrics collection service (not part of this change)

---

## 9. Files Modified

### BigBang Sync:
1. `data_sync/src/sync/bigbang/sync_download_stage_logs.js` (NEW)
2. `data_sync/src/bigbang_sync.js` (MODIFIED)

### UI Components:
3. `ui/src/components/statistics/DataAccessCountByTimeChart.vue` (MODIFIED)
4. `ui/src/components/statistics/MetricCountByTimeChart.vue` (MODIFIED)
5. `ui/src/pages/stats.vue` (MODIFIED)
6. `ui/src/config.js` (MODIFIED)

### Documentation:
7. `BIGBANG_STATS_CHANGES.md` (NEW - this file)

---

## 10. Success Criteria

✅ BigBang creates data_access_log entries with SLATE_PROJECT access type  
✅ BigBang creates stage_request_log entries for historic stages  
✅ BigBang creates dataset_state entries (FETCHED and STAGED)  
✅ Data Access chart shows 4 lines including SLATE_PROJECT  
✅ Data Access pie chart shows 3 sections including SLATE_PROJECT  
✅ New Slate-Project Space Utilization chart appears on stats page  
✅ API endpoints return correct data for SLATE_PROJECT  
✅ No breaking changes to existing functionality  

---

**Last Updated:** 2026-02-06
