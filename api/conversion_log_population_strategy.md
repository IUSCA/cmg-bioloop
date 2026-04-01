Implementation Specification: Historic CMG Conversion Logs Population
(Code-Free Documentation)
1. Executive Summary
This feature implements population of the log and worker_process tables with historic CMG conversion logs. Each conversion gets its own worker_process record and its own copy of log entries, intentionally duplicating logs when multiple conversions ran against the same sequencing run dataset.
2. Architecture & Core Design Decisions
2.1 Log Duplication Strategy
CMG Original Behavior:
Multiple conversions run against the same sequencing run (raw dataset) shared one log file named convert_[dataset_name].log
All conversions appended to the same file
Bioloop Implementation:
Each conversion gets its own worker_process record
Each worker_process gets its own complete copy of the log entries from the shared file
Log duplication is intentional and acceptable
Result: 5 conversions on same dataset = 5 worker_process records + 5 identical sets of duplicated log entries
Database Relationships:
conversion.workflow_id → worker_process.workflow_id (1:1)
worker_process.id → log.worker_process_id (1:N)
Multiple conversions share the same source log file, but each gets duplicate entries in the database
2.2 Rollback Philosophy
Question: Why provide rollback instructions?
Answer: Rollback is optional and primarily useful for:
Development/testing iterations when refining parsing logic
Cases where migration produces incorrect log levels or timestamps
Staging environment testing before production deployment
Not Required Because:
The migration script is idempotent (checks for existing workflow_id before processing)
Re-running the script on failure is safe and sufficient
Script skips already-processed conversions automatically
Recommendation: Document rollback procedures but emphasize that simply re-running the idempotent script handles most failure scenarios.
3. Environment Configuration
3.1 Required Environment Variable
Variable Name: CMG_LEGACY_CONVERSIONS_LOGS_DIR
Purpose: Single directory path where historic CMG conversion log files are stored
Example Value: /N/project/CMG-SCA/runlogs
Location: Add to data_sync/.env
No Fallback Paths: Unlike the original specification, this uses only one configured directory path. No fallback search locations.
3.2 Configuration Loading
The data sync configuration module needs to expose this value as config.cmg.legacyConversionsLogsDir with a default fallback of /N/project/CMG-SCA/runlogs if the environment variable is not set.
4. File Structure & Integration Points
4.1 New File to Create
File: data_sync/src/sync/bigbang/sync_conversion_logs.js
Purpose: Main log synchronization logic
Exports: syncConversionLogs(prisma, cmgDb) function
4.2 Files to Modify
File 1: data_sync/src/bigbang_cmg_sync.js
Add import for syncConversionLogs function
Add new step in sync sequence (after conversion sync, before remaining steps)
Add skipConversionLogs option to function parameters (default: false)
Add CLI argument parsing for --skip-conversion-logs flag
Call sync function conditionally based on flag
File 2: data_sync/src/migrate.js (or equivalent migrate script)
Add --skip-conversion-logs CLI argument using yargs
Pass skipConversionLogs option to bigbang sync function
Default value: false (enabled by default)
File 3: data_sync/src/config.js (or configuration module)
Add cmg.legacyConversionsLogsDir configuration key
Load from CMG_LEGACY_CONVERSIONS_LOGS_DIR environment variable
4.3 Modified Sync Sequence
The bigbang sync process currently has approximately 12 steps. After adding conversion logs sync:
Step 1-9: (existing steps - users, datasets, projects, etc.)
Step 10: Sync conversions (existing)
Step 11: Sync conversion logs (NEW)
Step 12-13: (remaining existing steps)
5. Database Schema Reference
5.1 Relevant Models
conversion table:
Has cmg_id field (string, nullable) - tracks original CMG MongoDB ID
Has dataset_id field (integer, nullable) - references source sequencing run
Has workflow_id field (string, nullable) - links to worker_process
Initially workflow_id is null for CMG conversions
worker_process table:
id: Primary key (integer)
pid: Process ID (integer, use 0 for historic data)
task_id: Task identifier (string)
step: Workflow step name (string, use 'conversion')
workflow_id: Unique workflow identifier (string, indexed)
tags: JSON metadata field
start_time: When process started (timestamp)
hostname: Host machine name (string, use 'cmg-historic')
Has one-to-many relationship with log table
log table:
id: Primary key (integer)
timestamp: When log entry occurred (timestamp)
message: Full log line (string)
level: Log severity (string: 'DEBUG', 'INFO', 'WARNING', 'ERROR')
worker_process_id: Foreign key to worker_process (integer)
Cascading delete when worker_process is deleted
5.2 Tags JSON Structure
For each worker_process record created, populate the tags JSON field with:
source: Always 'cmg-migration'
conversion_id: Bioloop conversion ID (integer)
cmg_conversion_id: Original CMG MongoDB ID (string)
dataset_id: Source dataset ID (integer)
dataset_name: Dataset name (string)
This allows easy querying of migrated records and provides traceability.
6. Implementation Logic Flow
6.1 Main Function: syncConversionLogs
Input Parameters:
prisma: Prisma client instance
cmgDb: MongoDB connection (for potential future use, not currently needed)
High-Level Steps:
Query all CMG conversions from database
Group conversions by their source dataset_id
For each dataset group:
Locate the CMG log file
Read file contents once
Process each conversion individually (create worker_process and duplicate logs)
Track and report statistics
Statistics Tracked:
Datasets processed (count)
Conversions updated (count)
Worker processes created (count)
Log entries created (total count across all conversions)
Missing log files (array of dataset names)
Errors encountered (array of error details)
6.2 Grouping Conversions by Dataset
Query Criteria:
cmg_id IS NOT NULL (only CMG conversions)
dataset_id IS NOT NULL (only those with known source dataset)
Include related dataset object for name access
Grouping Logic:
Create JavaScript object with dataset_id as keys
Each key maps to array of conversion records
Log summary: number of unique datasets and total conversions
6.3 Processing Each Dataset
For each dataset group:
Extract dataset information:
Get dataset name from first conversion's dataset object
Log dataset name and conversion count
Check log file existence:
Construct expected file path: {CMG_LEGACY_CONVERSIONS_LOGS_DIR}/convert_{dataset_name}.log
Attempt to access file with read permissions
If missing: log warning, add to missing files list, skip this dataset
If found: log file path confirmation
Read log file once:
Read entire file as UTF-8 text
Split into lines, filter out empty lines
Log line count
Process each conversion individually:
Iterate through all conversions for this dataset
For each conversion, call single-conversion processing function
6.4 Processing Individual Conversions
For each conversion:
Skip if already processed:
Check if conversion.workflow_id is not null
If already has workflow_id, skip (idempotency)
Generate unique workflow_id:
Format: cmg-historic-conversion-{conversion.id}
Example: cmg-historic-conversion-42
Create worker_process record:
pid: 0 (dummy value)
task_id: cmg-conversion-{cmg_id}
step: 'conversion'
workflow_id: Generated unique ID
tags: JSON object with metadata (see section 5.2)
start_time: Use conversion's initiated_at timestamp, or current time if null
hostname: 'cmg-historic'
Parse log entries:
Process each line from log file
Extract timestamp (or use conversion's initiated_at as fallback)
Determine log level based on content
Create log entry object for each line
Batch insert log entries:
Insert in chunks of 1000 entries for performance
Each entry links to the worker_process via worker_process_id
Track total inserted count
Update conversion record:
Set workflow_id field to the generated workflow ID
This links conversion to worker_process
Update statistics:
Increment conversions updated
Increment worker processes created
Add log entry count to total
6.5 Log File Path Construction
Simple, single-path approach:
Concatenate config.cmg.legacyConversionsLogsDir + /convert_ + dataset_name + .log
No fallback searching
No multiple location attempts
Path examples:
/N/project/CMG-SCA/runlogs/convert_20250606_LH00300_0148_B22NWCVLT4.log
/N/project/CMG-SCA/runlogs/convert_20250928_LH00186_0161_A22LCJMLT3.log
6.6 Log Entry Parsing
Timestamp Extraction:
Search for pattern: YYYY-MM-DD HH:MM:SS (4 digits, dash, 2 digits, dash, 2 digits, space, time)
Example: 2025-06-10 14:36:29
If found, parse into JavaScript Date object (treat as UTC)
If not found in line, use conversion's initiated_at timestamp as fallback
Log Level Determination:
Convert line to lowercase for case-insensitive matching
Check for keywords:
'ERROR' if contains: error, failed, exception
'WARNING' if contains: warning, warn
'INFO' if contains: info, completed, finished
'DEBUG' otherwise (default)
Message Storage:
Store entire original line as-is, including any timestamp prefix
No modification or truncation of message content
7. CLI Arguments & Execution
7.1 Bigbang Sync Usage
Run with conversion logs (default):
Command: node src/bigbang_cmg_sync.js
Behavior: Performs full sync including conversion logs
Run without conversion logs:
Command: node src/bigbang_cmg_sync.js --skip-conversion-logs
Behavior: Skips conversion logs sync step
7.2 Migrate Script Usage
Run with conversion logs (default):
Command: node src/migrate.js
Behavior: Performs full migration including conversion logs
Run without conversion logs:
Command: node src/migrate.js --skip-conversion-logs
Behavior: Skips conversion logs sync step
7.3 Flag Implementation
Flag name: --skip-conversion-logs
Type: Boolean
Default: false (logs sync is enabled by default)
Description: Skip syncing historic CMG conversion logs
8. Validation & Testing
8.1 Post-Migration Validation Queries
Query 1: Verify All CMG Conversions Have Logs
Purpose: Ensure every CMG conversion got a worker_process and log entries
Expected result: Zero rows returned (all conversions should have logs)
Query joins: conversion → dataset → worker_process → log
Filter: CMG conversions only (cmg_id IS NOT NULL)
Group by: conversion details
Having clause: log count equals zero
Query 2: Verify Log Duplication Worked
Purpose: Show that multiple conversions on same dataset have equal numbers of log entries
Expected result: Datasets with multiple conversions should show consistent log counts per conversion
Query shows:
Dataset name
Number of conversions per dataset
Number of worker processes (should equal conversion count - not shared)
Log counts for each conversion (should be consistent within dataset)
Filter: Only datasets with more than one conversion
Query 3: Check Worker Process to Conversion Ratio
Purpose: Verify 1:1 ratio between conversions and worker processes
Expected result: Three metrics with equal counts
Shows:
Total CMG conversions
Total CMG worker processes
Total CMG log entries
All from CMG migration (filtered by tags->>'source' = 'cmg-migration')
8.2 Data Integrity Checks
Check 1: No orphaned worker processes
Every worker_process should have at least one log entry
Every worker_process should link to exactly one conversion
Check 2: No conversions missing worker_process
Every CMG conversion should have a non-null workflow_id
Every workflow_id should match exactly one worker_process
Check 3: Consistent dataset grouping
Conversions on same dataset should have same number of log entries
Tag metadata should correctly identify source dataset
9. Performance Characteristics
9.1 Expected Duplication Example
Scenario:
Dataset: 20250606_LH00300_0148_B22NWCVLT4
Original log file: 10,000 lines
Conversions against this dataset: 5
Database Result:
5 worker_process records created
50,000 log entries inserted (10,000 lines × 5 conversions)
All 5 conversions show identical logs when viewed in UI
Database size increase: approximately 10,000 lines × 5 × (average line length + overhead)
9.2 Performance Estimates
Small dataset:
Log file: 1,000 lines
Conversions: 3
Processing time: ~2 seconds
Database writes: 3 worker_process + 3,000 log entries
Medium dataset:
Log file: 5,000 lines
Conversions: 4
Processing time: ~5-8 seconds
Database writes: 4 worker_process + 20,000 log entries
Large dataset:
Log file: 10,000 lines
Conversions: 5
Processing time: ~10-15 seconds
Database writes: 5 worker_process + 50,000 log entries
Total migration estimate:
Assuming 18 historic conversions across ~8 datasets
Average log file size: 3,000 lines
Total time: 1-2 minutes
Total log entries: ~40,000-60,000 entries
9.3 Optimization Strategies
Batch Inserts:
Log entries inserted in chunks of 1000
Reduces database round trips
Maintains transaction safety
Sequential Processing:
One dataset processed at a time
Prevents memory issues with large log files
Easier error isolation and recovery
File Reading:
Each log file read only once per dataset
Content held in memory during conversion processing
Trade-off: memory usage vs. file I/O
Idempotency:
Skip already-processed conversions
Allows safe re-runs after failures
No duplicate data on retry
10. Error Handling & Edge Cases
10.1 Missing Log Files
Scenario: Expected log file doesn't exist at configured path
Handling:
Log warning message with dataset name and expected path
Add to missingLogFiles statistics array
Skip all conversions for that dataset
Continue processing remaining datasets
Report summary at end
User Action: Review missing files list, verify file locations, potentially copy files to expected location and re-run
10.2 Already Processed Conversions
Scenario: Conversion already has non-null workflow_id
Handling:
Skip silently (or with debug log)
Don't create duplicate worker_process
Don't insert duplicate logs
Continue to next conversion
Benefit: Enables safe re-runs without data duplication
10.3 Missing Timestamps in Logs
Scenario: Log line doesn't contain parseable timestamp
Handling:
Use conversion's initiated_at timestamp as fallback
Still insert the log entry
Maintains completeness of log data
Trade-off: Timestamp accuracy vs. log completeness (completeness wins)
10.4 Large Log Files
Scenario: Log file is 100MB+ with 500,000+ lines
Handling:
File read completely into memory (single read operation)
Batch inserts prevent database overload
May cause memory pressure on data sync process
Mitigation: If needed, implement streaming parser, but current approach sufficient for expected file sizes (< 20,000 lines typically)
10.5 Conversions Without Datasets
Scenario: Conversion has cmg_id but dataset_id is null
Handling:
Excluded from initial query (query filters dataset_id IS NOT NULL)
Never processed
Can't determine log file without dataset name
User Action: If these conversions need logs, manually investigate and populate
10.6 Processing Failures
Scenario: Exception thrown during dataset or conversion processing
Handling:
Catch exception at dataset level
Log error with dataset ID and error message
Add to errors statistics array
Continue processing remaining datasets
Report errors in final summary
Recovery: Re-run migration (idempotent design handles partial completion)
11. Rollback Procedures (Optional)
11.1 When Rollback is Needed
Scenarios where manual rollback is useful:
Testing different log parsing strategies in development/staging
Migration produced incorrect log levels or timestamps due to parsing bugs
Need to modify worker_process tags structure
Want completely clean slate before fixing and re-running
11.2 When Rollback is NOT Needed
Most common scenarios:
Migration failed partway through (just re-run - it's idempotent)
Missing log files (add files and re-run)
Some conversions processed, others not (re-run - skips completed ones)
11.3 Manual Rollback Process
Step 1: Delete log entries
Delete all log records linked to CMG migration worker processes
Filter: worker_process.tags->>'source' = 'cmg-migration'
Cascading delete may handle this automatically
Step 2: Delete worker_process records
Delete all worker_process records from CMG migration
Filter: tags->>'source' = 'cmg-migration'
Step 3: Reset conversion workflow_ids
Set workflow_id back to null for all CMG conversions
Filter: cmg_id IS NOT NULL
Step 4: Re-run migration
Execute bigbang sync or migrate script normally
All conversions will be processed fresh
11.4 Verification After Rollback
Run validation queries (section 8.1) to verify:
No CMG conversions have workflow_ids
No worker_process records remain with CMG migration tags
No log entries remain from CMG migration
12. Production Deployment Checklist
12.1 Pre-Deployment
[ ] Add CMG_LEGACY_CONVERSIONS_LOGS_DIR to production .env file
[ ] Verify directory path exists and is accessible from data sync process
[ ] Verify all expected log files are present in directory
[ ] Test file permissions (read access required)
[ ] Review list of CMG conversions in production database
[ ] Estimate disk space needed for duplicated log entries
[ ] Back up production database before migration
12.2 Initial Deployment (Dry Run)
[ ] Run migration with --skip-conversion-logs flag first
[ ] Verify rest of bigbang sync works correctly
[ ] Check existing conversions are properly migrated
12.3 Logs Migration Execution
[ ] Run bigbang sync without skip flag (includes conversion logs)
[ ] Monitor console output for progress and errors
[ ] Check for missing log file warnings
[ ] Review final statistics summary
[ ] Note any errors or unexpected results
12.4 Post-Migration Validation
[ ] Run validation query 1 (all conversions have logs)
[ ] Run validation query 2 (log duplication correct)
[ ] Run validation query 3 (worker process ratio 1:1)
[ ] Verify database size increased appropriately
[ ] Test UI display of logs for 2-3 historic conversions
[ ] Verify multiple conversions on same dataset show same logs in UI
[ ] Check performance of log viewing in UI (should be fast)
12.5 Issue Resolution
[ ] If missing log files found: obtain files, place in directory, re-run
[ ] If parsing errors: review error messages, fix logic, potentially rollback and re-run
[ ] If performance issues: investigate query performance, add indexes if needed
[ ] If UI doesn't display logs: check API endpoint /conversions/:id/logs functionality
13. Integration with Existing UI/API
13.1 API Endpoint (Already Exists)
Endpoint: GET /api/conversions/:id/logs
Purpose: Retrieve logs for a specific conversion
Expected Behavior After Migration:
Query joins: conversion → worker_process (via workflow_id) → logs
Returns log entries in chronological order
Supports pagination if implemented
Historic CMG conversions now return data (previously would be empty)
No API Changes Needed: Existing endpoint works automatically once data is populated
13.2 UI Component (Already Exists)
Component: ui/src/components/conversion/ConversionView.vue
Features:
Logs modal at line 110
Fetches logs via conversionApiService.getLogs()
Displays logs in table or list format
Expected Behavior After Migration:
Historic CMG conversions now display logs
Multiple conversions on same dataset show identical logs (expected)
Timestamps display correctly
Log levels show with appropriate styling (ERROR red, WARNING yellow, etc.)
No UI Changes Needed: Component works automatically with populated data
14. Monitoring & Observability
14.1 Migration Metrics to Monitor
Console Output:
Total datasets found
Total conversions found
Progress per dataset (dataset name, conversion count)
Log file paths found
Log entry counts per conversion
Final statistics summary
Database Queries for Monitoring:
Count of CMG conversions with null workflow_id (should decrease to zero)
Count of worker_process records with CMG migration tags
Total log entries from CMG migration
Disk space used by log table
14.2 Log Messages to Watch
Info Level:
"Found X datasets with CMG conversions"
"Processing logs for dataset: {name}"
"Found log file: {path}"
"Completed conversion {cmg_id}: X log entries"
Warning Level:
"Log file not found: {path}"
"Dataset {name} already processed, skipping"
Error Level:
"Failed to process logs for dataset {id}: {error}"
File access errors
Database constraint violations
15. Future Enhancements (Out of Scope)
15.1 Potential Improvements
Log Compression:
Store log content in compressed format
Decompress on retrieval
Trade-off: storage space vs. query performance
Deduplication Layer:
Create shared log storage table
Multiple worker_processes reference same log set
More complex schema, breaks current simplicity
Streaming Parser:
Process very large files without loading entirely into memory
Line-by-line processing
Only needed if log files exceed 100MB regularly
Parallel Processing:
Process multiple datasets concurrently
Requires connection pooling and careful transaction management
Current sequential approach is simpler and sufficient
15.2 Not Planned
Real-time log streaming from CMG (system is historic/legacy only)
Log analysis or statistics generation during migration
Log search indexing (can be added later if needed)
Automatic log level classification via ML (simple keyword matching sufficient)
16. Summary of Key Points
One Worker Process Per Conversion: Each conversion gets its own worker_process record, not shared across conversions on the same dataset.
Intentional Log Duplication: Log entries are duplicated in the database for each conversion. This is acceptable and matches the UI/API expectations.
Single Configuration Path: Uses only CMG_LEGACY_CONVERSIONS_LOGS_DIR, no fallback path searching.
Idempotent Design: Safe to re-run after failures. Already-processed conversions are skipped automatically.
Optional Rollback: Rollback procedures documented but typically unnecessary due to idempotent design.
No API/UI Changes Needed: Existing endpoints and components work automatically once data is populated.
Enabled by Default: Flag --skip-conversion-logs must be explicitly provided to disable.
Performance Adequate: Expected 1-2 minute runtime for ~18 conversions with typical log file sizes.
This implementation preserves CMG's original log sharing behavior while adapting it to Bioloop's database structure through intentional duplication.
