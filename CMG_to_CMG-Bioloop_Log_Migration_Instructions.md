# 📋 CMG to CMG-Bioloop Log Migration Instructions

## 🎯 **MISSION**: Migrate Historic CMG Conversion Logs to CMG-Bioloop Database

### 📖 **BACKGROUND CONTEXT**

**CMG System Behavior:**
- CMG stored multiple conversions' logs in single files named `convert_[dataset_name].log`
- All conversions run against the same sequencing run (dataset) share the same log file
- A single sequencing run could have 1-50+ conversions, all writing to the same log file
- Each conversion appends "logfile header" + its pipeline output to the shared log file

**CMG-Bioloop Schema:**
- `conversion` table links to `workflow_id`
- `worker_process` table links workflows via `workflow_id` 
- `log` table contains individual log entries linked to `worker_process_id`
- **CRITICAL**: Cannot link same log entries to multiple conversions (schema limitation)
- **SOLUTION**: Must duplicate log entries for each conversion that shared a log file

---

## 🔧 **STEP-BY-STEP MIGRATION INSTRUCTIONS**

### **STEP 1: Identify Target Conversions**

```sql
-- Find all CMG conversions that need log migration
SELECT 
    c.id,
    c.cmg_id,
    c.dataset_id,
    d.name as dataset_name,
    c.workflow_id,
    COUNT(*) OVER (PARTITION BY c.dataset_id) as conversions_per_dataset
FROM conversion c
JOIN dataset d ON d.id = c.dataset_id
WHERE c.cmg_id IS NOT NULL
ORDER BY c.dataset_id, c.initiated_at;
```

### **STEP 2: Locate CMG Log Files**

For each unique dataset with CMG conversions:

**Log File Path Pattern**: `/path/to/cmg/runlogs/convert_[dataset_name].log`

**Examples**:
- Dataset: `20250606_LH00300_0148_B22NWCVLT4`
- Log file: `/N/project/CMG-SCA/runlogs/convert_20250606_LH00300_0148_B22NWCVLT4.log`

**File Location Strategy**:
```python
def find_cmg_log_file(dataset_name):
    possible_paths = [
        f"/N/project/CMG-SCA/runlogs/convert_{dataset_name}.log",
        f"/N/scratch/cmguser/runlogs/convert_{dataset_name}.log",
        f"/opt/sca/cmg/api/runlogs/convert_{dataset_name}.log"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None
```

### **STEP 3: Handle Workflow IDs**

```python
def ensure_workflow_id(conversion):
    if not conversion.workflow_id:
        # Create dummy workflow_id for historic conversions
        dummy_workflow_id = f"cmg-historic-{conversion.cmg_id}"
        
        # Update conversion record
        update_conversion_workflow_id(conversion.id, dummy_workflow_id)
        return dummy_workflow_id
    
    return conversion.workflow_id
```

### **STEP 4: Create Worker Process Records**

For each CMG conversion, create a `worker_process` record:

```python
def create_worker_process_for_conversion(conversion):
    workflow_id = ensure_workflow_id(conversion)
    
    worker_process_data = {
        'pid': 0,  # Dummy PID for historic data
        'task_id': f"cmg-conversion-{conversion.cmg_id}",
        'step': 'conversion',
        'workflow_id': workflow_id,
        'tags': {
            'source': 'cmg-migration',
            'cmg_id': conversion.cmg_id,
            'dataset_name': conversion.dataset.name
        },
        'start_time': conversion.initiated_at,
        'hostname': 'cmg-historic'
    }
    
    return insert_worker_process(worker_process_data)
```

### **STEP 5: Parse and Insert Log Entries**

```python
def migrate_log_entries(conversion, log_file_content, worker_process_id):
    log_lines = log_file_content.split('\n')
    log_entries = []
    
    for line_num, line in enumerate(log_lines):
        if line.strip():  # Skip empty lines
            # Extract timestamp from line
            timestamp = extract_timestamp_from_line(line)
            if not timestamp:
                timestamp = conversion.initiated_at
            
            # Determine log level
            level = determine_log_level(line)
            
            log_entry = {
                'timestamp': timestamp,
                'message': line,
                'level': level,
                'worker_process_id': worker_process_id
            }
            log_entries.append(log_entry)
    
    # Batch insert for performance
    batch_insert_log_entries(log_entries)
    return len(log_entries)
```

### **STEP 6: Timestamp and Log Level Extraction**

```python
def extract_timestamp_from_line(log_line):
    # CMG timestamp format: "2025-06-10 14:36:29 [process] message"
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
    match = re.search(pattern, log_line)
    
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    return None

def determine_log_level(log_line):
    line_lower = log_line.lower()
    
    if any(word in line_lower for word in ['error', 'failed', 'exception']):
        return 'ERROR'
    elif any(word in line_lower for word in ['warning', 'warn']):
        return 'WARNING'  
    elif any(word in line_lower for word in ['info', 'completed', 'finished']):
        return 'INFO'
    else:
        return 'DEBUG'
```

### **STEP 7: Main Migration Algorithm**

```python
def migrate_cmg_conversion_logs():
    print("🚀 Starting CMG to CMG-Bioloop log migration...")
    
    # 1. Group conversions by dataset (since they share log files)
    conversions_by_dataset = group_conversions_by_dataset()
    
    migration_stats = {
        'datasets_processed': 0,
        'conversions_migrated': 0,
        'log_entries_created': 0,
        'missing_log_files': []
    }
    
    for dataset_id, conversions in conversions_by_dataset.items():
        dataset_name = conversions[0].dataset.name
        print(f"\n📁 Processing dataset: {dataset_name}")
        print(f"🔄 Found {len(conversions)} conversions to migrate")
        
        # 2. Find the shared log file
        log_file_path = find_cmg_log_file(dataset_name)
        if not log_file_path:
            print(f"⚠️  Log file not found for dataset: {dataset_name}")
            migration_stats['missing_log_files'].append(dataset_name)
            continue
            
        # 3. Read log file content once
        with open(log_file_path, 'r') as f:
            log_content = f.read()
        
        print(f"📄 Log file size: {len(log_content):,} characters")
        
        # 4. For each conversion, create worker_process and duplicate log entries
        for conversion in conversions:
            print(f"   🔄 Migrating conversion {conversion.cmg_id}...")
            
            # Create worker_process record
            worker_process_id = create_worker_process_for_conversion(conversion)
            
            # Create log entries (duplicated for each conversion)
            log_count = migrate_log_entries(conversion, log_content, worker_process_id)
            
            print(f"   ✅ Created {log_count:,} log entries")
            
            migration_stats['conversions_migrated'] += 1
            migration_stats['log_entries_created'] += log_count
        
        migration_stats['datasets_processed'] += 1
    
    # 5. Print final statistics
    print(f"\n🎉 Migration completed!")
    print(f"📊 Statistics:")
    print(f"   - Datasets processed: {migration_stats['datasets_processed']}")
    print(f"   - Conversions migrated: {migration_stats['conversions_migrated']}")
    print(f"   - Log entries created: {migration_stats['log_entries_created']:,}")
    print(f"   - Missing log files: {len(migration_stats['missing_log_files'])}")
    
    if migration_stats['missing_log_files']:
        print(f"⚠️  Datasets with missing log files:")
        for dataset in migration_stats['missing_log_files']:
            print(f"     - {dataset}")
```

### **STEP 8: Validation Queries**

After migration, run these queries to verify success:

```sql
-- 1. Check all CMG conversions have logs
SELECT 
    c.id,
    c.cmg_id,
    c.dataset_id,
    COUNT(l.id) as log_count
FROM conversion c
LEFT JOIN worker_process wp ON wp.workflow_id = c.workflow_id
LEFT JOIN log l ON l.worker_process_id = wp.id
WHERE c.cmg_id IS NOT NULL
GROUP BY c.id, c.cmg_id, c.dataset_id
HAVING COUNT(l.id) = 0;  -- Should return no results

-- 2. Check log distribution per dataset
SELECT 
    d.name as dataset_name,
    COUNT(DISTINCT c.id) as conversion_count,
    COUNT(l.id) as total_log_entries,
    COUNT(l.id) / COUNT(DISTINCT c.id) as avg_logs_per_conversion
FROM dataset d
JOIN conversion c ON c.dataset_id = d.id
JOIN worker_process wp ON wp.workflow_id = c.workflow_id
JOIN log l ON l.worker_process_id = wp.id
WHERE c.cmg_id IS NOT NULL
  AND wp.tags->>'source' = 'cmg-migration'
GROUP BY d.id, d.name
ORDER BY conversion_count DESC;

-- 3. Check for missing worker_process records
SELECT c.id, c.cmg_id, c.workflow_id
FROM conversion c
LEFT JOIN worker_process wp ON wp.workflow_id = c.workflow_id
WHERE c.cmg_id IS NOT NULL
  AND wp.id IS NULL;  -- Should return no results
```

### **STEP 9: Error Handling and Logging**

```python
def safe_migrate_conversion(conversion, log_content):
    try:
        worker_process_id = create_worker_process_for_conversion(conversion)
        log_count = migrate_log_entries(conversion, log_content, worker_process_id)
        return True, log_count
    except Exception as e:
        print(f"❌ Error migrating conversion {conversion.cmg_id}: {e}")
        # Log error to migration log file
        log_migration_error(conversion.cmg_id, str(e))
        return False, 0
```

### **STEP 10: Performance Considerations**

- **Batch insert log entries** (1000-5000 at a time) for better performance
- **Process datasets sequentially** to avoid memory issues with large log files
- **Use database transactions** to ensure data consistency
- **Monitor memory usage** when processing very large log files (>100MB)

---

## 🎯 **KEY REQUIREMENTS SUMMARY**

1. **Duplicate Strategy**: Same log content must be duplicated for each conversion that shared a CMG log file
2. **Workflow ID Handling**: Create dummy workflow IDs for conversions that don't have them
3. **File Location**: Search multiple possible paths for CMG log files
4. **Timestamp Extraction**: Parse CMG timestamp format from log lines
5. **Log Level Detection**: Categorize log entries by severity
6. **Validation**: Verify all conversions have associated logs after migration
7. **Error Handling**: Log and continue processing if individual conversions fail
8. **Performance**: Use batch operations for large datasets

---

## 📊 **Expected Outcome**

This migration will ensure all historic CMG conversion logs are properly integrated into the cmg-bioloop logging system while respecting the database schema constraints. Each CMG conversion will have its own set of log entries linked through the `worker_process` table, enabling proper log viewing and debugging capabilities in the cmg-bioloop UI.
