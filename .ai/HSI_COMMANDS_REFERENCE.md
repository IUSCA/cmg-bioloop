# HSI Commands Reference for Bioloop

**Version Compatibility:** This guide documents HSI commands used in Bioloop. All commands shown are standard HSI operations that should work with hsi.10.3.0.p3 and other modern HSI versions.

**Safety Level:** ✅ All commands documented here are **READ-ONLY or SAFE** for debugging purposes, unless explicitly marked as "Workflow-Only".

---

## What is HSI/HPSS?

- **HSI:** HPSS Storage Interface - command-line client for HPSS
- **HPSS:** High Performance Storage System - tape-backed archival storage
- **SDA:** Storage for Data and Analysis (local alias for HPSS in Bioloop code)

---

## HSI Flag Reference

### `-P` flag (Prompt Mode)
**Usage:** `hsi -P 'command'`

**What it does:** Execute a single HSI command and exit (non-interactive mode)

**Why we use it:** Allows running HSI commands from scripts without opening interactive session

**Example:**
```bash
hsi -P 'pwd'
# Returns current directory, then exits
```

---

## Workflow-Specific HSI Commands

These commands are used by Bioloop workflows. **DO NOT run these manually** unless you know what you're doing.

### 1. Archive Workflow

**Purpose:** Upload dataset tar bundles to HPSS for long-term storage

**Commands used:**
```bash
# Check if file already exists (preflight check)
hsi -P 'hashlist <archive_path>'
# Returns: <md5_checksum> <size> <path>
# Example: d41d8cd98f00b204e9800998ecf8427e  1024  /hpss/archive/2023/dataset.tar

# Create archive directory if needed
hsi -P 'mkdir -p <directory_path>'
# -p: create parent directories as needed (like mkdir -p on Linux)
# Example: hsi -P 'mkdir -p /hpss/archive/2023/raw_data'

# Upload file with checksum verification
hsi -P 'put -c on <local_file> : <hpss_path>'
# -c on: enable checksum creation and verification
# : separates local path from HPSS path
# Example: hsi -P 'put -c on /tmp/bundle.tar : /hpss/archive/2023/bundle.tar'

# Get file size during upload (for progress tracking)
hsi -P 'ls -s1 <hpss_path>'
# -s1: size in bytes, one entry per line
# Returns: <size_in_bytes>
# Example output: 1048576
```

**Workflow file:** `workers/workers/tasks/archive.py`

**When it runs:** When user archives a dataset (moves to HPSS tape storage)

---

### 2. Stage Workflow

**Purpose:** Download dataset tar bundles from HPSS to staging area for user access

**Commands used:**
```bash
# Get checksum to verify file integrity (preflight check)
hsi -P 'hashlist <hpss_path>'
# Returns: <md5_checksum> <size> <path>

# Check if file exists in HPSS
hsi -P 'ls <hpss_path>'
# Returns: file info if exists, error if not
# Example: -rw-r--r-- 1 user group 1024 Jan 01 2023 dataset.tar

# Download file with checksum verification
hsi -P 'get -c on <local_file> : <hpss_path>'
# -c on: enable checksum verification during download
# Example: hsi -P 'get -c on /staging/bundle.tar : /hpss/archive/2023/bundle.tar'

# Get file size during download (for progress tracking)
hsi -P 'ls -s1 <hpss_path>'
```

**Workflow file:** `workers/workers/tasks/stage.py`

**When it runs:** When user stages an archived dataset (brings back from tape to disk)

---

### 3. Bundle Population Script

**Purpose:** Standalone utility to populate bundle table with MD5 checksums for legacy datasets

**Commands used:**
```bash
# Test HSI connectivity
hsi -P 'pwd'
# Returns: current working directory in HPSS
# Example: /hpss/home/cmguser

# Get MD5 checksum for archived dataset
hsi -P 'hashlist <archive_path>'
# Returns: <checksum> <size> <path>
# Special case: "(none)" if file has no checksum
```

**Script file:** `data_sync/populate_bundles.js`

**When it runs:** Manually by operator to populate bundles for legacy archived datasets

---

## Safe Debugging Commands

These are **READ-ONLY** commands safe for developers to run for debugging HSI/HPSS issues.

### Navigation & Inspection

```bash
# 1. Check current directory
hsi -P 'pwd'
# Output: /hpss/home/cmguser
# Use case: Verify where you are in HPSS filesystem

# 2. List directory contents
hsi -P 'ls /hpss/archive/2023'
# Output: drwxr-xr-x  2 user group 4096 Jan 01 2023 raw_data
# Use case: See what files/directories exist

# 3. List with detailed size info
hsi -P 'ls -l /hpss/archive/2023/dataset.tar'
# -l: long format (permissions, size, date, name)
# Output: -rw-r--r-- 1 user group 1048576 Jan 01 2023 dataset.tar
# Use case: Check file size and permissions

# 4. List with size in bytes only
hsi -P 'ls -s1 /hpss/archive/2023/dataset.tar'
# -s1: size in bytes, minimal output
# Output: 1048576
# Use case: Get exact file size for calculations

# 5. Recursive directory listing
hsi -P 'ls -R /hpss/archive/2023'
# -R: recursive (list subdirectories)
# Use case: See entire directory tree
```

### Checksum & Verification

```bash
# 6. Get MD5 checksum of file
hsi -P 'hashlist /hpss/archive/2023/dataset.tar'
# Output: d41d8cd98f00b204e9800998ecf8427e  1024  /hpss/archive/2023/dataset.tar
# Use case: Verify file integrity, compare with database records

# 7. Get checksums for multiple files
hsi -P 'hashlist /hpss/archive/2023/*.tar'
# Output: one line per file with checksum
# Use case: Bulk verification of archives

# 8. Check if file has a checksum stored
hsi -P 'hashlist /path/to/file'
# Output: "(none)" if no checksum exists
# Use case: Identify files that need checksum regeneration
```

### File Status

```bash
# 9. Check if file exists (exit code method)
hsi -P 'ls /hpss/archive/2023/dataset.tar'
# Exit code 0: file exists
# Exit code non-zero: file does not exist
# Use case: Verify archive_path in database is correct

# 10. Check file metadata
hsi -P 'ls -l /hpss/archive/2023/dataset.tar'
# Shows: permissions, owner, size, modification date
# Use case: Diagnose permission or ownership issues
```

### Quota & Space

```bash
# 11. Check quota usage (if available)
hsi -P 'quota'
# Output: quota information for your account
# Use case: Diagnose "out of space" errors

# 12. Check disk usage of directory
hsi -P 'du -s /hpss/archive/2023'
# -s: summary only (total size)
# Output: total size in KB
# Use case: See how much space archives are using
```

---

## Common Debugging Scenarios

### Scenario 1: Dataset Won't Stage

**Problem:** User tries to stage dataset, fails with "file not found"

**Debug steps:**
```bash
# 1. Get archive path from database
# SELECT archive_path FROM dataset WHERE id = <dataset_id>;
# Example output: /hpss/archive/2023/raw_data/CMG001.tar

# 2. Check if file exists in HPSS
hsi -P 'ls /hpss/archive/2023/raw_data/CMG001.tar'
# If fails: archive_path in database is wrong or file was deleted

# 3. If exists, check file size
hsi -P 'ls -l /hpss/archive/2023/raw_data/CMG001.tar'
# Compare size with bundle.size in database

# 4. Verify checksum
hsi -P 'hashlist /hpss/archive/2023/raw_data/CMG001.tar'
# Compare with bundle.md5 in database
```

### Scenario 2: Bundle Population Fails

**Problem:** `populate_bundles.js` reports "no checksum retrieved"

**Debug steps:**
```bash
# 1. Check if you can connect to HSI
hsi -P 'pwd'
# If fails: HSI authentication issue (run kinit)

# 2. Try to get checksum manually
hsi -P 'hashlist /hpss/archive/2023/raw_data/CMG001.tar'
# If returns "(none)": file has no stored checksum
# If fails: file doesn't exist or permission issue

# 3. Check if file exists at all
hsi -P 'ls /hpss/archive/2023/raw_data/CMG001.tar'
# Verify the path is correct

# 4. Check parent directory
hsi -P 'ls /hpss/archive/2023/raw_data'
# See if file is there with different name
```

### Scenario 3: Archive Upload Slow or Failing

**Problem:** Archive workflow hangs or fails during upload

**Debug steps:**
```bash
# 1. Check if file already exists (might be partial upload)
hsi -P 'ls -l /hpss/archive/2023/raw_data/CMG001.tar'
# If exists, compare size with expected size

# 2. Check available space
hsi -P 'quota'
# See if you're out of quota

# 3. Check if directory exists
hsi -P 'ls /hpss/archive/2023/raw_data'
# If fails, directory might need to be created

# 4. Test with small file
hsi -P 'put -c on /tmp/test.txt : /hpss/test.txt'
# Verify basic HSI functionality works
```

### Scenario 4: Checksum Mismatch

**Problem:** Stage workflow fails with "checksum mismatch"

**Debug steps:**
```bash
# 1. Get HPSS checksum
hsi -P 'hashlist /hpss/archive/2023/raw_data/CMG001.tar'
# Example: abc123... 1048576 /hpss/archive/2023/raw_data/CMG001.tar

# 2. Compare with database
# SELECT md5 FROM bundle WHERE dataset_id = <dataset_id>;

# 3. If they don't match, file was modified or corrupted
# Either:
# - Re-archive the dataset (if original still exists)
# - Update database with correct checksum (if HPSS is correct)
```

---

## HSI Authentication

### Checking Authentication Status

```bash
# Check Kerberos ticket
klist
# Shows: ticket expiration time

# Renew ticket if needed
kinit <username>
# Enter password when prompted
```

### Common Authentication Errors

**Error:** `Error: HSI is not available or not authenticated`

**Solution:**
```bash
# 1. Check if HSI is installed
which hsi
# Output: /usr/local/bin/hsi (or similar)

# 2. Get new Kerberos ticket
kinit <username>

# 3. Verify HSI works
hsi -P 'pwd'
```

---

## Performance Notes

From the codebase documentation:

> The checksum algorithms that are used are very CPU-intensive.
> Although the checksum code is compiled with a high level of compiler optimization,
> transfer rates can be significantly reduced when checksum creation or verification is in effect.

**Typical speeds:**
- **Download (get):** ~56 MBps with checksum verification
- **Upload (put):** Varies based on CPU, network, and filesystem speed
- **Checksum query (hashlist):** 2-5 seconds per file (involves tape access)

**Bundle population performance:**
- ~2-5 seconds per dataset (HSI query overhead)
- 100 datasets: ~5-10 minutes
- 1000 datasets: ~45-90 minutes

---

## Safety Reminders

### ✅ Safe Commands (OK to run anytime)
- `pwd` - Get current directory
- `ls` - List files
- `hashlist` - Get checksums
- `quota` - Check quota
- `du` - Check disk usage

### ⚠️ Workflow-Only Commands (DO NOT run manually)
- `put` - Upload files (only in archive workflow)
- `get` - Download files (only in stage workflow)
- `mkdir` - Create directories (only in archive workflow)
- `rm` - Delete files (NOT USED in current codebase)

### 🚨 NEVER Use These
- `rm` - Delete files
- `rmdir` - Delete directories
- Any command that modifies `/N/...` paths (production data)

---

## Command Summary Table

| Command | Flags | Purpose | Safety | Used By |
|---------|-------|---------|--------|---------|
| `pwd` | none | Show current directory | ✅ Safe | populate_bundles.js |
| `ls` | none | List files | ✅ Safe | archive, stage, sda.py |
| `ls` | `-l` | List with details | ✅ Safe | debugging |
| `ls` | `-s1` | Get file size | ✅ Safe | archive, stage (progress) |
| `ls` | `-R` | Recursive list | ✅ Safe | debugging |
| `hashlist` | none | Get MD5 checksum | ✅ Safe | archive, stage, populate_bundles |
| `quota` | none | Check quota | ✅ Safe | debugging |
| `du` | `-s` | Directory size | ✅ Safe | debugging |
| `mkdir` | `-p` | Create directory | ⚠️ Workflow | archive workflow |
| `put` | none | Upload file | ⚠️ Workflow | archive workflow |
| `put` | `-c on` | Upload with checksum | ⚠️ Workflow | archive workflow (default) |
| `get` | none | Download file | ⚠️ Workflow | stage workflow |
| `get` | `-c on` | Download with checksum | ⚠️ Workflow | stage workflow (default) |

---

## Related Files

**Python HSI Interface:**
- `workers/workers/sda.py` - HSI command wrapper functions

**Workflow Tasks:**
- `workers/workers/tasks/archive.py` - Archive workflow
- `workers/workers/tasks/stage.py` - Stage workflow
- `workers/workers/workflow_utils.py` - Upload/download utilities

**Scripts:**
- `data_sync/populate_bundles.js` - Bundle population utility

**Documentation:**
- `data_sync/POPULATE_BUNDLES_USAGE.md` - Bundle population guide
- `BIOLOOP_WORKFLOW_ARCHITECTURE.md` - Workflow architecture

---

## Version Notes

**HSI Version in Production:** hsi.10.3.0.p3 (Feb 12 2025 build)

**Compatibility:** All commands documented here are standard HSI commands that have been stable across HSI versions 5.x through 10.x. The `-P` flag for prompt mode is available in all modern versions.

**Not Documented:** Commands that might exist in HSI but are not used by Bioloop are not documented here. This includes interactive mode, tape management commands, class of service settings, etc.

---

**Last Updated:** 2026-01-17


