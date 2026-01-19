# Staging in Production - Testing & Issue Resolution

**Feature Scope:** Issues encountered while testing staging functionality in production environment and their resolutions.

**Status:** In Progress

**Related Documentation:**
- `.ai/PRODUCTION_ENVIRONMENT.md` (Production restrictions)
- `.ai/bioloop/worker_conventions.md` (Worker patterns)
- `workers/workers/tasks/stage.py` (Staging task implementation)
- `workers/workers/tasks/download.py` (Download setup task)

---

## 2026-01-16

### Initial Issue Documentation

**Context:** Testing staging functionality in production revealed permission errors when attempting to set up downloads for staged datasets.

**Error Encountered:**
```
workers.exceptions.RetryableException: [Errno 1] Operation not permitted: '/N/project/CMG-SCA'
```

**Location:** 
- Task: `setup_dataset_download` (in `workers/workers/tasks/declarations.py`, line 97)
- Implementation: `workers/workers/tasks/download.py`, function `setup_download()`

**Root Cause Analysis:**

The error occurs in the `setup_download()` task which attempts to prepare staged files for download by:
1. Creating symlinks from download directory to staged path
2. Granting read permissions to "others" on staged files/directories
3. Granting execute permissions on parent directory chain

**Why This Fails:**

1. **Permission Modification on Network Filesystem:**
   - The `grant_read_permissions_to_others()` function calls `p.chmod()` on files/directories
   - The `grant_access_to_parent_chain()` function walks up the directory tree calling `chmod()` on each parent
   - When `staged_path` points to `/N/project/CMG-SCA` (network mount), the worker process lacks permission to modify file permissions on the network filesystem
   - Error: `[Errno 1] Operation not permitted`

2. **Dataset Path References:**
   - Datasets migrated from CMG may have `staged_path` pointing to original `/N/...` locations
   - OR staging process failed to properly update `staged_path` to local staging directory
   - The `setup_download()` task expects `staged_path` to be in a location where the worker has permission to modify permissions

3. **Production Environment Constraints:**
   - `.cursorrules` explicitly forbids write/delete operations on `/N/...` paths
   - Network filesystem permissions are managed externally
   - Worker process runs as unprivileged user without sudo access

**Potential Causes:**
- Dataset was never properly staged (staging task skipped or failed)
- Dataset `staged_path` was populated with original archive path instead of local staging path
- Migrated datasets from CMG database have incorrect path references
- Staging workflow not executed for these datasets

**Questions to Investigate:**
1. What is the value of `dataset.staged_path` for the failing dataset?
2. Does the dataset have `FETCHED` state in the database?
3. Was the `stage_dataset` task successfully executed for this dataset?
4. Is the `archive_path` pointing to `/N/...` and incorrectly being used as `staged_path`?
5. Are there configuration differences between development and production for staging paths?

**Constraint:** Worker processes do NOT have permission to modify file permissions on `/N/...` network mounts.

**Constraint:** Staging MUST copy/extract files to local staging directory, not reference original `/N/...` locations.

---

---

## 2026-01-17

### Investigation: Dataset 277 Staging Error - ACL Permission Analysis

**Context:** Dataset 277 successfully staged to `/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data/48527b9f2034e0238c580446a6316bd8/210413_M70445_0080_000000000-DCBT2` but failed during `setup_dataset_download` task with:

```
PermissionError: [Errno 1] Operation not permitted: '/N/scratch'
```

**Root Cause:**

The `setup_download()` function in `workers/workers/tasks/download.py` calls:
```python
grant_access_to_parent_chain(staged_path, root=Path(config['paths']['root']))
```

This function walks up the directory tree from `staged_path` to `root`, calling `chmod()` on each parent directory to grant execute permissions to "others" (o+x). 

**Problem:** The production config (`workers/workers/config/production.py`) is **MISSING** the `'root'` key in the `paths` configuration. When `config['paths']['root']` is accessed, it likely falls back to the common.py default (`'/path/to/root'`) or raises a KeyError that gets caught, causing the function to walk all the way up to `/N/scratch` (and beyond).

The worker process (running as `cmguser`) does NOT have permission to modify permissions on `/N/scratch` (owned by root) or `/N/project/CMG-SCA` (owned by root with special ACLs).

**Current ACL Permissions:**

1. **`/N/scratch`** (cannot modify - owned by root):
   - owner: root
   - group: root
   - user::rwx, group::r-x, other::r-x

2. **`/N/scratch/cmguser`** (CAN modify - owned by cmguser):
   - owner: cmguser
   - group: root
   - user::rwx, other::--x
   - ACL users: hongao:r-x, dgluser:r-x

3. **`/N/scratch/cmguser/CMG-SCA`** (CAN modify - owned by cmguser):
   - owner: cmguser
   - group: cmguser
   - user::rwx, other::--x
   - ACL users: hongao:r-x, dgluser:r-x

4. **`/N/project/CMG-SCA`** (cannot modify - owned by root):
   - owner: root
   - group: condo_CMG-SCA
   - user::rwx, group::rwx, other::---
   - ACL group: condo_CMG-SCA-ro:r-x

**What ACL Changes Would Be Needed:**

The `setup_download()` task tries to grant `o+x` (execute for "others") on all parent directories from the staged path up to the configured root. For this to work:

### For `/N/scratch` paths:
- **Root path should be configured as:** `/N/scratch/cmguser`
- **Directories that need o+x:**
  - `/N/scratch/cmguser/CMG-SCA` ✓ (can modify - owned by cmguser)
  - `/N/scratch/cmguser/CMG-SCA/cmg-bioloop` ✓ (can modify - owned by cmguser)
  - `/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage` ✓ (can modify - owned by cmguser)
  - `/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data` ✓ (can modify - owned by cmguser)
  - And the dataset-specific subdirectories ✓ (can modify - owned by cmguser)

### For `/N/project` paths:
- **Root path should be configured as:** `/N/project/CMG-SCA`
- **Directories that need o+x:**
  - `/N/project/CMG-SCA/cmg-bioloop` ✓ (can modify if owned by cmguser or group member)
  - `/N/project/CMG-SCA/cmg-bioloop/stage` ✓ (can modify if owned by cmguser or group member)
  - `/N/project/CMG-SCA/cmg-bioloop/stage/raw_data` ✓ (can modify if owned by cmguser or group member)
  - And the dataset-specific subdirectories ✓ (can modify if owned by cmguser or group member)

**Answer to User's Question:**

**YES**, setting ACL permissions WITHIN `/N/scratch/cmguser` and `/N/project/CMG-SCA` would solve the issue, **BUT ONLY IF** the `'root'` path is properly configured in `production.py` to prevent the chmod loop from walking up to `/N/scratch` or `/N/project/CMG-SCA` itself.

**Required Configuration Fix:**

Add to `workers/workers/config/production.py`:
```python
'root': '/N/scratch/cmguser'
```

This will ensure `grant_access_to_parent_chain()` stops at `/N/scratch/cmguser` (which cmguser owns and can modify) instead of trying to chmod `/N/scratch` (which is owned by root).

**Alternative Solution:**

The code could be modified to:
1. Check ownership before attempting chmod
2. Stop walking up the tree when encountering a directory the user doesn't own
3. Use ACL commands (`setfacl`) instead of chmod for more granular control
4. Skip the parent chain permission modification entirely if the staged path is already accessible

**Status:** Configuration fix needed - add `'root'` key to production.py

---

## 2026-01-18

### TODO: Fix Nginx X-Accel-Redirect Path Concatenation Issue

**Context:** Download feature returns 200 from Express API but nginx returns 404 when serving the actual file via X-Accel-Redirect.

**Issue:** Missing trailing slash in nginx `alias` directive causes incorrect path concatenation.

**Current Configuration (conf.d/download.conf):**
```nginx
location /data/ {
  internal;
  alias /N/scratch/cmguser/cmg-bioloop/production/downloads;  # ← MISSING TRAILING SLASH
```

**Problem:**
When Express sends `X-Accel-Redirect: /data/210413_M70445_0080_000000000-DCBT2.RAW_DATA.tar`, nginx constructs:
```
/N/scratch/cmguser/cmg-bioloop/production/downloads210413_M70445_0080_000000000-DCBT2.RAW_DATA.tar
                                                   ↑ MISSING SLASH
```

**Fix Required:**
```nginx
location /data/ {
  internal;
  alias /N/scratch/cmguser/cmg-bioloop/production/downloads/;  # ← ADD TRAILING SLASH
```

**Environment:** Production only (nginx container configuration)

**Status:** TODO - needs nginx config file update and container restart

---

## Future Entries

Add entries here as investigation progresses and resolution is implemented.

Format:
```
## YYYY-MM-DD

- Investigation: [What was investigated and findings]
- Decision: [What was decided]
- Change: [What changed and why]
- Resolution: [How the issue was resolved]
```

---

**Last Updated:** 2026-01-18
