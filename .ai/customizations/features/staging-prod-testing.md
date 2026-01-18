# Staging in Production Testing

**Feature:** Dataset staging and file download workflow testing in production environment  
**Status:** In Progress  
**Scope:** Issues encountered and resolved while testing staging functionality in production

---

## Overview

This document tracks challenges, debugging efforts, and resolutions specific to testing the staging functionality in the production environment. Staging involves copying files from network filesystem mounts (`/N/...`) to local staging areas for improved performance and genome browser access.

---

## Key Components

### Worker Tasks
- `workers/workers/tasks/stage.py` - Staging task implementation
- `workers/workers/tasks/download.py` - Download setup task
- `workers/workers/utils/file_utils.py` - File operations and path utilities

### API Routes
- `api/src/routes/datasets.js` - Dataset and file management
- `api/src/services/dataset.js` - Dataset service layer

### Configuration
- `workers/config/common.py` - Worker configuration including staging paths
- `api/config/staging.json` - Staging configuration

---

## Production Environment Constraints

### Network Filesystem Access
- **Path:** `/N/...` contains research data storage
- **Permission Issues:** Must verify read permissions before attempting to stage
- **Never modify:** Read-only access to network storage (see `.ai/PRODUCTION_ENVIRONMENT.md`)

### Staging Directory
- **Local Path:** Typically `/tmp/staging/` or configured staging directory
- **Cleanup:** Must clean up temporary files after operations
- **Permissions:** Ensure worker has write access to staging directory

---

## Common Issues & Resolutions

### 1. Permission Errors on Network Mounts
**Problem:** Worker cannot read files from `/N/...` paths

**Resolution:**
- Verify file permissions using `stat` command
- Check worker process user has appropriate group membership
- Validate path exists and is accessible before staging

### 2. Dataset Path Reference Errors
**Problem:** Incorrect path construction for staged files

**Resolution:**
- Use `dataset.metadata.stage_alias` for stage path prefix
- Properly construct relative paths in file exposure URLs
- See `api/src/routes/sessions.js` for correct path construction pattern

### 3. Download Setup Task Failures
**Problem:** Download preparation fails silently or with vague errors

**Resolution:**
- Add explicit logging in download task
- Verify file existence before creating download records
- Check disk space in staging directory

---

## Testing Checklist

When testing staging in production:

- [ ] Verify network filesystem mount paths are accessible
- [ ] Check worker logs for permission errors
- [ ] Confirm staging directory has sufficient disk space
- [ ] Test with various file types (BAM, BigWig, VCF, etc.)
- [ ] Verify staged files are accessible via file exposure routes
- [ ] Confirm cleanup of temporary files after staging
- [ ] Test download generation for staged files

---

## Related Documentation

- `.ai/PRODUCTION_ENVIRONMENT.md` - Production restrictions and guidelines
- `.ai/customizations/features/sessions-tracks.md` - Genome browser session management
- `workers/README.md` - Worker architecture and task patterns

---

## Changelog

### 2026-01-17
- Initial documentation created
- Documented common issues and production constraints
- Added testing checklist

---

**Last Updated:** 2026-01-17

