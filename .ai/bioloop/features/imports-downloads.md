# Imports & Downloads Feature

**Feature Scope:** Dataset import (from external sources) and secure download mechanisms.

**Status:** Core Platform Feature

**Note:** Browser-based file uploads are covered in `uploads.md`, not here.

---

## Imports (External Dataset Ingestion)

### Overview
The Import feature ingests datasets from external sources (like SDA/tape archives) into the system. This is different from browser uploads.

### Import Flow
1. Operator schedules import via API
2. Worker downloads dataset from source (SDA, network path)
3. Dataset registered in system
4. `integrated` workflow processes the dataset
5. Import status tracked in `dataset_import_log`

### Database
- `dataset_import_log` - Tracks import operations
- `dataset_audit` - Audit trail for imported datasets

### Import Sources
- SDA (Scholarly Data Archive) via HSI commands
- Network file systems
- External URLs

---

## Downloads

### Overview
Secure download mechanism that enforces access control and provides audit logging.

### Download Types

#### Direct Download
- Small files served directly from API
- Appropriate for files < 100MB
- Uses standard HTTP file serving

#### Secure Download Service
- Large files served via dedicated download service
- Supports resume (Range requests)
- Token-based authentication
- Rate limiting and quota enforcement

### Secure Download Flow
1. User requests download via API
2. API validates user access to dataset/file
3. API generates time-limited download token (via Signet)
4. User redirected to secure download service with token
5. Download service validates token
6. File streaming begins

### API Endpoints
- `GET /datasets/:id/files/:file_id/download` - Request download
- `GET /download/:token` - Execute download (download service)

---

## Configuration

### Download Limits
```json
{
  "download": {
    "rate_limit": "100MB/s",
    "concurrent_downloads": 3,
    "token_expiry": "1h"
  }
}
```

---

**Last Updated:** 2026-02-05
