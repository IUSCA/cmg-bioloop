# Imports & Downloads Feature

**Feature Scope:** File upload (Import) and secure download mechanisms for the Bioloop platform.

**Status:** Core Platform Feature

---

## Imports (File Upload)

### Overview
The Import feature allows users to upload files to create new datasets. This is a core data ingestion mechanism.

### Upload Flow
1. User selects files via UI
2. UI validates file types and sizes
3. Files uploaded to temporary staging area
4. API creates dataset record
5. Files moved to permanent storage
6. Dataset marked as available

### Supported File Types
- Genomic data: FASTQ, BAM, VCF, BED, BigWig
- Analysis results: CSV, TSV, TXT
- Archives: ZIP, TAR, GZ

### API Endpoints
- `POST /datasets/upload` - Initiate file upload
- `POST /datasets/upload/chunk` - Upload file chunk (for large files)
- `POST /datasets/upload/complete` - Finalize upload

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
3. API generates time-limited download token
4. User redirected to secure download service with token
5. Download service validates token
6. File streaming begins

### Token Generation
```javascript
const downloadToken = jwt.sign(
  {
    user_id: req.user.id,
    file_id: fileId,
    expires_in: '1h'
  },
  DOWNLOAD_SECRET,
  { expiresIn: '1h' }
);
```

### API Endpoints
- `GET /datasets/:id/files/:file_id/download` - Request download
- `GET /download/:token` - Execute download (download service)

---

## Configuration

### Upload Limits
```json
{
  "upload": {
    "max_file_size": "10GB",
    "max_concurrent_uploads": 5,
    "chunk_size": "10MB",
    "allowed_extensions": ["fastq", "bam", "vcf", "bed", "bw"]
  }
}
```

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

## File Validation

### Import Validation
- File extension check
- MIME type validation
- Size limit enforcement
- Virus scanning (if enabled)
- Integrity check (checksum)

### Post-Upload Processing
- Compute file hashes (MD5, SHA256)
- Extract metadata
- Index for search
- Link to parent dataset

---

**Last Updated:** 2026-01-16

