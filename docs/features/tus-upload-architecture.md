# TUS Upload Architecture


---

## Executive Summary

We've migrated from a custom chunk-based upload system to the **TUS (The Upload Server)** protocol. This is an open standard ([tus.io](https://tus.io)) designed specifically for reliable file uploads over HTTP, providing automatic resume capability for large files without custom implementation.

---

## Architecture Comparison

### Old Architecture (Chunk-Based) - For Reference

**Note for migration:** This was the previous implementation. If migrating a Bioloop fork, this is what needs to be replaced.

```
┌─────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────┐
│   UI    │────▶│ Secure Download  │────▶│     Worker      │────▶│   API    │
│         │     │    Service       │     │  (Chunk Merge)  │     │          │
└─────────┘     └──────────────────┘     └─────────────────┘     └──────────┘
                        │
    ┌───────────────────┼───────────────────┐
    ▼                   ▼                   ▼
 Chunk 1             Chunk 2             Chunk N
 + MD5               + MD5               + MD5
    │                   │                   │
    └───────────────────┼───────────────────┘
                        ▼
               Worker merges chunks
               into final file
```

**Limitations of old system:**
- Complex chunk tracking logic in UI, API, and workers
- Manual resume required client-side state management
- Separate service (secure_download) added operational complexity
- Worker responsible for chunk assembly (CPU/IO intensive)
- MD5 checksums per-chunk (weaker, more overhead)

---

### New Architecture (TUS-Based)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              UPLOAD FLOW                                     │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────┐                    ┌─────────────────┐
    │   UI    │                    │       API       │
    │ (TUS    │                    │   (TUS Server)  │
    │ Client) │                    │                 │
    └────┬────┘                    └────────┬────────┘
         │                                  │
         │  1. POST /uploads/files          │
         │  (Create upload slot)            │
         │─────────────────────────────────▶│
         │                                  │
         │  2. Returns upload_id + offset   │
         │◀─────────────────────────────────│
         │                                  │
         │  3. PATCH /uploads/files/{id}    │
         │  (Send file bytes, resumable)    │
         │─────────────────────────────────▶│───▶ FileStore
         │            ...                   │     (streams to disk)
         │  (auto-resume from last offset)  │
         │                                  │
         │  4. POST /datasets/uploads/:id   │
         │     /complete                    │
         │─────────────────────────────────▶│───▶ Move file to
         │                                  │     final location
         └──────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           ASYNC PROCESSING                                   │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐         ┌─────────┐         ┌──────────┐
    │ Polling Job  │────────▶│   API   │────────▶│  Rhythm  │
    │ (Every 30s)  │         │         │         │ Workflow │
    └──────────────┘         └─────────┘         └──────────┘
           │
           │  1. Find uploads with status=UPLOADED
           │  2. Verify integrity (optional BLAKE3)
           │  3. Trigger integrated workflow
           │  4. Update status to COMPLETE
           │
           ▼
    ┌──────────────┐
    │  Integrated  │
    │   Workflow   │
    │ (Staging,    │
    │  Metadata)   │
    └──────────────┘
```

---

## Key Components

| Component | Role | Technology |
|-----------|------|------------|
| **TUS Client** | Browser-side upload, auto-resume | `tus-js-client` library |
| **TUS Server** | Handles upload protocol, streams to disk | `@tus/server`, `@tus/file-store` |
| **API** | TUS hosting, `/complete` endpoint, metadata | Express.js |
| **Polling Job** | Detects completed uploads, triggers workflows | Python script (PM2 cron) |
| **Checksum Service** | Optional BLAKE3 integrity verification | `hash-wasm` (UI), `blake3` (Python) |

---

## Advantages Over Previous Architecture

### 1. Protocol-Level Resume

```
Connection Lost at 60%              Resume from 60%
         │                                  │
         ▼                                  ▼
┌─────────────────┐              ┌─────────────────┐
│ TUS FileStore   │              │ TUS FileStore   │
│                 │              │                 │
│ file.bin        │   ─────▶    │ file.bin        │
│ ████████░░░░░░░ │   reconnect │ █████████████░░ │
│ offset: 60MB    │              │ offset: 60MB    │
│                 │              │ (continues)     │
└─────────────────┘              └─────────────────┘

No client-side state management required.
Server tracks offset in .json metadata file.
```

**Old chunk-based system:** Required storing chunk IDs, tracking completion state, handling retries manually.

**New TUS system:** Client asks server "where was I?" — server responds with byte offset. Zero client state.

---

### 2. Eliminated Service Complexity

```
BEFORE: 4 services involved in upload
┌────┐   ┌────────────────┐   ┌────────┐   ┌─────┐
│ UI │──▶│secure_download │──▶│ Worker │──▶│ API │
└────┘   └────────────────┘   └────────┘   └─────┘
         (chunk receiving)    (merging)

AFTER: 2 services involved in upload
┌────┐   ┌─────┐
│ UI │──▶│ API │
└────┘   └─────┘
         (TUS server)
```

- Removed secure_download from upload path entirely
- Removed worker-side chunk assembly
- Single service handles upload (API)
- Async processing decoupled (polling job)

---

### 3. Streaming vs. Buffering

```
CHUNK-BASED (Old)                    TUS (New)
                                     
UI sends chunk ──▶ Service           UI sends bytes ──▶ API
                    │                                    │
Service buffers     │                TUS streams         │
entire chunk        │                directly to         │
in memory           │                disk                │
                    │                                    │
Then writes ────────▶ Disk           ──────────────────▶ Disk
                                     
Memory usage: O(chunk_size)          Memory usage: O(buffer_size)
Typically 5-10 MB                    Typically 64 KB
```

**Result:** Lower memory footprint, better scalability for concurrent uploads.

---

### 4. Stronger Integrity Verification

```
OLD: MD5 per chunk                   NEW: BLAKE3 manifest
                                     
Chunk 1 ─▶ MD5 ─┐                    File 1 ─▶ BLAKE3 ─┐
Chunk 2 ─▶ MD5 ─┤                    File 2 ─▶ BLAKE3 ─┼─▶ Manifest ─▶ BLAKE3
Chunk 3 ─▶ MD5 ─┘                    File 3 ─▶ BLAKE3 ─┘
                                     
- Weaker algorithm (MD5)             - Modern algorithm (BLAKE3)
- Per-chunk only                     - Entire upload integrity
- No ordering guarantee              - Deterministic ordering
- Vulnerable to collision            - Cryptographically secure
```

**BLAKE3 Manifest Format:**
```
blake3-manifest-v1
path/to/file1.txt    1234    <hash>
path/to/file2.txt    5678    <hash>
```

Sorted alphabetically for determinism. Hash of manifest = upload integrity proof.

---

### 5. Simplified Error Handling

```
CHUNK-BASED: Multiple failure modes
┌────────────────────────────────────────────┐
│ • Chunk upload failed (retry that chunk)   │
│ • Chunk checksum mismatch (re-upload)      │
│ • Chunk ordering issue (re-sequence)       │
│ • Service unavailable (which service?)     │
│ • Merge failed (worker retry?)             │
│ • Partial merge (cleanup state?)           │
└────────────────────────────────────────────┘

TUS: Binary success/fail
┌────────────────────────────────────────────┐
│ • Connection lost → Auto-resume from offset│
│ • Upload complete → Call /complete         │
│ • /complete fails → Retry button           │
│ • Integrity fails → VERIFICATION_FAILED    │
└────────────────────────────────────────────┘
```

---

## Upload State Machine

```
                    ┌───────────────┐
                    │   UPLOADING   │
                    └───────┬───────┘
                            │
              TUS upload complete
                            │
                            ▼
                    ┌───────────────┐
                    │   UPLOADED    │
                    └───────┬───────┘
                            │
              Polling job spawns
              verification task
                            │
                            ▼
                    ┌───────────────┐
                    │   VERIFYING   │◀──────────────┐
                    └───────┬───────┘               │
                            │                       │
              ┌─────────────┴─────────────┐         │
              │                           │    (Task retry)
     Verification OK            Verification Failed
              │                           │         │
              ▼                           ▼         │
      ┌───────────────┐         ┌─────────────────────┐
      │   VERIFIED    │         │ VERIFICATION_FAILED │
      └───────┬───────┘         └─────────────────────┘
              │
    Workflow triggered
              │
              ▼
      ┌───────────────┐
      │   COMPLETE    │
      └───────┬───────┘
              │
    Workflow processing
              │
              ▼
      ┌─────────────────┐
      │ Workflow status │
      │ (via Rhythm)    │
      └─────────────────┘
```

**Key Transitions:**
- `UPLOADING` → `UPLOADED`: TUS upload completes, `/complete` endpoint called
- `UPLOADED` → `VERIFYING`: Polling job spawns async verification Celery task
- `VERIFYING` → `VERIFIED`: Verification task succeeds
- `VERIFYING` → `VERIFICATION_FAILED`: Verification task fails after max retries
- `VERIFIED` → `COMPLETE`: Polling job triggers integrated workflow

---

## Operational Benefits

| Aspect | Improvement |
|--------|-------------|
| **Debugging** | Single service to inspect, TUS metadata files show upload state |
| **Monitoring** | Upload progress tracked server-side, queryable |
| **Recovery** | Incomplete uploads auto-expire (7 days), no manual cleanup |
| **Scaling** | Stateless TUS server, can load-balance with shared storage |
| **Standards** | TUS 1.0 is widely adopted; clients exist for all platforms |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| TUS server crash mid-upload | FileStore persists offset; client auto-resumes |
| Browser refresh/close | Same — resume on return |
| Large file (50+ GB) | Tested up to 100 GB; chunked internally by TUS |
| Network instability | TUS handles retry at protocol level |
| Orphaned uploads | 7-day expiration; polling job cleans up |

---

## Known Failure Modes & Resolutions

This section documents production failure modes that have been identified, diagnosed, and resolved. Each entry explains root cause, symptom, and the fix applied.

---

### 1. 413 Entity Too Large on PATCH Request

**Symptom:**
- Upload fails early (often showing some progress in the UI)
- Browser network tab shows `PATCH /api/uploads/files/{id}` returning `413`
- API logs show no PATCH entry at all (request rejected before reaching the API)
- `DELETE /api/uploads/files/{id}` appears ~30 seconds later (TUS abort cleanup)

**Root cause:**
`tus-js-client` defaults to uploading the entire file in a single PATCH request body when no `chunkSize` is configured. Nginx's `client_max_body_size 100M` rejects any request body over 100 MB — meaning any file over 100 MB fails. The "progress" shown in the UI before failure is XHR upload progress to nginx; nginx was buffering the body and then rejecting it.

The TUS client retries the rejected PATCH using the configured `retryDelays`. After ~30 seconds of retries the frontend upload timeout fires, calling `upload.abort(true)` which triggers the DELETE to clean up the partial upload on the server.

**Fix:**
Set `chunkSize: 50 * 1024 * 1024` (50 MB) in the TUS client config in `UploadDatasetStepper.vue` and `GenericUploader.vue`. 50 MB is safely under the 100 M nginx limit and enables TUS's protocol-level resume (without chunking there is nothing to resume from).

```javascript
// ui/src/components/dataset/upload/UploadDatasetStepper.vue
// ui/src/components/upload/GenericUploader.vue
upload = new tus.Upload(file, {
  chunkSize: 50 * 1024 * 1024,  // 50 MB per PATCH request
  ...
});
```

---

### 2. Nginx Buffering Chunks Before Forwarding to API

**Symptom:**
- PATCH requests show upload progress in the browser but never appear in API logs
- Uploads of files slightly under 100 MB may hang or fail inconsistently

**Root cause:**
By default, nginx buffers the entire request body from the client before forwarding it to the upstream API (`proxy_request_buffering on`). This means nginx holds the full 50 MB chunk in memory/disk before the API sees any of it. This negates TUS's streaming design, increases latency, and can cause failures if nginx's internal buffer limits are hit.

**Fix:**
Add a dedicated nested `location /api/uploads/` block inside the main `/api/` block in `/etc/nginx/conf.d/cmg.conf`. Using a nested block (rather than modifying the main `/api/` block) means all other API endpoints are unaffected.

```nginx
location /api/ {
    # ... existing config unchanged ...

    location /api/uploads/ {
        proxy_pass http://172.19.0.2:3030/uploads/;
        proxy_request_buffering off;   # stream chunk body directly to API, no buffering
        client_body_timeout 300;       # allow slow clients up to 5 min to send a chunk
        proxy_read_timeout 300;        # allow API up to 5 min to respond after receiving a chunk
    }
}
```

The nested location covers all TUS endpoints:
- `POST /api/uploads/files` — create upload
- `PATCH /api/uploads/files/{id}` — send chunk (primary upload path)
- `HEAD /api/uploads/files/{id}` — query offset for resume
- `DELETE /api/uploads/files/{id}` — abort/terminate

Nginx prefix matching means `/api/uploads/` (more specific) always wins over `/api/` for these paths.

---

### 3. Nginx Timeout Directives Explained

The two timeout directives added to the `/api/uploads/` block serve distinct purposes:

| Directive | Controls | Default | Why It Matters for Uploads |
|-----------|----------|---------|---------------------------|
| `client_body_timeout 300` | How long nginx waits between successive reads of the request body **from the client** | 60s | On a slow connection, uploading a 50 MB chunk at ~1 MB/s takes ~50 s — borderline for the 60s default. 300s gives a 5-minute window per chunk for slow connections. |
| `proxy_read_timeout 300` | How long nginx waits for **a response from the API** after forwarding the request | 60s | After receiving a full chunk, the API writes it to disk and responds with the new offset. On a loaded server with slow disk I/O, this could theoretically exceed 60s. 300s prevents premature gateway timeouts. |

Note: `proxy_buffering` (response buffering, upstream → client) is separate and not changed — it has no effect on upload performance.

---

### 4. Frontend Upload Timeout Too Tight for Large Files

**Symptom:**
- Uploads of large files abort with "Upload timeout after 30 seconds" even when the server is healthy
- `DELETE /api/uploads/files/{id}` appears exactly 30 seconds after the POST (the abort cleanup)

**Root cause:**
A hardcoded `UPLOAD_TIMEOUT_MS = 30000` (30 s) was set as an overall timeout for each file upload. This was originally intended to catch TUS getting permanently stuck in a retry loop, but it also fires for any legitimate large file upload that takes more than 30 s — which includes any file over ~300 MB on a 10 MB/s connection.

**Fix:**
Make the timeout proportional to file size, assuming a minimum upload speed of 512 KB/s, with a floor of 60 s and a ceiling of 30 minutes:

```javascript
// ui/src/components/dataset/upload/UploadDatasetStepper.vue
const MIN_TIMEOUT_MS = 60 * 1000;           // 60 seconds minimum
const MAX_TIMEOUT_MS = 30 * 60 * 1000;      // 30 minutes maximum
const MIN_SPEED_BYTES_PER_MS = 512 * 1024 / 1000;  // 512 KB/s
const UPLOAD_TIMEOUT_MS = Math.min(MAX_TIMEOUT_MS, Math.max(MIN_TIMEOUT_MS, file.size / MIN_SPEED_BYTES_PER_MS));
```

Representative values:

| File size | Timeout |
|-----------|---------|
| 10 MB | 60 s (floor) |
| 100 MB | ~3.3 min |
| 300 MB | ~9.7 min |
| 10 GB | 30 min (ceiling) |

The timeout does not affect upload speed. It is only a failure-detection safeguard: `onSuccess` clears the timer immediately when an upload completes normally, regardless of how long it took.

---

## Future Extensibility

The TUS architecture supports:

1. **Parallel uploads** — Multiple files upload concurrently (already implemented)
2. **Progress webhooks** — TUS supports server-side progress callbacks
3. **Distributed storage** — Swap FileStore for S3/GCS store (tus ecosystem)
4. **Upload quotas** — Add middleware before TUS server
5. **Other entity types** — Same infrastructure for session files, reports, etc.

---

## Summary

| Metric | Before | After |
|--------|--------|-------|
| Services in upload path | 4 | 2 |
| Resume implementation | Custom | Protocol-native |
| Chunk assembly | Worker job | TUS handles |
| Memory per upload | O(chunk) | O(buffer) |
| Checksum algorithm | MD5 | BLAKE3 |
| Max file size | ~10 GB practical | 100 GB configured |
| Client state required | Yes | No |

**Bottom line:** Industry-standard protocol, simpler architecture, better reliability.

---

*Last Updated: 2026-02-18*
