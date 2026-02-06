# TUS Upload Architecture

**Audience:** Technical leadership, architects  
**Purpose:** High-level overview of the new TUS-based upload system

---

## Executive Summary

We've migrated from a custom chunk-based upload system to the **TUS (The Upload Server)** protocol. This is an open standard ([tus.io](https://tus.io)) designed specifically for reliable file uploads over HTTP, providing automatic resume capability for large files without custom implementation.

---

## Architecture Comparison

### Old Architecture (Chunk-Based)

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

**Issues:**
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

**Old Way:** Required storing chunk IDs, tracking completion state, handling retries manually.

**New Way:** Client asks server "where was I?" — server responds with byte offset. Zero client state.

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
                    │   UPLOADING   │◀──────────────────────┐
                    └───────┬───────┘                       │
                            │                               │
              TUS upload complete                    (Retry)│
                            │                               │
                            ▼                               │
                    ┌───────────────┐                       │
                    │   UPLOADED    │───────────────────────┘
                    └───────┬───────┘
                            │
              Polling job picks up
                            │
              ┌─────────────┴─────────────┐
              │                           │
     Verification OK            Verification Failed
              │                           │
              ▼                           ▼
    ┌─────────────────┐         ┌─────────────────────┐
    │    COMPLETE     │         │ VERIFICATION_FAILED │
    └────────┬────────┘         └─────────────────────┘
             │
    Workflow triggered
             │
             ▼
    ┌─────────────────┐
    │ Workflow status │
    │ (via Rhythm)    │
    └─────────────────┘
```

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

*Last Updated: 2026-02-05*
