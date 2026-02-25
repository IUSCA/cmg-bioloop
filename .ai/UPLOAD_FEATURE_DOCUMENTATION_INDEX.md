# Upload Feature - Complete Documentation Index

**Purpose:** Guide for porting the rewritten TUS-based upload feature from CMG to Bioloop  
**Last Updated:** 2026-02-11  
**Feature Status:** Production-Ready

---

## 📚 Required Reading (Priority Order)

### 1. Core Feature Documentation

#### **`.ai/bioloop/features/uploads.md`** ⭐ START HERE
- **Priority:** CRITICAL - Read First
- **Content:**
  - Complete feature overview and architecture
  - All upload statuses and state machine
  - TUS protocol implementation details
  - API endpoints (14 routes documented)
  - Database schema (tables, relations, fields)
  - Path construction patterns
  - Polling job architecture
  - BLAKE3 manifest hash algorithm
  - Key files reference (API, workers, UI)
  - Complete changelog with all major changes
  - Schema refactor history (audit_log removal)
  - Route consolidation details
  - Dead code removal list
- **Why Critical:** Single source of truth for the entire upload feature
- **Lines:** 739 lines

---

### 2. Architecture & Design

#### **`docs/features/tus-upload-architecture.md`**
- **Priority:** HIGH - Read Second
- **Content:**
  - Executive summary of TUS migration rationale
  - Architecture comparison: Old (chunk-based) vs New (TUS)
  - Visual diagrams of upload flow
  - Complete state machine with all transitions
  - Component roles (TUS Client, TUS Server, API, Workers)
  - Advantages over chunk-based system
  - Performance benefits (streaming vs buffering)
  - BLAKE3 vs MD5 comparison
  - Operational benefits and risk mitigation
  - Future extensibility options
- **Why Important:** Understand the "why" behind architectural decisions
- **Lines:** 321 lines
- **Audience:** Tech leads, architects, developers new to the codebase

---

### 3. Schema Changes

#### **`.ai/SCHEMA_REFACTOR_2026_02_11.md`**
- **Priority:** HIGH - Read Third
- **Content:**
  - Schema refactor overview (removed audit_log intermediary)
  - Database changes: `dataset_upload_log`, `dataset_import_log`, `dataset`
  - Migration file details (`20260211_refactor_upload_import_logs_remove_audit_relation`)
  - Code changes summary (7 API files, 2 worker files, 3 UI files)
  - Route consolidation: `/uploads/*` → `/datasets/uploads/*`
  - Query simplification examples (before/after)
  - Route parameter standardization (all `:id` = `dataset_id`)
  - Files modified with detailed change descriptions
- **Why Important:** Understand database schema and data model changes
- **Lines:** 546 lines

---

### 4. Code Cleanup Documentation

#### **`.ai/UPLOAD_CLEANUP_2026_02_11.md`**
- **Priority:** MEDIUM
- **Content:**
  - Complete list of deleted files (9 files)
  - Code removed from 11 files
  - OAuth upload credentials removal
  - spark-md5 dependency removal
  - Dead workflow configuration cleanup
  - Verification results (no orphaned code)
  - Current authentication flow (Bearer token)
- **Why Important:** Understand what was removed and why (avoid re-implementing dead code)
- **Lines:** 165 lines

---

### 5. Testing & Debugging

#### **`UPLOAD_FAILURE_TESTING.md`**
- **Priority:** MEDIUM
- **Content:**
  - How to test TUS resumable upload functionality
  - Failure simulation scenarios (mid-upload failure)
  - localStorage flags for testing
  - Expected behaviors for different failure counts
  - TUS retry configuration
  - Log inspection instructions
- **Why Important:** Test upload resume functionality and error handling
- **Lines:** 250 lines

#### **`UPLOAD_FAILURE_SIMULATION_GUIDE.md`**
- **Priority:** LOW (detailed testing scenarios)
- **Content:**
  - Detailed developer testing guide
  - How the simulation system works (3 components)
  - Client-side configuration
  - API middleware logic
  - FileStore simulation implementation
  - Multiple test scenarios with expected outcomes
  - Verification checklist
  - Troubleshooting guide
- **Why Important:** Deep dive into failure simulation for comprehensive testing
- **Lines:** 685 lines

---

### 6. Documentation Status

#### **`UPLOAD_DOCS_STATUS_REPORT.md`**
- **Priority:** LOW (meta-documentation)
- **Content:**
  - Documentation accuracy verification
  - Status of all upload-related docs
  - Critical issues resolved
  - Dead code cleanup status
  - Recommendations
- **Why Important:** Verify all documentation is current
- **Lines:** 180 lines

---

## 🔧 Platform Conventions & Patterns

### General Development Patterns

#### **`.ai/AI_PROTOCOL.md`**
- **Priority:** HIGH
- **Content:** How to work with this repository, documentation standards
- **Why Important:** Understand the development workflow and conventions

#### **`.ai/bioloop/architecture.md`**
- **Priority:** HIGH
- **Content:** 
  - Microservice architecture overview
  - Workflow integration patterns
  - Database patterns
  - API/Worker/UI interaction patterns
- **Why Important:** Understand how uploads fit into overall architecture

#### **`.ai/bioloop/api_conventions.md`**
- **Priority:** MEDIUM
- **Content:**
  - API route patterns
  - Permission middleware (`isPermittedTo`)
  - Error handling
  - Transaction patterns
  - Workflow creation patterns
- **Why Important:** Follow platform conventions when porting code

#### **`.ai/bioloop/worker_conventions.md`**
- **Priority:** MEDIUM
- **Content:**
  - Celery task patterns
  - Workflow task signatures
  - API client usage
  - Error handling in workers
- **Why Important:** Implement polling job and verification tasks correctly

#### **`.ai/bioloop/ui_conventions.md`**
- **Priority:** MEDIUM
- **Content:**
  - Vue.js patterns
  - Component structure
  - API service patterns
  - Router patterns
- **Why Important:** Port UI components following platform patterns

#### **`.ai/bioloop/database_patterns.md`**
- **Priority:** MEDIUM
- **Content:**
  - Prisma usage patterns
  - Transaction patterns
  - Include patterns
  - Migration strategies
- **Why Important:** Handle database operations correctly

#### **`.ai/bioloop/pitfalls.md`**
- **Priority:** MEDIUM
- **Content:** Common mistakes and how to avoid them
- **Why Important:** Avoid known issues during porting

---

## 🔗 Related Features

### Import Feature (Related to Uploads)

#### **`.ai/bioloop/features/imports-downloads.md`**
- **Priority:** LOW
- **Content:**
  - Import feature overview
  - Shared patterns with upload feature
  - `dataset_import_log` schema
  - Relationship between imports and uploads
- **Why Important:** Understand how import logs relate to upload logs (same schema refactor)

---

## 📋 Quick Reference Checklist

**Before starting the port:**

- [ ] Read `.ai/bioloop/features/uploads.md` (complete overview)
- [ ] Read `docs/features/tus-upload-architecture.md` (architecture)
- [ ] Read `.ai/SCHEMA_REFACTOR_2026_02_11.md` (schema changes)
- [ ] Read `.ai/AI_PROTOCOL.md` (development workflow)
- [ ] Read `.ai/bioloop/architecture.md` (platform architecture)
- [ ] Read relevant conventions docs (API, Worker, UI, Database)
- [ ] Review `.ai/UPLOAD_CLEANUP_2026_02_11.md` (what NOT to port)
- [ ] Set up failure testing using `UPLOAD_FAILURE_TESTING.md`

**Key files to port:**

**API (7 files):**
- `api/src/routes/datasets/uploads.js` (all upload routes)
- `api/src/middleware/tus.js` (TUS authentication & failure simulation)
- `api/src/services/upload.js` (TUS server config, TestableFileStore)
- `api/src/services/dataset.js` (upload-related functions)
- `api/src/constants.js` (UPLOAD_STATUSES, includes)
- `api/config/default.json` (TUS config, upload paths)
- `api/src/app.js` (TUS server mounting)

**Workers (4 files):**
- `workers/workers/scripts/manage_upload_workflows.py` (polling job)
- `workers/workers/tasks/verify_upload.py` (verification task)
- `workers/workers/scripts/verify_upload_integrity.py` (verification script)
- `workers/workers/upload.py` (BLAKE3 manifest hash)

**UI (5+ files):**
- `ui/src/components/dataset/upload/UploadDatasetStepper.vue` (main upload UI)
- `ui/src/services/upload/checksum.js` (BLAKE3 manifest hash)
- `ui/src/pages/datasets/uploads/index.vue` (upload list)
- `ui/src/pages/datasets/uploads/[id].vue` (upload details)
- `ui/src/services/dataset.js` (upload API calls)
- `ui/src/components/dataset/upload/*` (supporting components)

**Database:**
- Migration: `api/prisma/migrations/20260211_refactor_upload_import_logs_remove_audit_relation/`
- Schema: See `api/prisma/schema.prisma` for `dataset_upload_log`, `dataset`, relations

---

## 🗑️ Dead Code to Remove from Target Bioloop Fork

**If the target Bioloop fork has the OLD chunk-based upload system, these must be REMOVED/REPLACED:**

See `.ai/UPLOAD_CLEANUP_2026_02_11.md` for complete list of what was deleted:

**Remove these from the old implementation:**
- OAuth upload token mechanism (`get_upload_token()`, `OAUTH_UPLOAD_CLIENT_*`)
- Upload token refresh mechanism (`refreshUploadToken()`, `uploadToken` refs)
- `spark-md5` dependency (replaced by BLAKE3)
- `process_dataset_upload` workflow (replaced by polling job)
- `cancel_dataset_upload` workflow (no longer needed)
- Chunk-based upload code (replaced by TUS protocol)
- `entity_type`/`entity_id` parameters (replaced by direct `dataset_id`)
- `audit_log_id` in upload logs (replaced by direct `dataset_id`)
- Old `/uploads/*` routes (replaced by `/datasets/uploads/*`)
- `ui/src/services/upload/token.js` file
- `ui/src/pages/datasetUpload/` directory (replaced by `datasets/uploads/`)
- `upload_scope` config in secure_download

**Note:** These have already been cleaned up in CMG, so you won't find them in the source code. This list helps you identify what to delete from the target fork during migration.

---

## 📦 Dependencies to Install

**API:**
- `@tus/server`
- `@tus/file-store`

**UI:**
- `tus-js-client`
- `hash-wasm` (for BLAKE3)

**Workers:**
- `blake3` (Python package)

---

## 🔐 Authentication

**Current Implementation:**
- TUS uploads use standard JWT Bearer token authentication
- Same token as all other API endpoints
- Token from `localStorage.getItem('token')`
- No separate OAuth upload credentials needed
- See `api/src/middleware/tus.js` for TUS authentication flow

---

## 🎯 Key Differences from Old System

**Old (Chunk-Based):**
- Custom chunk upload protocol
- secure_download service handling uploads
- MD5 checksums per chunk
- Worker-side chunk assembly
- OAuth2 client credentials for upload tokens
- Complex state management

**New (TUS):**
- TUS 1.0 protocol (industry standard)
- API service handling uploads
- BLAKE3 manifest hash (entire upload)
- TUS handles file assembly
- Standard Bearer token authentication
- Protocol-native resume capability

---

## 📞 Support

**If stuck during porting:**

1. Check `.ai/bioloop/pitfalls.md` for common issues
2. Review changelog in `.ai/bioloop/features/uploads.md`
3. Compare with `docs/features/tus-upload-architecture.md` for design intent
4. Check schema migration for database changes
5. Verify cleanup doc to ensure not reimplementing dead code

---

**Last Updated:** 2026-02-11  
**Feature Version:** TUS-based (post-chunk-migration)  
**Status:** Production-Ready in CMG
