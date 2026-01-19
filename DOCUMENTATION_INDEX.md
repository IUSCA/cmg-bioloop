# Bioloop Documentation Index
**Last Updated:** 2026-01-11  
**For:** Developers & AI Agents

---

## 📖 Overview

This document serves as the **master index** for all Bioloop documentation. Whether you're a new developer, an AI agent, or maintaining the system, start here to find the right documentation for your needs.

---

## 🎯 Quick Start

**New to Bioloop?** Read these in order:
1. [Genome Browser, Sessions, Tracks & Conversions Architecture](#primary-architecture-documentation) - System overview
2. [IGV & WashU Implementation Guide](#primary-architecture-documentation) - Browser integration details
3. [Conversion Pipelines Architecture](#primary-architecture-documentation) - Data transformation system
4. [Project Coding Conventions (`.cursorrules`)](#coding-conventions) - Essential coding patterns

**Need to find specific info?** Use the sections below to navigate directly to relevant docs.

---

## 📘 Primary Architecture Documentation

These are the **authoritative, current** documentation files. All other docs are either archived or supplementary.

### 1. **Genome Browser, Sessions, Tracks & Conversions Architecture**
**File:** `genome-browser-sessions-tracks-conversions-2026-01-03.md`  
**Last Updated:** 2026-01-03  
**Status:** ✅ Current & Accurate

**What it covers:**
- Complete system architecture overview
- Database schema for sessions, tracks, and conversions
- Entity relationships and data flow
- API endpoints for all three features
- UI component structure
- File serving and authentication patterns
- Integration between sessions, tracks, and conversions

**When to use:**
- Understanding the overall system architecture
- Database schema reference
- API endpoint documentation
- Learning how sessions, tracks, and conversions relate

**Key sections:**
- System Overview & Key Features
- Database Models (Prisma schema)
- Session Lifecycle (creation → viewing → browser opening)
- Track Management (creation → selection → visualization)
- Conversion Architecture (pipeline definitions → execution → outputs)
- File Serving & Authentication (cookie-based auth, range requests)
- Code Location Reference

---

### 2. **IGV & WashU Genome Browser Implementation Guide**
**File:** `genome-browser-igv-washu-implementation-2026-01-03.md`  
**Last Updated:** 2026-01-03  
**Status:** ✅ Current & Accurate

**What it covers:**
- Detailed technical implementation for both genome browsers
- IGV.js pure JavaScript integration
- WashU React-in-Vue integration patterns
- Track serialization for both browsers
- Authentication and file serving specifics
- Comprehensive troubleshooting guide

**When to use:**
- Implementing or debugging genome browser features
- Understanding React-in-Vue integration
- Troubleshooting browser-specific issues
- Adding support for new file formats
- Understanding cookie-based authentication for file access

**Key sections:**
- Browser Constants (IGV vs WashU)
- Common Infrastructure (browser selection modal)
- IGV Implementation (initialization, datahub format, cleanup)
- WashU Implementation (React wrapper, Redux persistence, event isolation)
- Track Serialization (format conversion for each browser)
- Authentication & File Serving (cookie flow, range requests)
- Troubleshooting Guide (common issues & solutions)

**Special topics:**
- React-Vue event isolation
- Redux persistence disabled for WashU
- Absolute URLs required for Web Workers
- Force remount patterns for clean state

---

### 3. **Genomic Data Conversion Pipelines Architecture**
**File:** `genome-conversion-pipelines-2026-01-11.md`  
**Last Updated:** 2026-01-11  
**Status:** ✅ Current & Accurate

**What it covers:**
- Conversion system architecture and design
- Conversion definitions, programs, and arguments
- Database schema for conversion management
- Dynamic variables and runtime resolution
- Worker implementation (Python Celery)
- CMG legacy pipeline migration strategy

**When to use:**
- Understanding the conversion/pipeline system
- Adding new conversion definitions
- Implementing worker tasks
- Migrating CMG conversion pipelines
- Understanding conversion → track → session integration

**Key sections:**
- Conversion Definitions (pipeline templates)
- Command-Line Programs & Arguments (parameter definitions)
- Conversion Execution (lifecycle & workflow)
- Worker Implementation (Python Celery tasks, command construction)
- UI Components (conversion forms, monitoring)
- CMG Migration Strategy (legacy pipeline mapping)

**Special topics:**
- Generic conversion framework (plugin-based)
- Argument validation (types, constraints, enums)
- Dynamic variable resolution
- Output dataset creation and linking

---

## 🛠️ Coding Conventions

### **Project Coding Conventions & Preferences**
**File:** `.cursorrules`  
**Last Updated:** 2026-01-03  
**Status:** ✅ Current & Essential Reading

**What it covers:**
- API development patterns (Prisma reuse, transactions, error handling)
- UI development conventions (Vuestic components, constants usage)
- Worker development patterns (Python configuration)
- Database patterns (Prisma includes, cascade deletes)
- Authentication patterns (permission checking)
- Common pitfalls and best practices

**When to use:**
- **BEFORE writing any code** (essential for consistency)
- When unsure about component names or patterns
- When implementing transactions across multiple services
- When adding new constants or config values
- When encountering linter errors

**Key sections:**
1. Project Structure & Constants (business logic vs config)
2. API Development Conventions (Prisma, transactions, routes)
3. UI Development Conventions (Vuestic, React-in-Vue, CSS)
4. Worker Development Conventions (Python config, task patterns)
5. Database & Prisma Patterns (schema, cascade deletes)
6. Authentication & Authorization (`canAdmin`, `canOperate`)
7. File Operations & Serving (path construction, compression)
8. Error Handling & Logging (createError, logger usage)
9. Component-Specific Guidelines (genome browsers, forms, modals)
10. Testing & Development Workflow (HMR, Docker, linter)

**Quick reference checklists included:**
- Starting new API route
- Starting new UI component
- Adding new constant
- Implementing transaction
- Embedding React component in Vue

---

## 📁 Archived Documentation

**Location:** `docs/archive/`  
**README:** `docs/archive/ARCHIVE_README.md`

These files represent **outdated or abandoned approaches**. They are preserved for historical context only.

### Archived Files:
1. `in_app_genome_browsers.md` - Early design for cookie-based auth (abandoned approach)
2. `DATAHUB_TOKEN_AUTHENTICATION.md` - Token-in-query-parameter auth (never implemented)
3. `SECURE_GENOME_BROWSER_IMPLEMENTATION.md` - OAuth2 approach (incomplete/abandoned)
4. `SESSIONS_TRACKS_IMPLEMENTATION_STATUS.md` - Early status checklist (now outdated)
5. `CONVERSION_FEATURE_ANALYSIS.md` - CMG vs CFNDAP comparison (replaced by comprehensive conversion docs)

**⚠️ WARNING:** Do NOT use code patterns or architectural decisions from archived documents. They do not reflect the current system.

See `docs/archive/ARCHIVE_README.md` for detailed explanations of why each file was archived and what replaced it.

---

## 🗂️ Documentation by Feature

### Sessions Feature

**Primary Docs:**
- `genome-browser-sessions-tracks-conversions-2026-01-03.md` (§4: Genome Browser Sessions)
- `genome-browser-igv-washu-implementation-2026-01-03.md` (§3: Common Infrastructure)

**What you'll find:**
- Session creation workflow
- Track selection and management within sessions
- Public vs private sessions
- Access control and permissions
- Browser selection modal
- Session → datahub export

**Key API Endpoints:**
- `POST /sessions` - Create session
- `GET /sessions/:id` - Get session details
- `PATCH /sessions/:id` - Update session
- `DELETE /sessions/:id` - Delete session
- `GET /sessions/:id/datahub?browser=igv|washu` - Export for browsers
- `POST /sessions/:id/set-file-cookie` - Set auth cookie
- `GET /sessions/:id/files/expose/*` - Serve files

**Key UI Components:**
- `ui/src/pages/sessions/index.vue` - Session list
- `ui/src/pages/sessions/new.vue` - Create session
- `ui/src/pages/sessions/[id].vue` - Session detail & browser rendering
- `ui/src/components/genomeBrowser/BrowserSelectionModal.vue`

---

### Tracks Feature

**Primary Docs:**
- `genome-browser-sessions-tracks-conversions-2026-01-03.md` (§5: Tracks)
- `genome-browser-igv-washu-implementation-2026-01-03.md` (§6: Track Serialization)

**What you'll find:**
- Track creation from dataset files
- Track file types and formats
- Track selection for sessions
- Track metadata (color, name, genome)
- Track serialization for IGV and WashU

**Key API Endpoints:**
- `GET /tracks` - List/search tracks
- `GET /tracks/:id` - Get track details
- `POST /tracks` - Create track
- `PATCH /tracks/:id` - Update track
- `DELETE /tracks/:id` - Delete track

**Key UI Components:**
- `ui/src/pages/tracks/index.vue` - Track list
- `ui/src/pages/tracks/new.vue` - Create track
- `ui/src/pages/tracks/[id].vue` - Track detail
- `ui/src/components/tracks/TracksAsyncAutoComplete.vue` - Track selector

**Track File Types Supported:**
- BAM (alignments)
- VCF_GZ (variants)
- BIGWIG (signal/coverage)
- BIGBED (regions)
- BED_GZ (regions compressed)
- CRAM (alignments compressed)
- FRAGMENTS_TSV_GZ (ATAC-seq fragments)

---

### Conversions Feature

**Primary Docs:**
- `genome-conversion-pipelines-2026-01-11.md` (complete reference)
- `genome-browser-sessions-tracks-conversions-2026-01-03.md` (§6: Conversions overview)

**What you'll find:**
- Conversion definitions and registry
- Command-line program definitions
- Argument validation and constraints
- Dynamic variable resolution
- Worker execution (Python Celery)
- CMG pipeline migration

**Key API Endpoints:**
- `GET /conversion-definitions` - List available conversions
- `GET /conversion-definitions/:id` - Get definition details
- `POST /conversions` - Initiate conversion
- `GET /conversions` - List conversions
- `GET /conversions/:id` - Get conversion details

**Key UI Components:**
- `ui/src/pages/conversions/index.vue` - Conversion list
- `ui/src/pages/conversions/new.vue` - Initiate conversion
- `ui/src/pages/conversions/[id].vue` - Conversion details
- `ui/src/pages/conversions/definitions/index.vue` - Browse definitions

**Key Worker Tasks:**
- `workers/workers/tasks/convert.py` - Generic conversion engine

---

### Genome Browsers (IGV & WashU)

**Primary Docs:**
- `genome-browser-igv-washu-implementation-2026-01-03.md` (complete reference)
- `genome-browser-sessions-tracks-conversions-2026-01-03.md` (§9: File Serving & Authentication)

**What you'll find:**
- IGV.js pure JavaScript integration
- WashU React-in-Vue wrapper component
- Browser selection flow
- Cookie-based authentication for file access
- Track serialization formats
- Troubleshooting common issues

**Key Implementation Files:**
- `ui/src/pages/sessions/[id].vue` - Browser initialization & rendering
- `ui/src/components/genomeBrowser/BrowserSelectionModal.vue` - Browser selection
- `ui/src/components/genomeBrowser/WashUBrowser.vue` - React-in-Vue wrapper
- `api/src/routes/sessions.js` - Datahub endpoint, file serving
- `ui/src/constants.js` - Browser constants

**Browser-Specific Patterns:**
- IGV: Pure JavaScript, simple cleanup
- WashU: React-in-Vue, Redux persistence disabled, absolute URLs required

---

## 🔍 Finding Information

### By Task

| Task | Documentation | Section |
|------|--------------|---------|
| Create a new session | `genome-browser-sessions-tracks-conversions-2026-01-03.md` | §4.1: Creation |
| Add track to session | `genome-browser-sessions-tracks-conversions-2026-01-03.md` | §5: Tracks |
| Open session in IGV | `genome-browser-igv-washu-implementation-2026-01-03.md` | §4: IGV Implementation |
| Open session in WashU | `genome-browser-igv-washu-implementation-2026-01-03.md` | §5: WashU Implementation |
| Create conversion definition | `genome-conversion-pipelines-2026-01-11.md` | §4: Conversion Definitions |
| Execute conversion | `genome-conversion-pipelines-2026-01-11.md` | §6: Conversion Execution |
| Implement new API route | `.cursorrules` | §2: API Development Conventions |
| Create new UI component | `.cursorrules` | §3: UI Development Conventions |
| Use Prisma transactions | `.cursorrules` | §2.3: Transaction Pattern |
| Embed React in Vue | `.cursorrules` | §3.8: React-in-Vue Integration |
| Troubleshoot browser issue | `genome-browser-igv-washu-implementation-2026-01-03.md` | §8: Troubleshooting |

### By Keyword

| Keyword | Documentation | Section |
|---------|--------------|---------|
| Authentication | `genome-browser-sessions-tracks-conversions-2026-01-03.md` | §9: File Serving & Authentication |
| Cookie auth | `genome-browser-igv-washu-implementation-2026-01-03.md` | §7: Authentication & File Serving |
| Prisma | `.cursorrules` | §5: Database & Prisma Patterns |
| Vuestic | `.cursorrules` | §3.1: Vuestic Component Usage |
| Constants | `.cursorrules` | §1: Project Structure & Constants |
| Transactions | `.cursorrules` | §2.3: Transaction Pattern |
| File serving | `genome-browser-sessions-tracks-conversions-2026-01-03.md` | §9: File Serving & Authentication |
| Range requests | `genome-browser-igv-washu-implementation-2026-01-03.md` | §7.2: Range Request Support |
| Track serialization | `genome-browser-igv-washu-implementation-2026-01-03.md` | §6: Track Serialization |
| Conversion definition | `genome-conversion-pipelines-2026-01-11.md` | §4: Conversion Definitions |
| Worker tasks | `genome-conversion-pipelines-2026-01-11.md` | §8: Worker Implementation |
| CMG migration | `genome-conversion-pipelines-2026-01-11.md` | §10: CMG Migration Strategy |

---

## 🏗️ System Architecture Summary

### High-Level Data Flow

```
User → Create Dataset → Upload/Import Files
                              ↓
                      Create Conversion
                              ↓
                      Execute Pipeline
                              ↓
                    Output Dataset (e.g., BAM, VCF)
                              ↓
                       Create Track
                              ↓
                      Add to Session
                              ↓
                Open in Genome Browser (IGV/WashU)
                              ↓
                    View Genomic Data
```

### Microservice Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Bioloop UI                          │
│                    (Vue 3 + Vuestic)                        │
│                                                             │
│  - Sessions Pages    - Genome Browser Components           │
│  - Tracks Pages      - Conversion Forms                    │
│  - Conversions Pages - Modal Components                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                      Bioloop API                            │
│                (Express.js + Prisma ORM)                    │
│                                                             │
│  - Session Endpoints     - Track Endpoints                 │
│  - Conversion Endpoints  - File Serving                    │
│  - Datahub Export        - Authentication                  │
└─────────────┬──────────────────────┬────────────────────────┘
              │                      │
              ↓                      ↓
┌─────────────────────┐  ┌──────────────────────────┐
│  PostgreSQL DB      │  │  Celery Workers          │
│  (Prisma Schema)    │  │  (Python)                │
│                     │  │                          │
│  - Sessions         │  │  - Conversion Execution  │
│  - Tracks           │  │  - Pipeline Processing   │
│  - Conversions      │  │  - Dataset Creation      │
│  - Datasets         │  │                          │
└─────────────────────┘  └──────────────────────────┘
```

### Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Vue 3, Vuestic UI, IGV.js, WashU (React) |
| Backend API | Node.js, Express.js, Prisma ORM |
| Database | PostgreSQL |
| Workers | Python, Celery |
| Authentication | JWT (cookie-based for file serving) |
| Containerization | Docker, Docker Compose |

---

## 📝 Documentation Maintenance

### When to Update Documentation

**Update immediately when:**
- API endpoints change (add/remove/modify)
- Database schema changes (migrations)
- New features are added
- Architecture patterns change
- Authentication mechanisms change
- File serving logic changes

**Update periodically for:**
- UI component structure changes
- Configuration changes
- Worker implementation changes
- Troubleshooting tips

### Documentation Update Process

1. **Identify affected files** (use this index to find them)
2. **Update the relevant sections** with accurate information
3. **Update the "Last Updated" date** at the top of the file
4. **Test examples and code snippets** to ensure accuracy
5. **Update this index** if new docs are added or files are moved

### Adding New Documentation

If you create new documentation:
1. Add entry to this index under appropriate section
2. Include "Last Updated" date in new file
3. Mark status (✅ Current, ⚠️ Incomplete, ❌ Outdated)
4. Add links to related documentation
5. Update the "Last Updated" date in this index

---

## 💡 Tips for AI Agents

### Before Starting Any Task:
1. ✅ Read relevant sections from this index
2. ✅ Check `.cursorrules` for coding patterns
3. ✅ Review database schema in `genome-browser-sessions-tracks-conversions-2026-01-03.md`
4. ✅ Check if similar functionality exists (avoid duplication)
5. ✅ Identify which services are affected (API, UI, Workers)

### When Implementing Features:
1. ✅ Follow patterns from `.cursorrules`
2. ✅ Reuse existing components and services
3. ✅ Use constants from `ui/src/constants.js` and `api/src/constants.js`
4. ✅ Add appropriate error handling and logging
5. ✅ Test with actual data

### When Debugging:
1. ✅ Check troubleshooting section in `genome-browser-igv-washu-implementation-2026-01-03.md`
2. ✅ Review authentication flow in documentation
3. ✅ Verify database relationships match schema
4. ✅ Check archived docs to see if issue was addressed before
5. ✅ Look for similar patterns in existing code

### Red Flags (Avoid These):
- ❌ Using archived documentation as source of truth
- ❌ Creating new Prisma instances instead of reusing `@/db`
- ❌ Using `auth.hasRole()` directly instead of helper methods
- ❌ Hardcoding browser types instead of using constants
- ❌ Not using transactions for multi-operation API calls
- ❌ Enabling compression for binary files
- ❌ Forgetting to unmount React components in Vue wrappers

---

## 📞 Getting Help

### Documentation Not Clear?
- Check related documents (use links in each doc)
- Search for keywords in this index
- Review code examples in `.cursorrules`
- Check archived docs for historical context (but don't use outdated patterns)

### Feature Not Documented?
- Check if it's in the Prisma schema (`api/prisma/schema.prisma`)
- Look for API endpoints (`api/src/routes/*.js`)
- Search UI components (`ui/src/pages/`, `ui/src/components/`)
- Check worker tasks (`workers/workers/tasks/*.py`)

### Found Documentation Bug?
- Update the relevant file immediately
- Update "Last Updated" date
- Update this index if structure changed

---

## 📅 Documentation History

| Date | Changes | Files Affected |
|------|---------|----------------|
| 2026-01-11 | Major documentation cleanup and consolidation | All |
| 2026-01-11 | Created comprehensive conversion pipelines doc | `genome-conversion-pipelines-2026-01-11.md` |
| 2026-01-11 | Archived outdated genome browser docs | `docs/archive/*` |
| 2026-01-11 | Created master documentation index | `DOCUMENTATION_INDEX.md` |
| 2026-01-03 | Created sessions/tracks/conversions overview | `genome-browser-sessions-tracks-conversions-2026-01-03.md` |
| 2026-01-03 | Created IGV/WashU implementation guide | `genome-browser-igv-washu-implementation-2026-01-03.md` |
| 2026-01-03 | Created project coding conventions | `.cursorrules` |

---

**End of Documentation Index**

---

**Quick Links:**
- [Architecture Docs](#primary-architecture-documentation)
- [Coding Conventions](#coding-conventions)
- [Archived Docs](docs/archive/ARCHIVE_README.md)
- [Sessions Feature](#sessions-feature)
- [Tracks Feature](#tracks-feature)
- [Conversions Feature](#conversions-feature)
- [Genome Browsers](#genome-browsers-igv--washu)


