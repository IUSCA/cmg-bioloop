# Archived Documentation

This folder contains documentation that is **outdated** or describes **abandoned approaches** to genome browser integration in Bioloop.

## Why These Files Were Archived

These documents represent earlier design iterations and implementation attempts that were superseded by the current architecture. They are preserved here for historical reference only.

---

## Archived Files

### 1. `in_app_genome_browsers.md`
**Date Archived**: 2026-01-11  
**Status**: ❌ **ABANDONED APPROACH**

**What it described:**
- Cookie-based authentication for in-app IGV browser only
- Stated intention to implement IGV but NOT WashU
- Proposed moving `/files/expose` from `secure_download` to core API
- In-app browser embedded in modal windows

**Why it's outdated:**
- Current implementation includes **both IGV AND WashU** browsers
- Current implementation uses **cookie-based auth** but with a different architecture
- File serving remains at `/sessions/:id/files/expose/*` but with refined authentication
- Architecture evolved beyond the simple approach described here

**Replaced by:**
- `genome-browser-igv-washu-implementation-2026-01-03.md` (current implementation guide)

---

### 2. `DATAHUB_TOKEN_AUTHENTICATION.md`
**Date Archived**: 2026-01-11  
**Status**: ❌ **ABANDONED APPROACH**

**What it described:**
- Token-in-query-parameter authentication for external WashU browser
- `GET /sessions/:id/datahub-token` endpoint for generating short-lived tokens
- 5-minute automatic token refresh intervals
- External genome browsers opening in new tabs
- `authenticateWithQueryToken` middleware

**Why it's outdated:**
- **None of these endpoints exist** in the current codebase
- Current implementation uses **in-app embedded browsers**, not external tabs
- Current authentication is **cookie-based**, not query-parameter tokens
- Token refresh mechanism was never implemented

**Replaced by:**
- `genome-browser-sessions-tracks-conversions-2026-01-03.md` (current architecture)
- Cookie-based authentication described in IGV/WashU implementation guide

---

### 3. `SECURE_GENOME_BROWSER_IMPLEMENTATION.md`
**Date Archived**: 2026-01-11  
**Status**: ❌ **INCOMPLETE/ABANDONED**

**What it described:**
- OAuth2-based file serving through `secure_download` microservice
- `/genome-browser/*` endpoint in secure_download
- `get_file_exposure_token()` function for token generation
- Client credentials OAuth2 flow
- File-scoped JWT tokens with `genome_browser_file:` scope

**Why it's outdated:**
- OAuth2 integration described here was **never fully implemented**
- Current file serving uses `/sessions/:id/files/expose/*` pattern
- No evidence of OAuth2 client credentials flow in current code
- The `secure_download` service integration differs from what's described

**Replaced by:**
- Current cookie-based file serving architecture
- File exposure endpoints in `api/src/routes/sessions.js`

---

### 4. `SESSIONS_TRACKS_IMPLEMENTATION_STATUS.md`
**Date Archived**: 2026-01-11  
**Status**: ⚠️ **PARTIALLY OUTDATED** (Historical Reference)

**What it described:**
- Implementation status checklist as of early 2025
- **Stated**: "In-App Genome Browser - INTENTIONALLY NOT IMPLEMENTED"
- **Stated**: External genome browsers always open in new tabs
- Progress tracking on sessions and tracks features
- Comparison with CMG legacy system

**Why it's outdated:**
- **In-app genome browser IS NOW IMPLEMENTED** (both IGV and WashU)
- **Browsers are embedded**, not opening in external tabs
- Implementation status has significantly progressed since this was written
- Many "planned" features are now complete

**Why we kept it:**
- Useful for understanding **implementation history**
- Good context on design decisions and evolution
- Shows what features were prioritized and why

**Current status documented in:**
- `genome-browser-sessions-tracks-conversions-2026-01-03.md`

---

## Current Documentation (Use These Instead)

The following files in the project root represent the **current, accurate** documentation:

### **📘 Primary Documentation**

1. **`genome-browser-sessions-tracks-conversions-2026-01-03.md`**
   - Comprehensive architecture overview
   - Database schema and entity relationships
   - API endpoints for sessions, tracks, and conversions
   - UI component structure
   - File serving and authentication flow
   - **This is the authoritative source for understanding the system**

2. **`genome-browser-igv-washu-implementation-2026-01-03.md`**
   - Detailed technical implementation guide
   - IGV.js integration specifics
   - WashU (React-in-Vue) integration details
   - Track serialization for both browsers
   - Authentication patterns
   - Comprehensive troubleshooting guide
   - **This is the go-to reference for implementing or debugging genome browsers**

3. **`.cursorrules`**
   - Project coding conventions and preferences
   - API, UI, and Worker development patterns
   - Prisma usage patterns
   - Component guidelines (Vuestic, React-in-Vue)
   - Common pitfalls and best practices
   - **Essential reading for any developer or AI agent working on this project**

### **📁 Specialized Documentation**

4. **`CONVERSION_FEATURE_ANALYSIS.md`**
   - Comparison of CMG vs CFNDAP conversion features
   - Migration strategy for conversion pipelines
   - Data model mapping
   - **Relevant for conversion feature development only**

---

## Timeline of Architecture Evolution

### **Phase 1: External Browser Approach** (Early 2025)
- Proposed opening genome browsers in new tabs
- Token-based authentication in URLs
- CORS complications
- **Status**: Abandoned

### **Phase 2: OAuth2 Secure Download** (Mid 2025)
- Attempted OAuth2 integration with secure_download service
- File-scoped JWT tokens
- Complex token generation flow
- **Status**: Incomplete/Abandoned

### **Phase 3: Current In-App Browser** (Late 2025 - Present)
- Embedded IGV and WashU browsers in application
- Cookie-based authentication
- Same-origin file serving
- React-in-Vue integration for WashU
- **Status**: ✅ Fully Implemented

---

## How to Use This Archive

**If you're looking for:**
- **Current implementation details** → See root-level docs (genome-browser-*.md files)
- **Why a certain approach was chosen** → Check this archive for historical context
- **Implementation history** → `SESSIONS_TRACKS_IMPLEMENTATION_STATUS.md` (archived)
- **Abandoned approaches to avoid** → This README

**Warning**: Do NOT use code patterns or architectural decisions from these archived documents. They represent abandoned or incomplete approaches that do not reflect the current system.

---

**Last Updated**: 2026-01-11  
**Archived By**: AI Agent Documentation Cleanup  
**Current Documentation Version**: 2026-01-03


