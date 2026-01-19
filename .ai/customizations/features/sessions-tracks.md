# Sessions & Tracks Feature

**Feature Scope:** Genome browser sessions and tracks for visualizing genomic data in IGV and WashU browsers.

**Status:** Implemented

**Related Documentation:**
- `/genome-browser-sessions-tracks-conversions-2026-01-03.md`
- `/genome-browser-igv-washu-implementation-2026-01-03.md`
- `/SESSION_STAGING_ENHANCEMENT_COMPLETE.md`
- `/SESSION_STAGING_DYNAMIC_EVALUATION.md`

---

## 2026-01-16

### Initial State Documentation

**Context:** This feature enables users to create genome browser sessions that visualize genomic tracks (from dataset files) in IGV or WashU browsers.

**Key Architecture Decisions:**
- Decision: Sessions and tracks are separate but related entities
- Decision: One session can have multiple tracks (many-to-many via session_track)
- Decision: One dataset file can have at most one track
- Decision: Tracks reference dataset files, not datasets directly
- Decision: Genome information comes from dataset.genomic_details, not track table
- Constraint: Files must be staged before they can be accessed by genome browsers
- Constraint: All tracks in a session should have compatible genome types

**Database Schema:**
- `genome_browser_session`: Browser session configuration
  - Includes: title, description, genome_type, genome_value, browser_type
  - References: user (owner)
  - Has many: session_tracks (join table)
  
- `track`: Genome browser track configuration
  - Includes: name, track_type, color, track_specific_data (JSON)
  - References: dataset_file (one-to-one)
  - Has many: session_tracks (join table)
  - Note: Genome info accessed via track.dataset_file.dataset.genomic_details
  
- `session_track`: Join table linking sessions and tracks
  - Includes: session_id, track_id, display_order
  - Unique constraint: (session_id, track_id)

**File Serving Pattern:**
- Files served via `/api/sessions/:id/files/expose/*` routes
- Cookie-based authentication for genome browser access
- Compression DISABLED for binary genome files (critical)
- Range request support for efficient browser loading
- File paths constructed: `stage_alias + dataset_file.path`

**Browser Integration:**
- IGV: JavaScript library embedded in Vue
- WashU: React component embedded in Vue via React-in-Vue wrapper
- Both browsers support: BAM, BIGWIG, VCF_GZ, BED file types
- Browser selection: User chooses IGV or WashU when opening session

**Key Code Locations:**
- API Routes: `/api/src/routes/sessions.js`, `/api/src/routes/tracks.js`
- API Services: `/api/src/services/session.js`, `/api/src/services/track.js`
- File Exposure Router: `/api/src/routes/sessions.js` (fileExposureRouter)
- UI Components: `/ui/src/components/sessions/`, `/ui/src/components/tracks/`
- Browser Components: `/ui/src/components/genome-browser/`
- Prisma Schema: `/api/prisma/schema.prisma` (models: genome_browser_session, track, session_track)

**Critical Implementation Notes:**
1. **Router Mounting Order:** File exposure router MUST be mounted BEFORE global auth middleware
2. **Compression:** Disabled for `/files/expose/` routes (binary files break if compressed)
3. **Range Requests:** Supported for efficient file streaming
4. **Auto-Population:** Preserve manual user selections when auto-populating genome fields
5. **Track Selection:** Multiple track selection supported via async autocomplete
6. **File Restrictions:** Removed browser-compatible file restrictions - all files can be added

**Recent Changes (from memory):**
- Removed browserCompatibleExtensions concept (all files now allowed in sessions)
- Fixed genome field access (track doesn't have genome fields, accessed via relations)
- Enhanced track selection UI with multiple selection and display list
- Session validation allows empty genome values (flexible validation)
- Config consolidation: moved file type arrays to service config files

**Current Status:**
- Core session/track framework: Implemented
- File serving with cookie auth: Implemented
- IGV integration: Implemented
- WashU integration: Implemented
- Session staging: Implemented
- Track creation from dataset files: Implemented
- UI components: Implemented

---

## Future Entries

Add entries here as decisions are made, changes are implemented, or issues are resolved.

Format:
```
## YYYY-MM-DD

- Decision: [What was decided]
- Constraint: [New constraint or limitation]
- Clarification: [Behavior clarification]
- Change: [What changed and why]
```

---

**Last Updated:** 2026-01-16

