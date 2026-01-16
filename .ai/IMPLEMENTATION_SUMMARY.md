# .ai/ Directory Implementation Summary

**Date:** 2026-01-16  
**Task:** Implement AI knowledge base system for cross-workspace continuity

---

## What Was Implemented

### Core Documentation Files

Created in `.ai/` directory:

1. **AI_PROTOCOL.md**
   - How AI agents should work with this repo
   - Pre-work requirements (read feature changelogs)
   - Post-work requirements (append decisions to changelogs)
   - Priority order of truth (changelogs > code > chat)
   - Git synchronization awareness

2. **ARCHITECTURE.md**
   - Microservice architecture overview (UI, API, Workers)
   - Database architecture (PostgreSQL, Redis, MongoDB)
   - Cross-service communication patterns
   - Environment-specific behavior
   - Service restart requirements

3. **CONVENTIONS_API.md**
   - Import organization patterns
   - Prisma instance reuse
   - Transaction patterns
   - Route handler structure
   - Router mounting order (critical for file exposure)
   - Error handling and logging

4. **CONVENTIONS_UI.md**
   - Vuestic component usage (correct names, props)
   - Constants import pattern
   - CSS & styling preferences
   - Auto-population logic (preserve user input)
   - Page vs list component pattern
   - Toast notification rules
   - React-in-Vue integration

5. **CONVENTIONS_WORKERS.md**
   - Configuration pattern
   - Task definition pattern
   - Logging pattern

6. **PRISMA_PATTERNS.md**
   - Schema conventions (snake_case)
   - Cascade delete patterns
   - JSON field usage
   - Shared Prisma includes as constants

7. **GENOME_BROWSER_NOTES.md**
   - Generic vs browser-specific naming
   - Disable compression for binary files
   - Range request support
   - Cookie-based file access
   - File path construction
   - React-in-Vue integration specifics

8. **PITFALLS.md**
   - 18 common mistakes to avoid
   - Organized by category (Database, Auth, UI, Genome Browser, etc.)

9. **FEATURES.md**
   - Index of all feature changelogs
   - Quick reference with status and related docs

10. **README.md**
    - Overview of .ai/ directory structure
    - How AI agents should use it
    - Priority order of truth

---

### Feature Changelogs

Created in `.ai/features/` directory:

1. **cmg-database-migration.md**
   - Scope: CMG MongoDB → Bioloop PostgreSQL migration
   - Status: In Progress
   - Documents: Two-phase strategy (Big-Bang + Poller)
   - Initial state documented with architecture decisions

2. **conversions.md**
   - Scope: Genomic data conversion pipelines
   - Status: Implemented
   - Documents: Stateless pipeline executors, database schema, execution flow
   - Initial state documented with CMG migration notes

3. **sessions-tracks.md** (combined as requested)
   - Scope: Genome browser sessions and tracks
   - Status: Implemented
   - Documents: Session/track relationships, file serving, browser integration
   - Initial state documented with recent changes

---

### Updated .cursorrules

**Lean version** focusing on:
- Production environment warnings
- Access restrictions (paths, hosts, sudo)
- Critical restrictions (no /N/, no DB reset, no docker exec)
- Allowlisted commands (comprehensive list)
- Development workflow (Docker, HMR, linter policy)
- Quick pointers to `.ai/` documentation

**Removed from .cursorrules:**
- Detailed coding conventions (moved to `.ai/CONVENTIONS_*.md`)
- Architecture details (moved to `.ai/ARCHITECTURE.md`)
- Prisma patterns (moved to `.ai/PRISMA_PATTERNS.md`)
- Genome browser specifics (moved to `.ai/GENOME_BROWSER_NOTES.md`)
- Common pitfalls list (moved to `.ai/PITFALLS.md`)

---

## Directory Structure

```
.ai/
├── README.md                       # Overview and usage guide
├── AI_PROTOCOL.md                  # Agent workflow protocol
├── FEATURES.md                     # Feature index
├── ARCHITECTURE.md                 # System architecture
├── CONVENTIONS_API.md              # API patterns
├── CONVENTIONS_UI.md               # UI patterns
├── CONVENTIONS_WORKERS.md          # Worker patterns
├── PRISMA_PATTERNS.md              # Database patterns
├── GENOME_BROWSER_NOTES.md         # Browser implementation
├── PITFALLS.md                     # Common mistakes
└── features/
    ├── cmg-database-migration.md  # Migration changelog
    ├── conversions.md              # Conversions changelog
    └── sessions-tracks.md          # Sessions/Tracks changelog
```

---

## Key Principles

### Chats explore. Changelogs decide. Files persist.

This system ensures:
- Cross-window consistency (no hallucinations)
- Local/remote synchronization (Git-based)
- Feature-scoped work (dedicated changelogs)
- Explicit decisions (no silent drift)

### Priority Order of Truth

1. `.ai/features/<feature>.md` (highest)
2. Other `.ai/` documentation
3. Repository code
4. Chat history (lowest)

---

## What's NOT Covered (Native Bioloop Features)

The following are native Bioloop platform features and don't need dedicated feature changelogs (as per user request):

- **Datasets**: Core data management (already established)
- **Staging**: File staging mechanism (already established)
- **Import**: File upload feature (carried over from CMG's "Upload")
- **Users & Authentication**: User management, roles, permissions
- **Projects**: Project organization
- **Access Control**: ACLs, ownership checks

These are documented in the existing codebase and general documentation.

---

## Next Steps

1. **Use the protocol:** AI agents should follow `.ai/AI_PROTOCOL.md`
2. **Update changelogs:** After every significant decision or change
3. **Keep `.cursorrules` lean:** Only add production/safety concerns
4. **Sync with Git:** Use Git to keep changelogs synchronized across machines
5. **Add new features:** Create new files in `.ai/features/` as needed

---

**Implementation Status:** ✅ Complete

All files created and organized. System ready for use.

---

**Last Updated:** 2026-01-16

