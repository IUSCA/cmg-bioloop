# .ai/ Directory Reorganization Summary

**Date:** 2026-01-16  
**Task:** Reorganize AI knowledge base to separate platform core from CMG customizations

---

## What Was Implemented

### New Structure

```
.ai/
├── README.md                      # Updated: new structure overview
├── AI_PROTOCOL.md                 # Unchanged: agent workflow
│
├── bioloop/                       # NEW: Platform core
│   ├── README.md
│   ├── architecture.md
│   ├── api_conventions.md
│   ├── ui_conventions.md
│   ├── worker_conventions.md
│   ├── database_patterns.md
│   ├── pitfalls.md
│   └── features/
│       ├── datasets.md
│       ├── workflows.md
│       ├── users-projects.md
│       └── imports-downloads.md
│
└── customizations/                # NEW: CMG-specific
    ├── README.md
    ├── api_conventions.md
    ├── ui_conventions.md
    ├── pitfalls.md
    ├── genome_browser_notes.md
    └── features/
        ├── sessions-tracks.md
        ├── conversions.md
        └── cmg-database-migration.md
```

---

## Organization Principle

### Platform Core (`/bioloop/`)
- **Purpose:** Documentation shared across all Bioloop platform instances
- **Applies to:** Original Bioloop, CMG-Bioloop, any forks
- **Contents:**
  - Microservice architecture
  - API/UI/Worker conventions
  - Database patterns
  - Core features: Datasets, Workflows, Users/Projects, Imports/Downloads

### Customizations (`/customizations/`)
- **Purpose:** Documentation specific to CMG-Bioloop fork
- **Applies to:** This repo only
- **Contents:**
  - Genome browser integration
  - CMG-specific features: Sessions/Tracks, Conversions
  - CMG Database Migration
  - Override patterns for platform defaults

---

## Priority Order of Truth

When conflicts exist:

1. `.ai/customizations/features/<feature>.md` (highest - CMG-specific)
2. `.ai/customizations/*.md` (CMG conventions)
3. `.ai/bioloop/features/<feature>.md` (platform features)
4. `.ai/bioloop/*.md` (platform conventions)
5. Repository code
6. Chat history (lowest)

**Key principle:** Customizations can override or extend platform defaults.

---

## Updated Initialization Protocol

### When you say "hi" or "new task", agent will:

**Step 1: Auto-Read Core Files**
- `.ai/AI_PROTOCOL.md`
- `.ai/bioloop/README.md`
- `.ai/bioloop/architecture.md`
- `.ai/customizations/README.md`

**Step 2: Report & Confirm**
```
✅ Initialization complete. I've read:
- .ai/AI_PROTOCOL.md
- .ai/bioloop/ (platform core docs)
- .ai/customizations/ (CMG customizations)

Platform features: Datasets, Workflows, Users/Projects, Imports/Downloads
CMG features: Sessions/Tracks, Conversions, CMG Database Migration

Which feature are you working on? (or type "general" if not feature-specific)
```

**Step 3: Load Feature Context**
- Platform feature → Read `.ai/bioloop/features/<feature>.md`
- CMG feature → Read `.ai/customizations/features/<feature>.md`

**Step 4: Load Conventions as Needed**
- API code → Both `api_conventions.md` + both `pitfalls.md`
- UI code → Both `ui_conventions.md` + both `pitfalls.md`
- Workers → `bioloop/worker_conventions.md`
- Database → `bioloop/database_patterns.md`
- Genome browser → `customizations/genome_browser_notes.md`

---

## What Was Moved/Split

### Platform Core Files (Created)
- `bioloop/architecture.md` - Split from ARCHITECTURE.md (removed genome browser content)
- `bioloop/api_conventions.md` - Copied from CONVENTIONS_API.md (removed genome browser routes)
- `bioloop/ui_conventions.md` - Split from CONVENTIONS_UI.md (removed React-in-Vue)
- `bioloop/worker_conventions.md` - Copied from CONVENTIONS_WORKERS.md
- `bioloop/database_patterns.md` - Copied from PRISMA_PATTERNS.md
- `bioloop/pitfalls.md` - Split from PITFALLS.md (platform-only pitfalls)

### Platform Core Features (Created)
- `bioloop/features/datasets.md` - NEW: raw_data, data_product, staging
- `bioloop/features/workflows.md` - NEW: Python workflow framework
- `bioloop/features/users-projects.md` - NEW: User management, roles, projects
- `bioloop/features/imports-downloads.md` - NEW: File upload/download

### CMG Customization Files (Created/Moved)
- `customizations/api_conventions.md` - NEW: File exposure, cookies, range requests
- `customizations/ui_conventions.md` - NEW: React-in-Vue, genome browser patterns
- `customizations/pitfalls.md` - Split from PITFALLS.md (CMG-only pitfalls)
- `customizations/genome_browser_notes.md` - Moved from GENOME_BROWSER_NOTES.md
- `customizations/features/` - Moved from old `features/` directory

### Deleted Files
- ARCHITECTURE.md (split into bioloop/ and customizations/)
- CONVENTIONS_API.md (split)
- CONVENTIONS_UI.md (split)
- CONVENTIONS_WORKERS.md (moved to bioloop/)
- PRISMA_PATTERNS.md (moved to bioloop/)
- PITFALLS.md (split)
- FEATURES.md (replaced with README files)
- IMPLEMENTATION_SUMMARY.md (obsolete)

---

## Key Benefits

### 1. Clear Separation
- Platform code maintainers can update `bioloop/` without CMG knowledge
- CMG maintainers can update `customizations/` without breaking platform docs
- Forks can easily identify what's platform vs customization

### 2. Testable Initialization
- Agent explicitly reports what it read
- User can verify context is correct
- Reproducible across tabs/machines

### 3. Override Pattern
- Customizations explicitly extend or override platform defaults
- Priority order is clear and documented
- No ambiguity about which convention applies

### 4. Scalable
- New Bioloop forks can start with `bioloop/` and add their own `customizations/`
- CMG-specific work doesn't pollute platform docs
- Platform improvements benefit all forks

---

## Testing the New Structure

### Test 1: Open New Tab
1. Open new Cursor tab
2. Type: `hi`
3. Expected: Agent reads platform core + customizations, lists features, asks which one

### Test 2: Load CMG Feature
1. After initialization, say: `Sessions/Tracks`
2. Expected: Agent reads `.ai/customizations/features/sessions-tracks.md`

### Test 3: Load Platform Feature
1. After initialization, say: `Datasets`
2. Expected: Agent reads `.ai/bioloop/features/datasets.md`

### Test 4: API Work
1. Say: "I need to add a new API endpoint"
2. Expected: Agent reads both `api_conventions.md` files (platform + customizations)

---

## Maintenance Guidelines

### When to Update bioloop/
- Core platform patterns that apply to all Bioloop instances
- Architecture changes that affect all services
- Database patterns that are platform-wide
- UI/API conventions that are not CMG-specific

### When to Update customizations/
- Genome browser-related changes
- Sessions, Tracks, Conversions feature work
- CMG database migration updates
- Patterns specific to CMG data types or workflows

### When Unclear
- If it could apply to other Bioloop forks → `bioloop/`
- If it's specific to CMG data/workflows → `customizations/`
- When in doubt, document in both with appropriate context

---

## Migration Notes

### No Breaking Changes
- Old `.cursorrules` behavior still works (it just reads different files now)
- Agent initialization protocol is enhanced, not replaced
- Priority order is more explicit but functionally similar

### What Users Need to Know
- New tabs will auto-read more files (platform core + customizations)
- Feature work now distinguishes platform vs CMG features
- Conventions may be split across two files (platform + customizations)

---

**Implementation Status:** ✅ Complete

All files created, organized, and documented. System ready for use.

---

**Last Updated:** 2026-01-16

