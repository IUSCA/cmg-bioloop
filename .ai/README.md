# AI Documentation Directory

This directory contains documentation specifically designed for AI agents working on the Bioloop/CMG-Bioloop codebase.

---

## Purpose

These documents serve as the **single source of truth** for:
- Architectural patterns and conventions
- Feature specifications and behavior
- Common pitfalls and anti-patterns
- Development workflows

**Priority Order:**
1. `.ai/customizations/` (highest - CMG-specific)
2. `.ai/bioloop/` (platform core)
3. Repository code
4. Chat history (lowest)

---

## Structure

### Root Level

- **`PRODUCTION_ENVIRONMENT.md`** - Critical production warnings and restrictions
- **`AI_PROTOCOL.md`** - How AI agents should work with this repository
- **`README.md`** - This file

### `/bioloop/` - Platform Core

**Overview:**
- `README.md` - Platform features and conventions overview
- `architecture.md` - Microservice architecture, workflows, database

**Conventions:**
- `api_conventions.md` - API development patterns
- `ui_conventions.md` - UI/Vue development patterns
- `worker_conventions.md` - Worker/Celery patterns
- `database_patterns.md` - Prisma and database patterns
- `e2e_testing_conventions.md` - Playwright e2e testing patterns
- `pitfalls.md` - Common platform mistakes
- `e2e_testing_pitfalls.md` - Common testing mistakes

**Features:**
- `features/datasets.md` - Dataset management
- `features/workflows.md` - Workflow execution
- `features/users-projects.md` - Users and projects
- `features/uploads.md` - File uploads (TUS)
- `features/imports-downloads.md` - Imports and downloads

### `/customizations/` - CMG-Specific

**Overview:**
- `README.md` - CMG customizations overview

**Conventions:**
- `api_conventions.md` - CMG API patterns
- `ui_conventions.md` - CMG UI patterns
- `genome_browser_notes.md` - Genome browser implementation
- `pitfalls.md` - CMG-specific mistakes

**Features:**
- `features/sessions-tracks.md` - Genome browser sessions and tracks
- `features/conversions.md` - Genomic conversions
- `features/cmg-database-migration.md` - MongoDB to PostgreSQL migration

---

## Lazy Loading Protocol

To optimize token usage, AI agents should:

1. **Always load** (on session start):
   - `PRODUCTION_ENVIRONMENT.md`
   - `AI_PROTOCOL.md`
   - `bioloop/README.md`
   - `bioloop/architecture.md`
   - `customizations/README.md`

2. **Load on-demand** (when working on specific features):
   - Feature documentation from `features/`
   - Relevant conventions based on work type
   - Pitfalls documentation

This approach:
- Reduces initial context from ~70k to ~20k tokens
- Leaves more room for code and conversation
- Provides complete documentation when needed

---

## Feature-Scoped Work Model

Development is organized **by feature**:
- Each feature has a dedicated changelog in `features/<feature>.md`
- Changelogs are the authoritative source of truth
- Chat history is exploratory context, not authority

**Protocol:**
1. Identify active feature
2. Read feature changelog
3. Work according to documented decisions
4. Update changelog when decisions are made

See `AI_PROTOCOL.md` for details.

---

## Documentation Maintenance

### When to Update

**REQUIRED:**
- Update feature changelogs when design decisions are made
- Update conventions when patterns change
- Update architecture docs when structure changes

**ALLOWED:**
- Update existing user-facing documentation
- Update existing technical documentation when code changes

**FORBIDDEN (unless explicitly requested):**
- Creating refactor summary files
- Creating "what changed" files
- Creating migration guide files (separate from feature changelogs)

### Documentation Style

- Use markdown format
- Include clear section headers
- Provide code examples
- Use emojis for visual clarity (documentation only, never in code)
- Include "Last Updated" date at bottom

---

## Quick Reference

### Starting New Work

1. Read core files (if new session)
2. Identify feature: Ask user or infer from context
3. Read `.ai/features/<feature>.md`
4. Ask what type of work (API/UI/Worker/Database/E2E/General)
5. Load relevant conventions
6. Begin work

### During Work

- Treat changelogs as current mental model
- Don't re-decide documented decisions
- Keep reasoning consistent with constraints
- Ask for clarification on conflicts

### After Work

Update changelog if:
- Design decision made
- Behavior clarified
- Constraints introduced/removed
- Architecture changed
- Assumption confirmed/rejected

---

## Related Files

- **`.cursorrules`** - Cursor IDE rules (references this directory)
- **`tests/README.md`** - Comprehensive testing setup guide
- **`docs/`** - User-facing documentation

---

**Last Updated:** 2026-02-06
