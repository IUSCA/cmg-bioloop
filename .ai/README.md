# .ai/ Directory

**Purpose:** Repo-resident AI knowledge base for cross-workspace and cross-machine continuity.

This directory contains **authoritative documentation** that persists across Cursor sessions and machines. It serves as the **single source of truth** for AI agents working on this project.

---

## Directory Structure

```
.ai/
├── README.md                       # This file
├── AI_PROTOCOL.md                  # How AI agents should work with this repo
├── FEATURES.md                     # Index of all features
├── ARCHITECTURE.md                 # System architecture overview
├── CONVENTIONS_API.md              # API development conventions
├── CONVENTIONS_UI.md               # UI development conventions
├── CONVENTIONS_WORKERS.md          # Worker development conventions
├── PRISMA_PATTERNS.md              # Database & Prisma patterns
├── GENOME_BROWSER_NOTES.md         # Genome browser implementation notes
├── PITFALLS.md                     # Common mistakes to avoid
└── features/
    ├── cmg-database-migration.md  # CMG → Bioloop migration changelog
    ├── conversions.md              # Conversions feature changelog
    └── sessions-tracks.md          # Sessions & Tracks feature changelog
```

---

## Core Files

### AI_PROTOCOL.md
**Read this first.** Defines how AI agents should:
- Work with feature changelogs
- Handle pre-work and post-work requirements
- Resolve conflicts between chat and documentation
- Use Git for synchronization

### FEATURES.md
Index of all feature changelogs in `features/` directory with brief descriptions.

### ARCHITECTURE.md
High-level system architecture:
- Service components (UI, API, Workers)
- Database architecture
- Cross-service communication patterns
- Environment-specific behavior

### CONVENTIONS_*.md
Coding conventions and patterns for each service:
- **API**: Import organization, Prisma usage, transactions, route handlers
- **UI**: Vuestic components, constants, auto-population, React-in-Vue
- **Workers**: Config patterns, task definitions, logging

### PRISMA_PATTERNS.md
Database and Prisma ORM patterns:
- Schema conventions
- Cascade deletes
- JSON fields
- Shared includes as constants

### GENOME_BROWSER_NOTES.md
Genome browser-specific implementation notes:
- Generic vs browser-specific naming
- File serving patterns
- Cookie-based authentication
- React-in-Vue integration

### PITFALLS.md
Common mistakes and anti-patterns to avoid. Read before making changes.

---

## Feature Changelogs

Located in `features/` directory. Each feature has a dedicated changelog file.

### Current Features
1. **cmg-database-migration.md**: CMG MongoDB → Bioloop PostgreSQL migration
2. **conversions.md**: Genomic data conversion pipelines
3. **sessions-tracks.md**: Genome browser sessions and tracks

### Changelog Format

Each feature file follows this structure:
```markdown
# Feature Name

**Feature Scope:** Brief description
**Status:** In Progress | Implemented | On Hold
**Related Documentation:** Links to other docs

---

## YYYY-MM-DD

- Decision: [What was decided]
- Constraint: [New constraint or limitation]
- Clarification: [Behavior clarification]
- Change: [What changed and why]
```

---

## How AI Agents Should Use This Directory

### Pre-Work (MANDATORY)
Before answering any development prompt:
1. Identify the active feature
2. Read `.ai/features/<feature>.md`
3. Assume changelog overrides chat memory
4. If unclear, STOP and ask for clarification

### During Work
- Treat changelog as current mental model
- Don't re-decide documented items
- Keep reasoning consistent with constraints

### Post-Work (MANDATORY)
If any design decision, constraint, or clarification occurs:
1. Append factual entry to feature changelog
2. Use decision-style language (not discussion)
3. Don't summarize chat—summarize outcome

See `AI_PROTOCOL.md` for full details.

---

## Priority Order of Truth

When conflicts exist:
1. `.ai/features/<feature>.md` (highest)
2. Other `.ai/` documentation
3. Repository code
4. Chat history (lowest)

**Chats explore. Changelogs decide. Files persist.**

---

## Maintenance

- Keep changelogs factual and concise
- Update after every significant decision
- Use Git to sync changes across machines
- Never contradict documented decisions unless explicitly instructed

---

**Last Updated:** 2026-01-16

