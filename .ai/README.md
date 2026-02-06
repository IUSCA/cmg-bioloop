# .ai/ Directory

**Purpose:** Repo-resident AI knowledge base for cross-workspace and cross-machine continuity.

This directory contains **authoritative documentation** that persists across Cursor sessions and machines. It serves as the **single source of truth** for AI agents working on this project.

---

## Directory Structure

```
.ai/
├── README.md                      # This file
├── AI_PROTOCOL.md                 # How AI agents should work with this repo
├── PRODUCTION_ENVIRONMENT.md      # ⚠️ Production warnings & restrictions
│
├── bioloop/                       # Platform core (shared across all Bioloop instances)
│   ├── README.md
│   ├── architecture.md
│   ├── api_conventions.md
│   ├── ui_conventions.md
│   ├── worker_conventions.md
│   ├── database_patterns.md
│   ├── pitfalls.md
│   └── features/
│       ├── datasets.md            # raw_data, data_product
│       ├── workflows.md           # integrated workflows
│       ├── users-projects.md      # user management, projects
│       ├── uploads.md             # TUS browser uploads
│       └── imports-downloads.md   # external imports, downloads
│
└── customizations/                # CMG-specific (this fork only)
    ├── README.md
    ├── api_conventions.md         # CMG-specific API additions
    ├── ui_conventions.md          # CMG-specific UI patterns
    ├── pitfalls.md                # CMG-specific pitfalls
    ├── genome_browser_notes.md    # Browser implementation
    └── features/
        ├── sessions-tracks.md     # Genome browser sessions & tracks
        ├── conversions.md         # Genomic conversion pipelines
        └── cmg-database-migration.md  # CMG → Bioloop migration
```

---

## Organization Principle

### Platform Core (`/bioloop/`)
Documentation for features and patterns **shared across all Bioloop platform instances**:
- Original Bioloop platform
- CMG-Bioloop (this repo)
- Any other Bioloop forks

### Customizations (`/customizations/`)
Documentation for features **specific to the CMG-Bioloop fork** that are not present in the base platform.

---

## Priority Order of Truth

When conflicts exist, resolve them in this order:

1. `.ai/customizations/features/<feature>.md` (highest - CMG-specific)
2. `.ai/customizations/*.md` (CMG conventions)
3. `.ai/bioloop/features/<feature>.md` (platform features)
4. `.ai/bioloop/*.md` (platform conventions)
5. Repository code
6. Chat history (lowest)

**Customizations can override or extend platform defaults.**

---

## Core Files

### PRODUCTION_ENVIRONMENT.md
**⚠️ CRITICAL - Read first.** Production warnings and restrictions:
- Forbidden paths and operations (NEVER touch `/N/...`)
- Access restrictions (allowed paths, hosts, commands)
- Docker and database operation policies
- Logging and monitoring conventions

### AI_PROTOCOL.md
**Read this second.** Defines how AI agents should:
- Work with feature changelogs
- Handle pre-work and post-work requirements
- Resolve conflicts between chat and documentation
- Use Git for synchronization

---

## Platform Core Documentation (`/bioloop/`)

### architecture.md
System architecture overview:
- Service components (UI, API, Workers)
- Database architecture
- Cross-service communication
- Environment-specific behavior

### api_conventions.md
API development patterns:
- Import organization
- Prisma usage
- Transactions
- Route handlers
- Error handling

### ui_conventions.md
UI development patterns:
- Vuestic component usage
- Constants patterns
- Form validation
- Modal patterns

### worker_conventions.md
Worker development patterns:
- Configuration
- Task definitions
- Logging

### database_patterns.md
Database and Prisma patterns:
- Schema conventions
- Cascade deletes
- JSON fields
- Shared includes

### pitfalls.md
Common platform mistakes:
- Database anti-patterns
- Authentication errors
- UI component mistakes

### Platform Features (`/bioloop/features/`)
1. **datasets.md** - Raw data vs data products, staging, archival
2. **workflows.md** - Python workflow framework, "integrated" workflows
3. **users-projects.md** - User management, roles, project ACLs
4. **uploads.md** - TUS browser uploads, resumable, BLAKE3 checksums
5. **imports-downloads.md** - External imports (SDA), secure downloads

---

## CMG Customizations (`/customizations/`)

### api_conventions.md
CMG-specific API additions:
- File exposure routing for genome browsers
- Cookie-based file authentication
- Range request support
- Compression handling

### ui_conventions.md
CMG-specific UI patterns:
- React-in-Vue integration (WashU browser)
- Genome-specific components
- Browser selection patterns

### genome_browser_notes.md
Genome browser implementation:
- IGV integration
- WashU integration
- File serving patterns
- Generic vs browser-specific naming

### pitfalls.md
CMG-specific mistakes:
- Genome browser file serving errors
- React unmounting issues
- Compression problems

### CMG Features (`/customizations/features/`)
1. **sessions-tracks.md** - Genome browser sessions and tracks
2. **conversions.md** - Genomic conversion pipelines
3. **cmg-database-migration.md** - MongoDB → PostgreSQL migration

---

## How AI Agents Should Use This Directory

### Pre-Work (MANDATORY)
Before answering any development prompt:
1. Read platform core docs (bioloop/)
2. Read customization docs (customizations/)
3. Identify the active feature
4. Read feature changelog
5. Assume changelogs override chat memory

### During Work
- Treat changelogs as current mental model
- Don't re-decide documented items
- Keep reasoning consistent with constraints
- Apply platform conventions + customizations

### Post-Work (MANDATORY)
If any design decision, constraint, or clarification occurs:
1. Append factual entry to feature changelog
2. Use decision-style language (not discussion)
3. Don't summarize chat—summarize outcome

See `AI_PROTOCOL.md` for full details.

---

## Override Pattern

Files in `/customizations/` can override or extend `/bioloop/` conventions:

**Example:**
- `/bioloop/api_conventions.md` - Base API patterns (all Bioloop platforms)
- `/customizations/api_conventions.md` - CMG-specific additions (file exposure, cookies, range requests)

When conflicts exist, **customizations take precedence**.

---

## Maintenance

- Keep changelogs factual and concise
- Update after every significant decision
- Use Git to sync changes across machines
- Never contradict documented decisions unless explicitly instructed
- Platform core docs should be applicable to all Bioloop forks
- Customization docs should be CMG-specific only

---

**Last Updated:** 2026-01-16
