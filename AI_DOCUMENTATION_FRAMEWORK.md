# AI-Agent Documentation Framework

**Version:** 1.0  
**Last Updated:** 2026-02-03

---

## Purpose

This document provides a **generic framework** for any software project to create AI-agent documentation that enables:
- **Cross-workspace continuity** - AI agents maintain context across chat sessions
- **Cross-machine consistency** - Documentation persists in git, not just local chat history
- **Systematic onboarding** - New AI agents in new chats start with essential context
- **Explicit memory artifacts** - Documentation overrides chat memory when conflicts arise
- **Feature-scoped work** - Organized by feature with dedicated changelogs

**This framework is technology-agnostic and can be adapted to any programming language, architecture, or domain.**

---

## Core Principles

### 1. Repository-Resident Truth

**Principle:** Chat history is ephemeral. Documentation in the repository is authoritative.

**Implementation:**
- All architectural decisions, conventions, and feature specifications live in versioned documentation
- Chat is for exploration and discussion
- Documentation is for decisions and persistence
- When chat and documentation conflict, documentation wins

**Why:** Chat history doesn't sync across machines or persist beyond workspace sessions. Repository files do.

---

### 2. Lazy Loading Initialization

**Principle:** AI agents should read core context at the start of each new chat, but only load what's immediately necessary.

**Implementation:**
- **Step 1:** Auto-read 5-7 core files (~15-25k tokens) that provide essential context
- **Step 2:** Report what was loaded and confirm readiness
- **Step 3:** Load feature-specific documentation on-demand when user specifies their task
- **Step 4:** Load convention files on-demand based on the type of work (API/UI/Database/etc.)

**Why:** Reduces initial token usage from ~70k to ~20k, leaving more room for code context and conversation. Avoids "lost in the middle" effect.

---

### 3. Feature-Scoped Work Model

**Principle:** Development work is organized by feature, not by chat session.

**Implementation:**
- Each feature has a dedicated changelog file
- One feature per chat/conversation (human-enforced guideline)
- Clear feature scope definition
- Feature changelogs track decisions, not discussions

**Why:** Enables multiple developers (or AI agents) to work on different features concurrently without conflicts.

---

### 4. Priority Order of Truth

**Principle:** When conflicts arise, resolve them according to a clear hierarchy.

**Implementation (example for projects with customizations):**
1. Customizations/extensions for specific feature (highest priority)
2. Customizations/extensions conventions
3. Core platform/base feature documentation
4. Core platform/base conventions
5. Repository code
6. Chat history (lowest priority)

**Why:** Eliminates ambiguity about which documentation applies in edge cases.

---

### 5. Pre-Work and Post-Work Requirements

**Principle:** AI agents must follow a consistent workflow before and after any development task.

**Pre-Work (MANDATORY before answering any development prompt):**
1. Identify the active feature
2. Locate and read the feature's changelog
3. Assume changelog overrides chat memory
4. If changelog is missing/unclear, STOP and ask for clarification

**Post-Work (MANDATORY after any design decision or clarification):**
1. Append factual entry to feature changelog
2. Use decision-style language (not discussion summaries)
3. Don't summarize chat—summarize the outcome

**Why:** Prevents re-deciding documented items and ensures decisions persist across sessions.

---

## Directory Structure

### Recommended Layout

```
.ai/                                    # Root AI knowledge base directory
├── README.md                           # Directory overview and usage guide
├── AI_PROTOCOL.md                      # How AI agents should work with this repo
├── PRODUCTION_ENVIRONMENT.md           # ⚠️ Production warnings (if applicable)
│
├── <core>/                             # Core/base platform documentation
│   ├── README.md                       # Core overview
│   ├── architecture.md                 # System architecture
│   ├── <component>_conventions.md      # Component-specific patterns
│   ├── database_patterns.md            # Database conventions (if applicable)
│   ├── pitfalls.md                     # Common mistakes
│   └── features/                       # Core feature documentation
│       ├── <feature1>.md
│       ├── <feature2>.md
│       └── ...
│
└── <customizations>/                   # Extensions/customizations (optional)
    ├── README.md                       # Customizations overview
    ├── <component>_conventions.md      # Extended conventions
    ├── pitfalls.md                     # Customization-specific mistakes
    └── features/                       # Extended feature documentation
        ├── <custom_feature1>.md
        ├── <custom_feature2>.md
        └── ...
```

**Notes:**
- `<core>` can be your project name (e.g., `myapp`, `platform`, `base`)
- `<customizations>` is optional (only if you have extensions/forks)
- `<component>` might be: api, ui, backend, frontend, worker, service, etc.

---

## Core Documentation Files

### 1. `.ai/README.md`

**Purpose:** Entry point for understanding the documentation structure.

**Contents:**
- Directory structure overview
- Organization principle (core vs customizations)
- Priority order of truth
- How AI agents should use this directory
- Quick reference to core files

**Template:**
```markdown
# .ai/ Directory

**Purpose:** Repo-resident AI knowledge base for cross-workspace continuity.

## Directory Structure
[Tree diagram]

## Priority Order of Truth
1. [Most specific documentation]
2. [More general documentation]
3. Repository code
4. Chat history (lowest)

## How AI Agents Should Use This Directory
[Pre-work, during work, post-work sections]
```

---

### 2. `.ai/AI_PROTOCOL.md`

**Purpose:** Defines the operating protocol for AI agents.

**Contents:**
- Feature-scoped work model
- Required pre-work (read changelog before answering)
- Required post-work (update changelog after decisions)
- Changelog entry format and style
- Git synchronization awareness
- Failure mode safeguards
- Documentation policy (what to document, what not to)

**Key Sections:**
- **Required Pre-Work (MANDATORY)**
- **During Work**
- **Required Post-Work (MANDATORY)**
- **Changelog Entry Style** (decision-style, not discussion)
- **Priority Order of Truth**
- **Git Synchronization Awareness**
- **Failure Mode Safeguards** (STOP if ambiguous)

---

### 3. `.ai/PRODUCTION_ENVIRONMENT.md` (if applicable)

**Purpose:** Critical production warnings and restrictions.

**Contents:**
- **NEVER Do These Things** section (most critical restrictions)
- Access restrictions (allowed paths, forbidden paths)
- Allowlisted commands (read-only vs write operations)
- Host restrictions
- Database operation policies
- Docker/container policies
- Deployment and restart patterns
- Emergency procedures

**Key Principles:**
- Use ❌ and ⚠️ symbols for clarity
- Make restrictions absolutely explicit
- Provide rationale for each restriction
- Include escape clause: "unless user explicitly permits"

---

### 4. `.ai/<core>/architecture.md`

**Purpose:** High-level system architecture overview.

**Contents:**
- System components and their responsibilities
- Database architecture
- Cross-component communication patterns
- Environment-specific behavior (dev vs production)
- Key architectural patterns
- Critical dependencies

**Adaptation Examples:**
- **Microservices:** Document service components, message queues, API contracts
- **Monolith:** Document modules, layers, internal APIs
- **Mobile App:** Document app architecture, data sync, offline-first patterns
- **Data Pipeline:** Document ingestion, transformation, storage layers

---

### 5. `.ai/<core>/<component>_conventions.md`

**Purpose:** Component-specific development patterns and best practices.

**Contents:**
- Code organization patterns
- Import/dependency management
- Common patterns for this component
- Error handling conventions
- Logging patterns
- Testing conventions (if applicable)
- Quick reference checklist

**Examples by Component:**
- **API:** Import organization, route handlers, transaction patterns, authentication
- **UI:** Component structure, state management, styling conventions, form validation
- **Database:** Schema conventions, query patterns, transaction usage, migrations
- **Workers:** Task definitions, logging, retry patterns, queue management

**Pattern:**
```markdown
## Pattern Name

### Standard Implementation
[Code example with ✅ CORRECT]

### Common Mistakes
[Code example with ❌ WRONG]

### Why This Pattern
[Rationale]
```

---

### 6. `.ai/<core>/database_patterns.md` (if applicable)

**Purpose:** Database schema conventions and query patterns.

**Contents:**
- Schema naming conventions
- Relationship patterns (one-to-many, many-to-many)
- Cascade delete policies
- JSON field usage (if applicable)
- Query optimization patterns
- Transaction boundaries
- Shared query fragments (as constants)

---

### 7. `.ai/<core>/pitfalls.md`

**Purpose:** Catalog of common mistakes and anti-patterns.

**Contents:**
- Numbered list of common mistakes
- Each entry shows: ❌ WRONG pattern, then ✅ CORRECT pattern
- Organized by category (Database, API, UI, etc.)
- Brief explanation of why it's a pitfall

**Template:**
```markdown
## Component Category

1. **❌ [Mistake description]**
   - Always use: [correct pattern]
   - Never use: [wrong pattern]

2. **❌ [Another mistake]**
   - [Explanation and correct approach]
```

---

### 8. `.ai/<core>/features/<feature>.md`

**Purpose:** Feature-specific documentation and changelog.

**Structure:**
```markdown
# Feature Name

**Feature Scope:** [Brief description]
**Status:** [Implemented/In Progress/Planned]
**Related Documentation:** [Links to related docs]

---

## Overview
[High-level feature description]

---

## Architecture/Design Decisions
[Initial architecture, key decisions, constraints]

---

## Database Schema (if applicable)
[Relevant models and relationships]

---

## API Endpoints (if applicable)
[Key endpoints and their purposes]

---

## Key Patterns
[Implementation patterns specific to this feature]

---

## Changelog

### YYYY-MM-DD

- Decision: [What was decided]
- Constraint: [New constraint or limitation]
- Clarification: [Behavior clarification]
- Change: [What changed and why]

### YYYY-MM-DD

[Additional entries as work progresses]

---

**Last Updated:** YYYY-MM-DD
```

**Changelog Entry Style:**
- Use **decision-style language**, not discussion summaries
- Date each section (YYYY-MM-DD format)
- Use bullet points starting with: Decision, Constraint, Clarification, Change, etc.
- Be factual and concise
- **FORBIDDEN:** "We talked about...", "It seems like...", "Maybe later..."

---

## Customizations/Extensions (Optional)

### When to Use Customizations Directory

Use a separate customizations directory when:
- Your project is a fork of a base platform
- You have extensions that other deployments won't use
- You want to maintain clear separation between base and custom code
- You might sync updates from upstream base platform

### Structure

```
.ai/
├── <core>/                    # Base platform (shared across all instances)
└── <customizations>/          # Project-specific extensions
    ├── <component>_conventions.md    # Extends core conventions
    ├── features/                     # Custom features only
    └── pitfalls.md                   # Custom pitfalls
```

### Override Pattern

**Principle:** Customizations can extend or override core conventions.

**Implementation:**
- Customization files explicitly state: `**Extends:** /<core>/<component>_conventions.md`
- When reading conventions, AI agent reads BOTH core and customizations
- Priority order makes customizations win in conflicts
- Document only the delta (what's different or additional)

---

## Agent Initialization Protocol

### Step-by-Step Initialization

**When user says "hi", "new task", or similar:**

#### Step 1: Auto-Read Core Files (~15-25k tokens)

Read these 5-7 files immediately:
1. `.ai/PRODUCTION_ENVIRONMENT.md` (if applicable) - Critical warnings
2. `.ai/AI_PROTOCOL.md` - How to work with this repo
3. `.ai/<core>/README.md` - Core overview
4. `.ai/<core>/architecture.md` - System architecture
5. `.ai/<customizations>/README.md` (if applicable) - Customizations overview
6. Optional: Other critical context files

**DO NOT auto-read:** Conventions, pitfalls, or feature docs (load on-demand)

#### Step 2: Report & Confirm

Respond with:
```
✅ Core initialization complete (~20k tokens loaded)

📚 I've read:
- [List of files read]

⚠️ [Critical restrictions summary if applicable]

📂 Available features (load on-demand):
[List of features organized by category]

🎯 What would you like to work on?
```

#### Step 3: Load Feature Context (On-Demand)

When user specifies a feature:
- Read `.ai/<core>/features/<feature>.md` or `.ai/<customizations>/features/<feature>.md`
- Confirm: "✅ Loaded <feature> documentation"
- Ask: "What type of work? (API/UI/Database/etc.)"

#### Step 4: Load Conventions (On-Demand)

Based on the type of work:
- **API work:** Read both `api_conventions.md` and `pitfalls.md` (core + customizations if applicable)
- **UI work:** Read both `ui_conventions.md` and `pitfalls.md`
- **Database work:** Read `database_patterns.md` and `pitfalls.md`
- **General work:** Load conventions as needed when editing specific files

---

## Cursor Rules Integration

### `.cursorrules` File

The `.cursorrules` file in the repository root should:
1. **Reference the AI protocol** - Point to `.ai/AI_PROTOCOL.md`
2. **Implement initialization protocol** - Auto-read core files on "hi"
3. **Enforce lazy loading** - Load features/conventions on-demand
4. **Define production restrictions** - Reference `.ai/PRODUCTION_ENVIRONMENT.md`
5. **Set documentation policy** - What to document, what not to
6. **Provide quick reference** - Key patterns and reminders

**Template sections:**
- Agent Initialization Protocol (with lazy loading steps)
- Production Environment Warnings (if applicable)
- Documentation & Conventions (reference to `.ai/` directory)
- Priority Order of Truth
- Documentation Policy
- Sensitive File Restrictions (if applicable)
- Allowlisted Commands (if applicable)
- Development Workflow (restart patterns, HMR, etc.)
- Key Architectural Reminders

---

## Feature Changelog Best Practices

### Entry Format

```markdown
## YYYY-MM-DD

- Decision: [What was decided, not why it was discussed]
- Constraint: [New constraint or limitation introduced]
- Clarification: [Behavior clarified or ambiguity resolved]
- Change: [What changed from previous state]
- Rationale: [Why this decision was made, if not obvious]
```

### Writing Style

**✅ CORRECT - Decision-style:**
```markdown
## 2026-02-03

- Decision: User authentication uses JWT tokens, not sessions
- Constraint: Tokens expire after 1 hour, no refresh mechanism
- Clarification: Password reset flow requires email verification
```

**❌ WRONG - Discussion summaries:**
```markdown
## 2026-02-03

- We talked about authentication approaches
- It seems like JWT might work better
- Maybe we should implement refresh tokens later
```

### When to Update

Update the feature changelog when:
- A design decision is made
- Behavior is clarified or an assumption is confirmed
- Constraints are introduced or removed
- Architecture or data flow changes
- A conflict between documentation and code is resolved

### When NOT to Update

**DO NOT create these files:**
- ❌ Refactor summary files (e.g., `*_REFACTOR.md`)
- ❌ "What changed" summary files
- ❌ Migration guide files (separate from feature changelogs)
- ❌ Change documentation files
- ❌ Implementation summary files

**Why:** This information belongs in:
1. Feature changelogs - for design decisions
2. Existing usage documentation - for user guidance
3. The code itself - implementation details are self-documenting
4. Git history - for change tracking

---

## Code Style Guidelines

### Emoji Usage Policy

**NEVER use emojis in:**
- ❌ Code comments
- ❌ Logging statements
- ❌ Error messages
- ❌ Variable names, function names, or code identifiers
- ❌ Commit messages (project preference)

**Emojis ALLOWED in documentation only:**
- ✅ Markdown documentation files (`.md`)
- ✅ Strictly documentational emojis (✅ ❌ ⚠️ 🎯 📖)

**Exception:** May use in complex debugging scripts when visual markers significantly aid debugging (rare, must be justified).

**Rationale:**
- Code should be text-based and emoji-free for compatibility
- Logs should be grep-friendly and parseable
- Documentation can use emojis for visual clarity

---

## Documentation Maintenance

### Version Control

**All `.ai/` documentation should be:**
- Committed to git
- Included in pull requests
- Reviewed like code
- Updated when decisions change

### Last Updated Date

**Every documentation file should have:**
```markdown
**Last Updated:** YYYY-MM-DD
```
at the bottom of the file.

### Git Synchronization Awareness

**AI agents should:**
- Check if feature changelogs have changed upstream (use `git status`, `git log`)
- Never assume changelogs are up to date unless verified
- Prefer using git to detect if documentation has changed
- Alert user if local and remote changelogs differ

---

## Production Environment Handling

### Critical Restrictions

If your project runs in production, document:
1. **NEVER operations** - Operations that should never be performed
2. **Forbidden paths** - Directories that must not be modified
3. **Allowlisted commands** - Read-only vs write operations
4. **Host restrictions** - Where changes can be made
5. **Escape clause** - "unless user explicitly permits in chat"

### Allowlisted Commands Pattern

**Read-Only Commands:**
- System info: `hostname`, `whoami`, `df -h`
- File inspection: `ls`, `cat`, `grep`, `find`
- Git: `git status`, `git log`, `git diff` (no write operations)
- Process: `ps`, `top` (no kill operations)

**Forbidden Commands:**
- Write operations: `rm`, `mv`, `chmod` (in production paths)
- Service control: `systemctl restart`, `docker restart`
- Database: Direct DB access without permission
- Sudo: `sudo` commands

### Temporary Files Policy

**If temporary files are allowed:**
- Specify allowed paths (e.g., `/tmp`)
- **ALWAYS** require cleanup after work is done
- No persistent storage in temporary locations

---

## Adapting This Framework

### For Different Project Types

#### Web Application
- Core components: `api_conventions.md`, `ui_conventions.md`, `database_patterns.md`
- Features: authentication, users, data management, integrations
- Architecture: Frontend, backend, database, authentication flow

#### Data Pipeline
- Core components: `ingestion_conventions.md`, `transformation_conventions.md`, `storage_conventions.md`
- Features: data sources, transformations, outputs, monitoring
- Architecture: Ingestion, processing, storage, orchestration

#### Mobile Application
- Core components: `mobile_conventions.md`, `api_conventions.md`, `data_sync_patterns.md`
- Features: offline-first, sync, notifications, authentication
- Architecture: App layers, data sync, backend communication, offline storage

#### CLI Tool
- Core components: `cli_conventions.md`, `command_patterns.md`
- Features: commands, config management, output formatting
- Architecture: Command structure, plugin system, configuration

### For Different Tech Stacks

**The framework is technology-agnostic.** Adapt by:
1. Renaming `<core>` to your project name
2. Renaming component files to match your architecture (api → backend, ui → frontend, etc.)
3. Adjusting conventions to your languages/frameworks
4. Keeping the same structure: core docs, features, conventions, pitfalls
5. Maintaining the same agent protocol: pre-work, post-work, changelogs

---

## Benefits of This Framework

### For AI Agents
- **Consistent context** across all chat sessions
- **Clear operating protocol** - no ambiguity about what to do
- **Explicit memory** - documentation overrides chat hallucinations
- **Efficient token usage** - lazy loading saves tokens for code context

### For Developers
- **Cross-machine consistency** - documentation syncs via git
- **Explicit decisions** - feature changelogs capture "why"
- **Onboarding efficiency** - new developers (or AI agents) get context fast
- **Reduced rework** - avoid re-deciding documented items

### For Projects
- **Knowledge preservation** - decisions persist beyond chat sessions
- **Multi-agent collaboration** - multiple AI agents work on different features
- **Fork-friendly** - clear separation of core vs customizations
- **Scalable** - structure grows with project complexity

---

## Anti-Patterns to Avoid

### ❌ Don't: Create Documentation Outside `.ai/`

**Wrong:**
```
/docs/ai-context.md
/AI_NOTES.md (at root)
/architecture/ai-guide.md
```

**Right:**
```
/.ai/architecture.md
/.ai/features/feature-name.md
```

**Why:** Centralized location makes it easy for AI agents to find all context.

---

### ❌ Don't: Put Implementation Details in Changelogs

**Wrong:**
```markdown
## 2026-02-03
- Implemented login endpoint with 247 lines of code
- Added password hashing with bcrypt
- Created JWT token generation function
```

**Right:**
```markdown
## 2026-02-03
- Decision: Authentication uses JWT tokens with 1-hour expiration
- Constraint: Password must be at least 8 characters
- Clarification: Tokens are signed with HMAC-SHA256
```

**Why:** Changelogs document decisions, not implementation steps. Code is self-documenting for implementation.

---

### ❌ Don't: Make All Documentation Mandatory Reading

**Wrong:** Require AI agents to read all 70k tokens of documentation at the start of every chat.

**Right:** Lazy loading protocol - core files first (~20k), then features/conventions on-demand.

**Why:** Saves tokens for code context and conversation. Reduces "lost in the middle" effect.

---

### ❌ Don't: Create Separate Documentation for Each Fork

**Wrong:** Every deployment has completely separate documentation.

**Right:** Separate core platform docs from customizations, with clear override pattern.

**Why:** Enables sharing improvements to core platform across all deployments.

---

### ❌ Don't: Let Chat Memory Override Documentation

**Wrong:** AI agent remembers something from earlier in chat that contradicts documentation, uses chat memory.

**Right:** AI agent follows priority order of truth - documentation overrides chat.

**Why:** Chat context can be wrong or misremembered. Documentation is reviewed and versioned.

---

## Quick Start Checklist

### Setting Up .ai/ Directory for Your Project

- [ ] Create `.ai/` directory in repository root
- [ ] Create `.ai/README.md` with directory overview
- [ ] Create `.ai/AI_PROTOCOL.md` with agent operating protocol
- [ ] Create `.ai/PRODUCTION_ENVIRONMENT.md` (if applicable)
- [ ] Create `.ai/<your-project>/` directory for core documentation
- [ ] Create `.ai/<your-project>/architecture.md`
- [ ] Create `.ai/<your-project>/<component>_conventions.md` for each component
- [ ] Create `.ai/<your-project>/pitfalls.md`
- [ ] Create `.ai/<your-project>/features/` directory
- [ ] Create feature changelog for each major feature
- [ ] Update `.cursorrules` with initialization protocol
- [ ] Test with new AI agent chat session
- [ ] Iterate based on agent behavior

### Testing Your Setup

1. Open new AI agent chat session
2. Say "hi"
3. Verify agent reads core files and reports what was loaded
4. Specify a feature to work on
5. Verify agent loads feature documentation on-demand
6. Make a code change and verify agent updates feature changelog
7. Open another chat and verify agent has same context from docs

---

## Example Projects

### Example 1: E-commerce Platform

```
.ai/
├── README.md
├── AI_PROTOCOL.md
├── PRODUCTION_ENVIRONMENT.md
├── platform/
│   ├── README.md
│   ├── architecture.md
│   ├── api_conventions.md
│   ├── frontend_conventions.md
│   ├── database_patterns.md
│   ├── pitfalls.md
│   └── features/
│       ├── products.md
│       ├── cart.md
│       ├── checkout.md
│       ├── payments.md
│       └── orders.md
```

### Example 2: Data Processing Pipeline

```
.ai/
├── README.md
├── AI_PROTOCOL.md
├── pipeline/
│   ├── README.md
│   ├── architecture.md
│   ├── ingestion_conventions.md
│   ├── transformation_conventions.md
│   ├── storage_conventions.md
│   ├── orchestration_patterns.md
│   ├── pitfalls.md
│   └── features/
│       ├── data-ingestion.md
│       ├── data-transformation.md
│       ├── data-validation.md
│       ├── data-export.md
│       └── monitoring.md
```

### Example 3: Mobile App with Backend

```
.ai/
├── README.md
├── AI_PROTOCOL.md
├── core/
│   ├── README.md
│   ├── architecture.md
│   ├── mobile_conventions.md
│   ├── backend_conventions.md
│   ├── data_sync_patterns.md
│   ├── pitfalls.md
│   └── features/
│       ├── authentication.md
│       ├── offline-sync.md
│       ├── push-notifications.md
│       └── in-app-purchases.md
```

---

## Summary

This framework enables AI agents to:
1. **Start every chat with essential context** (~20k tokens)
2. **Load feature-specific documentation on-demand**
3. **Follow consistent pre-work and post-work protocols**
4. **Maintain feature changelogs as single source of truth**
5. **Resolve conflicts using clear priority order**
6. **Work across multiple machines and workspace sessions**

**Key Success Factors:**
- Lazy loading reduces token waste
- Feature changelogs capture decisions, not discussions
- Production restrictions prevent dangerous operations
- Convention files standardize patterns
- Pitfall files prevent common mistakes
- Git synchronization ensures consistency

**Start small, iterate, and grow the documentation as your project evolves.**

---

**Framework Version:** 1.0  
**Created:** 2026-02-03  
**License:** Public Domain / CC0 (adapt freely for your project)

---

## Further Resources

### Recommended Reading Order for New Projects

1. This framework document (you're here)
2. Example `.ai/README.md` structure
3. Example `.ai/AI_PROTOCOL.md` template
4. Your project's architecture documentation
5. Convention files for your tech stack
6. Feature changelog template

### Community Examples

As this framework is adopted, consider contributing examples of:
- `.ai/` directories for different project types
- `.cursorrules` files with initialization protocols
- Feature changelog examples
- Convention file templates for different tech stacks

---

**Questions? Issues? Improvements?**

This is a living framework. Adapt it to your needs, and consider sharing your improvements with the community.
