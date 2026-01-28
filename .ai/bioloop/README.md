# Bioloop Platform Core Documentation

**Purpose:** Documentation for features and patterns shared across all Bioloop platform instances.

This directory contains the **base platform documentation** that applies to:
- The original Bioloop platform
- CMG-Bioloop (this repo)
- Any other Bioloop forks

---

## Core Platform Features

### 1. Datasets
**File:** `features/datasets.md`
- Raw data vs data products
- Dataset types and lifecycle
- Staging and archival

### 2. Workflows
**File:** `features/workflows.md`
- Python-based workflow framework
- "Integrated" workflows
- Workflow execution and monitoring

### 3. Users & Projects
**File:** `features/users-projects.md`
- User management and authentication
- Role-based access control
- Project organization and ACLs

### 4. Imports & Downloads
**File:** `features/imports-downloads.md`
- File upload (Import feature)
- Secure download mechanisms
- File validation and processing

---

## Platform Conventions

### architecture.md
Microservice architecture overview:
- Service components (UI, API, Workers)
- Database architecture
- Cross-service communication
- **Workflow architecture (Rhythm integration)**
- Environment-specific behavior

### api_conventions.md
API development patterns:
- Import organization
- Prisma usage
- Transactions
- Route handlers
- **Workflow creation pattern (CRITICAL)**
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
- **Workflow task pattern and argument flow**
- Logging

### database_patterns.md
Database and Prisma patterns:
- Schema conventions
- Cascade deletes
- JSON fields
- Shared includes

### pitfalls.md
Common mistakes in platform code:
- Database anti-patterns
- Authentication errors
- UI component mistakes

---

## Customization Override Pattern

Files in `/customizations/` can override or extend platform conventions:

- `/bioloop/api_conventions.md` - Base API patterns
- `/customizations/api_conventions.md` - CMG-specific additions/overrides

When conflicts exist, customizations take precedence.

---

**Last Updated:** 2026-01-27

