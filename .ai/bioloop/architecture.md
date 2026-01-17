# Bioloop Platform Architecture

## Overview

Bioloop is a **microservice architecture** with separate UI, API, and Worker components.

**Always consider the impact of changes across all services.**

---

## Service Components

### UI (Vue.js)
- **Technology:** Vue 3 with Vuestic UI component library
- **Port:** 3000 (development), proxied via Nginx in production
- **Responsibilities:**
  - User interface and interactions
  - Client-side validation
  - API consumption

### API (Node.js/Express)
- **Technology:** Express.js with Prisma ORM
- **Port:** 3001 (development), 3000 (production via Nginx)
- **Responsibilities:**
  - RESTful API endpoints
  - Authentication & authorization
  - Database operations
  - File serving and exposure
  - Job submission to workers

### Workers (Python/Celery)
- **Technology:** Python with Celery task queue
- **Queue:** Redis (broker and result backend)
- **Responsibilities:**
  - Asynchronous task processing
  - Long-running computations
  - SLURM job submission
  - Workflow execution

---

## Database Architecture

### PostgreSQL
- **Purpose:** Primary data store
- **ORM:** Prisma (schema in `api/prisma/schema.prisma`)
- **Naming Convention:** `snake_case` for all models and fields

### Redis
- **Purpose:** Celery broker and result backend
- **Port:** 6379

### MongoDB
- **Purpose:** Task metadata and worker state (legacy)
- **Status:** Being phased out in favor of PostgreSQL

---

## Cross-Service Communication

### API → Workers
- Celery tasks submitted via Redis queue
- Task results stored in Redis backend
- API polls for task completion

### UI → API
- RESTful HTTP requests
- JWT-based authentication

### Workers → API
- Task status updates via database
- No direct HTTP calls from workers to API

---

## Key Architectural Patterns

### Authentication Flow
1. User logs in via UI
2. API validates credentials and issues JWT
3. JWT stored in httpOnly cookie
4. API middleware validates JWT on each request

### File Serving Pattern
1. Files stored on filesystem (not in database)
2. Database stores file metadata and paths
3. API serves files via `/files/expose/` routes

### Data Consistency
- Use Prisma transactions for multi-operation updates
- Cascade deletes configured at database level
- Optimistic locking for concurrent updates where needed

---

## Environment-Specific Behavior

### Development
- Hot module replacement (HMR) enabled
- Detailed logging
- CORS permissive
- Direct service access (no Nginx)

### Production
- Nginx reverse proxy
- Compressed responses (except binary files)
- Secure cookies (HTTPS only)
- Rate limiting
- Access logs

---

## Critical Cross-Service Dependencies

1. **Prisma Schema Changes**
   - Requires: `npx prisma generate` in API
   - Requires: API restart
   - May require: Worker restart if workers use Prisma models

2. **Constants Changes**
   - Update in: `api/src/constants.js`, `ui/src/constants.js`, `workers/config/common.py`
   - Requires: Service restart only if config files changed

3. **Database Migrations**
   - Run in: API service
   - Affects: All services that access database
   - Coordination: Must be run before deploying code changes

---

## Service Restart Requirements

### Requires Restart
- Config file changes (`config/*.json`)
- New npm/pip package installed
- Prisma schema changes (after `npx prisma generate`)
- Environment variable changes
- Docker container rebuild

### Does NOT Require Restart
- API route changes (HMR)
- UI component changes (HMR)
- Service function changes (HMR)
- Database data changes (only structure requires restart)

---

**Last Updated:** 2026-01-16

