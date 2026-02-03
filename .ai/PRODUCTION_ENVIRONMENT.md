# Production Environment Guide

**⚠️ CRITICAL: THIS IS THE PRODUCTION ENVIRONMENT**

This document consolidates all production-specific warnings, restrictions, and operational guidelines.

---

## 🚨 Critical Restrictions

### NEVER Do These Things

1. **NEVER EVER EVER** write/delete anything in paths beginning with `/N/...`
   - This is the shared research storage
   - Contains hundreds of TB of irreplaceable data
   - No recovery possible if deleted

2. **NEVER** reset the database yourself
   - Only do so if user explicitly permits in chat
   - Database contains production user data
   - Coordinate with team before any DB operations

3. **NEVER** exec into docker containers or restart them
   - Ask user and provide commands to run
   - User will coordinate with operations team
   - Uncoordinated restarts can disrupt active users

4. **NEVER** use sudo
   - You do not have sudo access in production
   - All operations must work without elevated privileges

5. **NEVER** run migrations automatically
   - Only when user explicitly requests
   - Migrations must be tested in dev first
   - Coordinate with team before running

---

## 📂 Access Restrictions

### Allowed Paths

You have access to:
- `/opt/sca/cmg` on host `cmg-new-service1.sca.iu.edu` (current repository)
- `/opt/sca/cmg-bioloop` if it exists on the host (alternate repo path)
- `/tmp` for temporary operations

### Host Restrictions

- You can only make changes on `cmg-new-service1.sca.iu.edu`
  - Use `hostname` command - it's in the allowlist
  - This hostname is explicitly allowed in permissions
- You don't have access to `/opt/sca/cmg` on any other host

### Docker-Specific Paths

**Paths beginning with `/opt/sca/data/...` are ONLY valid in docker environment:**

- These paths are referenced in codebase and environment variables
- They **ONLY** apply when `NODE_ENV='docker'` (API) or `APP_ENV='docker'` (Workers)
- In **production** environment, these paths **DO NOT EXIST** and should not be used
- Docker mounts these paths from the host into containers
- Production uses different data path configurations, and Production does not use containers

**Example:**
```bash
# In docker environment (NODE_ENV='docker')
DATA_PATH=/opt/sca/data/datasets  # ✓ Valid

# In production environment (NODE_ENV='production')
DATA_PATH=/N/slate/... or /N/project/...  # ✓ Valid
# /opt/sca/data/... paths DO NOT APPLY
```

**Note:** This restriction is specific to `/opt/sca/data/...` paths. The repository paths `/opt/sca/cmg` and `/opt/sca/cmg-bioloop` are NOT affected and remain valid in production.

### Temporary Files

- You can write to `/tmp` for temporary operations
- **ALWAYS** delete anything written to `/tmp` after work is done
- No persistent storage in `/tmp`

---

## 🔒 Allowlisted Commands

Only these commands are available (see `.cursorrules` for full list):

**Read-Only:**
- System info: `hostname`, `whoami`, `df -h`, `uptime`
- File inspection: `ls`, `cat`, `grep`, `find`
- Git: `git status`, `git log`, `git diff` (no commits without permission)
- Docker: `docker ps`, `docker logs` (no exec, no restart)

**No Write Operations:**
- No `sudo` commands
- No `docker exec`, `docker restart`
- No database operations without explicit permission
- No file operations outside allowed paths

---

## 🏗️ Production Architecture

### Service Configuration

**Nginx Reverse Proxy:**
- All traffic goes through Nginx
- Compression enabled (except binary files)
- Rate limiting active
- Access logs captured

**Security:**
- HTTPS only (secure cookies)
- JWT authentication
- httpOnly cookies
- CORS configured

**Performance:**
- Response compression (except `/files/expose/`)
- Rate limiting per endpoint
- Connection pooling
- Caching where appropriate

### Ports

- **UI:** Port 3000 (proxied via Nginx)
- **API:** Port 3000 (production), Port 3001 (dev)
- **Redis:** Port 6379
- **PostgreSQL:** Internal only
- **MongoDB:** Internal only (legacy, being phased out)

---

## 🔄 Deployment & Restart Patterns

### Docker Restart Pattern

**⚠️ IMPORTANT: Avoid Restarting Containers When Possible**

**Docker/Localhost Mode Uses Hot Reloading:**
- **API:** Uses `nodemon` - automatically restarts on file changes
- **UI:** Uses Vite HMR - live reloads on file changes
- **Workers:** Use PM2 with watch mode - restart on changes

**DO NOT restart containers for code changes** - nodemon/Vite/PM2 handle it automatically.

**Container Dependencies:**
- Restarting the API container brings down dependent containers
- Always check `docker compose ps` before and after operations
- Prefer waiting for auto-reload over manual restarts

**If you MUST restart containers:**
```bash
docker compose up -d
```

**Reason:** Bringing down one container can affect others; always use `up -d` to restart the full stack.

### When Restarts Are Required

**Must restart:**
- Config file changes (`config/*.json`)
- New npm/pip package installed
- Prisma schema changes (after `npx prisma generate`)
- Environment variable changes
- Docker container rebuild

**No restart needed (HMR):**
- API route changes
- UI component changes
- Service function changes
- Database data changes (structure changes require restart)

---

## 🗄️ Database Operations

### Migration Policy

1. **NEVER** run migrations automatically
2. User must explicitly request: "please run migrations"
3. Migrations should be tested in dev first
4. Coordination required with operations team

### Database Access

- Read operations: OK through API/Prisma
- Write operations: OK through API/Prisma (normal app flow)
- Schema changes: Require explicit permission
- Direct DB access: Not available (use Prisma through API)

### Backup Awareness

- Daily backups run automatically
- Located in `/db/backups/` directory
- Do not delete backup files
- User can restore if needed

---

## 📊 Monitoring & Logging

### Logging Conventions

**Use `logger` in production:**
```javascript
// API/Worker code
const logger = require('@/services/logger');

logger.info('[FEATURE] Operation started');
logger.warn('[FEATURE] Unusual condition detected');
logger.error('[FEATURE] Operation failed', error);
```

**NOT `console.log`:**
```javascript
// ❌ WRONG in production
console.log('Something happened');
```

**Exception:** CLI scripts can use `console.log` for user feedback

### Log Locations

- API logs: `/opt/sca/cmg/api/logs/`
- Worker logs: `/opt/sca/cmg/workers/logs/`
- Nginx logs: `/var/log/nginx/`
- Access: Use `docker logs` command (in allowlist)

---

## 🛡️ Security Considerations

### Cookie Security

Production cookies must be secure:
```javascript
res.cookie('token', jwt, {
  httpOnly: true,
  secure: true,        // HTTPS only (production)
  sameSite: 'lax',
  maxAge: 3600000,
});
```

### Authentication

- JWT-based authentication
- Tokens expire after 1 hour (configurable)
- Cookie-based file access for genome browsers
- Role-based access control (admin, operator, user)

### Data Access

- All data access must go through API
- ACL checks enforced at API layer
- Project membership required for dataset access
- Audit logging for sensitive operations

---

## 🔧 Environment Detection

Check environment in code:

```javascript
// API
const isProduction = process.env.NODE_ENV === 'production';
const isDocker = process.env.NODE_ENV === 'docker';

if (isProduction) {
  // Production-specific behavior
  res.set('Strict-Transport-Security', 'max-age=31536000');
} else {
  // Development-specific behavior
  res.set('Access-Control-Allow-Origin', '*');
}
```

```python
# Workers
import os
app_env = os.getenv('APP_ENV', 'development')

if app_env == 'production':
    # Production-specific paths (e.g., /N/slate/...)
    pass
elif app_env == 'docker':
    # Docker-specific paths (e.g., /opt/sca/data/...)
    pass
```

**Important:** Paths beginning with `/opt/sca/data/...` only apply when `NODE_ENV='docker'` or `APP_ENV='docker'`. See "Docker-Specific Paths" section above.

---

## 📋 Pre-Deployment Checklist

Before making changes in production:

- [ ] Changes tested in development environment
- [ ] Linter errors addressed (if applicable)
- [ ] User explicitly requested the change
- [ ] Impact on active users considered
- [ ] Rollback plan identified
- [ ] Coordination with operations team (if needed)
- [ ] Backup verified (for risky operations)

---

## 🚑 Emergency Contacts

**If something goes wrong:**

1. **Stop immediately** - Don't make it worse
2. **Notify user** - Describe what happened
3. **Check logs** - `docker logs <container>`
4. **Don't panic** - Backups exist
5. **Let operations team handle** - They have recovery procedures

**You cannot:**
- Restart services yourself (user must do it)
- Access database directly (use Prisma through API)
- Recover deleted files from `/N/...` (no recovery possible)

---

## 📝 Change Log Protocol

When making production changes:

1. Document in relevant `.ai/features/<feature>.md` file
2. Note production-specific considerations
3. Update changelog with date and change description
4. User should commit changes to git

---

**Last Updated:** 2026-01-16

