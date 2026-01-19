# API Development Conventions

## Import Organization

**ALWAYS organize imports at the top of JavaScript files:**

```javascript
// ✅ CORRECT - Imports at the top
const express = require('express');
const { body, param, query } = require('express-validator');
const prisma = require('@/db');
const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl } = require('@/middleware/auth');
const createError = require('http-errors');
const logger = require('@/services/logger');
const datasetService = require('@/services/dataset');

const router = express.Router();
const isPermittedTo = accessControl('sessions');

// Route definitions below...
```

**Import Order (recommended):**
1. External dependencies (express, lodash, etc.)
2. Validation libraries (express-validator)
3. Database clients (`@/db`)
4. Middleware (`@/middleware/*`)
5. Services (`@/services/*`)
6. Utilities (`@/utils/*`)
7. Constants and configuration

**Key Points:**
- All `require()` statements should be at the top of the file
- Avoid importing modules inside route handlers or functions
- Group related imports together
- Use blank lines to separate import groups

---

## Prisma Instance Reuse

**ALWAYS reuse the Prisma instance from `@/db`:**

```javascript
// ✅ CORRECT
const prisma = require('@/db');

router.get('/:id', asyncHandler(async (req, res) => {
  const session = await prisma.genome_browser_session.findUnique({
    where: { id: req.params.id }
  });
}));
```

**NEVER create new Prisma instances:**
```javascript
// ❌ WRONG
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();
```

---

## Transaction Pattern for Multi-Operation Calls

When a single API call needs to perform multiple database operations across different service methods:

**Pattern: Service methods accept optional Prisma/transaction instance**

```javascript
// api/src/services/dataset.js
const prisma = require('@/db');

async function createDataset(data, tx = prisma) {
  return tx.dataset.create({ data });
}

async function linkToProject(datasetId, projectId, tx = prisma) {
  return tx.project_dataset.create({
    data: { dataset_id: datasetId, project_id: projectId }
  });
}

module.exports = { createDataset, linkToProject };
```

```javascript
// api/src/routes/datasets.js
const prisma = require('@/db');
const datasetService = require('@/services/dataset');

router.post('/', asyncHandler(async (req, res) => {
  const { name, type, project_id } = req.body;
  
  // Use transaction when operations must be atomic
  const result = await prisma.$transaction(async (tx) => {
    // Create dataset
    const dataset = await datasetService.createDataset(
      { name, type, user_id: req.user.id },
      tx
    );
    
    // Link to project
    await datasetService.linkToProject(dataset.id, project_id, tx);
    
    // Return full dataset with relation
    return tx.dataset.findUnique({
      where: { id: dataset.id },
      include: { projects: true }
    });
  });
  
  res.status(201).json({ dataset: result });
}));
```

**Key Points:**
- Service methods default to `prisma` if no transaction provided: `tx = prisma`
- When transaction needed, pass `tx` to all service calls
- Always return the final result from transaction callback
- Use transactions for: create → link, update → audit log, delete → cleanup

---

## Route Handler Structure

**Standard pattern:**
```javascript
const express = require('express');
const { body, param, query } = require('express-validator');
const prisma = require('@/db');
const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl } = require('@/middleware/auth');
const createError = require('http-errors');
const logger = require('@/services/logger');

const router = express.Router();
const isPermittedTo = accessControl('resource_name');

router.get(
  '/:id',
  isPermittedTo('read'),
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const id = req.params.id;
    
    const resource = await prisma.resource.findUnique({
      where: { id }
    });
    
    if (!resource) throw createError(404, 'Resource not found');
    
    res.json({ resource });
  })
);

module.exports = router;
```

**Key Elements:**
1. Use `asyncHandler` for all async route handlers (auto error catching)
2. Use `express-validator` for input validation before handler
3. Use `createError` for HTTP errors (not `throw new Error()`)
4. Use `logger` for logging (not `console.log`)
5. Use `isPermittedTo` middleware for authorization
6. Always validate and sanitize user input

---

## Router Mounting Order

**CRITICAL:** File exposure routers must be mounted **before** global authentication middleware:

```javascript
// api/src/routes/index.js

// ✅ CORRECT ORDER
const fileExposureRouter = require('./sessions').fileExposureRouter;

// 1. Mount file exposure (has its own auth)
app.use('/sessions', fileExposureRouter);

// 2. Global authentication middleware
app.use(authenticate);

// 3. Other protected routes
app.use('/sessions', sessionsRouter);
app.use('/tracks', tracksRouter);
```

**Reason:** Genome browsers make `GET` requests with cookies; global middleware would trigger CORS preflight for routes that need cookie auth.

---

## API Response Patterns

**Consistent response structure:**
```javascript
// Single resource
res.json({ session });
res.json({ track });

// List with metadata
res.json({
  sessions,
  total: count,
  limit: req.query.limit,
  offset: req.query.offset
});

// Error (use createError)
throw createError(404, 'Resource not found');
throw createError(403, 'Not authorized');
```

---

## Error Handling Pattern

```javascript
const createError = require('http-errors');
const logger = require('@/services/logger');

// ✅ CORRECT
router.get('/:id', asyncHandler(async (req, res) => {
  const resource = await prisma.resource.findUnique({
    where: { id: req.params.id }
  });
  
  if (!resource) {
    throw createError(404, 'Resource not found');
  }
  
  res.json({ resource });
}));

// ❌ WRONG
router.get('/:id', async (req, res) => {
  try {
    const resource = await prisma.resource.findUnique({
      where: { id: req.params.id }
    });
    
    if (!resource) {
      throw new Error('Not found'); // No status code
    }
    
    res.json({ resource });
  } catch (error) {
    res.status(500).json({ error: error.message }); // Manual error handling
  }
});
```

---

## Logging Pattern

```javascript
const logger = require('@/services/logger');

// API logging
logger.info('[FILE EXPOSE] Request received');
logger.warn('[IGV] Unsupported file type:', filePath);
logger.error('[WashU] Failed to initialize:', error);
```

**When to use logger:**
- API server logs
- Worker task logs
- Production-grade logging

**When to use console.log:**
- CLI scripts for user feedback
- Debug statements during development (remove in production)

---

## Quick Reference Checklist

### Starting New API Route
- [ ] Import `prisma` from `@/db`
- [ ] Use `asyncHandler` for route handler
- [ ] Add `express-validator` validation
- [ ] Use `isPermittedTo` for authorization
- [ ] Use `createError` for HTTP errors
- [ ] Use `logger` for logging
- [ ] Check if service methods need transaction support

---

**Last Updated:** 2026-01-16

