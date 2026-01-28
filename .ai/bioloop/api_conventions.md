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

## Access Control Pattern

**CRITICAL:** Use `isPermittedTo` middleware for authorization. Never manually check ownership or roles in route handlers.

### Standard Pattern

```javascript
const { accessControl } = require('@/middleware/auth');

const router = express.Router();
const isPermittedTo = accessControl('resource_name');

// ✅ CORRECT - Middleware handles authorization
router.post(
  '/:id/action',
  isPermittedTo('update'), // Allows: owner, admin, operator
  asyncHandler(async (req, res) => {
    const resource = await prisma.resource.findUnique({
      where: { id: req.params.id }
    });
    
    if (!resource) {
      return res.status(404).json({ error: 'Not found' });
    }
    
    // Proceed with action - authorization already verified
    // ...
  })
);

// ❌ WRONG - Manual ownership check
router.post(
  '/:id/action',
  isPermittedTo('update'),
  asyncHandler(async (req, res) => {
    const resource = await prisma.resource.findUnique({
      where: { id: req.params.id }
    });
    
    // Don't do this - middleware already handles it!
    if (resource.user_id !== req.user.id) {
      return res.status(403).json({ error: 'Not authorized' });
    }
    
    // ...
  })
);
```

### How `isPermittedTo` Works

The middleware grants access if:
1. **User owns the resource** (e.g., `session.user_id === req.user.id`)
2. **User has 'admin' role** (full access to everything)
3. **User has 'operator' role** (full access to everything)

For specific resources (datasets, sessions, etc.), the middleware:
- Fetches the resource from the database
- Checks ownership or role-based permissions
- Sets `req.permission.granted = true/false`
- Returns 403 if access denied

### When to Use Manual Checks

**Only use manual authorization checks when:**
- Complex business logic requires it (e.g., project ACLs, team permissions)
- The middleware doesn't support your use case
- You need to check permissions on multiple resources

**Example of acceptable manual check:**
```javascript
// Checking if user has access to ALL required resources
router.post('/complex-action', isPermittedTo('create'), asyncHandler(async (req, res) => {
  const { dataset_ids } = req.body;
  
  // Check user has access to all datasets
  for (const datasetId of dataset_ids) {
    const dataset = await prisma.dataset.findUnique({ where: { id: datasetId } });
    
    // Manual check needed because isPermittedTo can't check multiple resources
    const hasAccess = dataset.user_id === req.user.id || 
                      req.user.roles.some(r => ['admin', 'operator'].includes(r.role.name));
    
    if (!hasAccess) {
      return res.status(403).json({ error: `No access to dataset ${datasetId}` });
    }
  }
  
  // Proceed with action
}));
```

### Key Points

1. **Trust the middleware** - If `isPermittedTo` passes, user is authorized
2. **Don't duplicate checks** - Manual checks make code harder to maintain
3. **Log access attempts** - Use `logger` for audit trail, not manual 403s
4. **Consistent errors** - Let middleware provide standard error responses

---

## Workflow Creation Pattern

**CRITICAL:** The Rhythm workflow server generates workflow IDs. Never manually create workflow IDs.

### Pattern Overview

1. Build workflow body from `config.workflow_registry`
2. Call `wfService.create()` with workflow body and args - **Rhythm generates the ID**
3. Create database association using the **returned** workflow ID

### Standard Implementation

```javascript
const wfService = require('@/services/workflow');
const config = require('config');

// ✅ CORRECT - Rhythm generates workflow ID
async function createWorkflowForEntity(entity, wfName, initiatorId) {
  // 1. Get workflow definition from config
  const workflowConfig = config.get('workflow_registry')[wfName];
  
  if (!workflowConfig) {
    throw new Error(`Workflow ${wfName} not found in workflow_registry`);
  }

  // 2. Build workflow body (same pattern as datasets)
  const wfBody = {
    ...workflowConfig,
    name: wfName,
    app_id: config.get('app_id'),
    steps: workflowConfig.steps.map((step) => ({
      ...step,
      queue: step.queue || `${config.get('app_id')}.q`,
    })),
  };

  // 3. Create workflow via Rhythm API (Rhythm generates workflow_id)
  const wf = (await wfService.create({
    ...wfBody,
    args: [entity.id], // Pass entity ID as argument to workflow tasks
  })).data;

  // 4. Create database association using Rhythm-generated workflow_id
  await prisma.workflow.create({
    data: {
      id: wf.workflow_id, // ← Use ID from Rhythm response
      dataset_id: entity.id,
      initiator_id: initiatorId,
    },
  });

  return wf;
}
```

### Real Example: Dataset Workflows

```javascript
// api/src/services/dataset.js
const config = require('config');
const wfService = require('./workflow');
const prisma = require('@/db');

async function create_workflow(dataset, wf_name, initiator_id) {
  // Get workflow body helper
  const wf_body = get_wf_body(wf_name);

  // Check for active workflows with same name
  const active_wfs = dataset.workflows
    .filter((_wf) => _wf.name === wf_body.name)
    .filter((_wf) => !DONE_STATUSES.includes(_wf.status));

  if (active_wfs.length > 0) {
    throw new Error('A workflow with the same name is already running');
  }

  // Create workflow (Rhythm generates ID)
  const wf = (await wfService.create({
    ...wf_body,
    args: [dataset.id],
  })).data;

  // Create association with Rhythm-generated ID
  await prisma.workflow.create({
    data: {
      id: wf.workflow_id,
      dataset_id: dataset.id,
      ...(initiator_id && { initiator_id }),
    },
  });

  return wf;
}

function get_wf_body(wf_name) {
  const wf_body = { ...config.workflow_registry[wf_name] };
  wf_body.name = wf_name;
  wf_body.app_id = config.app_id;
  wf_body.steps = wf_body.steps.map((step) => ({
    ...step,
    queue: step.queue || `${config.app_id}.q`,
  }));
  return wf_body;
}
```

### Real Example: Session Workflows

```javascript
// api/src/routes/sessions.js
router.post(
  '/:id/workflows/:wf',
  isPermittedTo('update'), // ← Authorization handled by middleware
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    const wfName = req.params.wf;

    // Check session exists (no manual ownership check - middleware handles it)
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
    });

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // Build workflow body
    const workflowConfig = config.get('workflow_registry')[wfName];
    const wfBody = {
      ...workflowConfig,
      name: wfName,
      app_id: config.get('app_id'),
      steps: workflowConfig.steps.map((step) => ({
        ...step,
        queue: step.queue || `${config.get('app_id')}.q`,
      })),
    };

    // Create workflow (Rhythm generates ID)
    const wf = (await wfService.create({
      ...wfBody,
      args: [sessionId],
    })).data;

    // Create association with Rhythm-generated ID
    await prisma.session_workflow.create({
      data: {
        session_id: sessionId,
        workflow_id: wf.workflow_id, // ← From Rhythm
        initiator_id: req.user.id,
      },
    });

    return res.json(wf);
  })
);
```

### Common Mistakes

```javascript
// ❌ WRONG - Manually generating workflow ID
const { v4: uuidv4 } = require('uuid');
const workflowId = uuidv4(); // Don't do this!

await prisma.workflow.create({
  data: { id: workflowId, dataset_id: datasetId }
});

await wfService.create({
  id: workflowId, // Rhythm ignores this anyway
  ...wfBody
});

// ❌ WRONG - Deleting association on workflow creation failure
try {
  await wfService.create(wfBody);
} catch (error) {
  // Don't delete association if it doesn't exist yet
  await prisma.workflow.deleteMany({ where: { id: workflowId } });
}

// ✅ CORRECT - Create workflow first, THEN create association
const wf = (await wfService.create(wfBody)).data;
await prisma.workflow.create({
  data: { id: wf.workflow_id, dataset_id: datasetId }
});
// If association creation fails, workflow exists in Rhythm but not in DB
// This is acceptable - workflow will run but won't show in UI
```

### Error Handling

```javascript
// ✅ CORRECT - Simple error handling
try {
  wf = (await wfService.create({ ...wfBody, args: [entityId] })).data;
  logger.info(`Workflow ${wf.workflow_id} created for entity ${entityId}`);
} catch (error) {
  logger.error(`Failed to create workflow:`, error);
  return res.status(500).json({ error: 'Failed to create workflow' });
}

// Create association after successful workflow creation
await prisma.entity_workflow.create({
  data: {
    entity_id: entityId,
    workflow_id: wf.workflow_id,
    initiator_id: req.user.id,
  },
});
```

### Key Points

1. **Rhythm generates workflow IDs** - never use `uuid` or manual ID generation
2. **Workflow created first** - then database association
3. **Use returned `workflow_id`** - from `wfService.create()` response
4. **Don't delete on failure** - if workflow creation fails, there's no association to delete
5. **Pass entity ID as args** - `args: [entityId]` makes ID available to workflow tasks
6. **Build consistent wfBody** - name, app_id, steps with queues

---

## Quick Reference Checklist

### Starting New API Route
- [ ] Import `prisma` from `@/db`
- [ ] Use `asyncHandler` for route handler
- [ ] Add `express-validator` validation
- [ ] Use `isPermittedTo` for authorization
- [ ] **Trust `isPermittedTo` - no manual ownership checks**
- [ ] Use `createError` for HTTP errors
- [ ] Use `logger` for logging
- [ ] Check if service methods need transaction support

### Creating Workflows
- [ ] Get workflow config from `config.workflow_registry`
- [ ] Build wfBody with name, app_id, and mapped steps
- [ ] Call `wfService.create()` with args - **Rhythm generates ID**
- [ ] Create database association with returned `workflow_id`
- [ ] Never manually generate workflow IDs
- [ ] Never delete association in catch block (it doesn't exist yet)
- [ ] Use `isPermittedTo` for authorization - no manual checks

---

**Last Updated:** 2026-01-27

