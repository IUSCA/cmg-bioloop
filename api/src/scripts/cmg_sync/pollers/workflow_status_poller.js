/**
 * CMG Sync - Workflow Status Poller
 * 
 * Polls Rhythm MongoDB workflow_meta collection to determine dataset states
 * based on workflow completion (100% progress = SUCCESS status)
 * 
 * Only "integrated" and "stage" workflows assign states to datasets
 */

const { MongoClient } = require('mongodb');
const config = require('config');
const logger = require('@/services/logger');
const { getPrismaClient } = require('../connections');
const { acquireLock, releaseLock, updateCursor } = require('../cursor_manager');
const { buildCursorQuery } = require('../utils/cmg_helpers');
const { logSyncError } = require('../error_logger');

const POLLER_NAME = 'workflow_status';
const BATCH_SIZE = 200;
const POLL_INTERVAL_MS = 10000; // 10 seconds

// Map workflow names to dataset states
const WORKFLOW_STATE_MAP = {
  'integrated': {
    // Integrated workflow assigns multiple states based on completion
    states: ['INSPECTED', 'ARCHIVED', 'STAGED']  // All states when complete
  },
  'stage': {
    // Stage workflow assigns STAGED state
    states: ['STAGED']
  }
  // Other workflows (conversion, delete, etc.) don't assign states
};

/**
 * Get Rhythm MongoDB connection
 */
async function getRhythmConnection() {
  const rhythmConfig = config.get('rhythm_mongodb');
  
  const uri = `mongodb://${rhythmConfig.username}:${rhythmConfig.password}@${rhythmConfig.host}:${rhythmConfig.port}/${rhythmConfig.database}?authSource=${rhythmConfig.authSource}`;
  
  const client = new MongoClient(uri, {
    maxPoolSize: 5,
    minPoolSize: 1,
    serverSelectionTimeoutMS: 5000
  });
  
  await client.connect();
  return client;
}

/**
 * Fetch workflows from Rhythm MongoDB with cursor
 */
async function fetchWorkflowBatch(rhythmDb, cursor) {
  const roundEnd = new Date();
  
  // Build query with cursor
  const query = buildCursorQuery(cursor, roundEnd);
  
  // Only fetch completed workflows
  query._status = 'SUCCESS';
  
  // Only fetch workflows that assign states
  query.name = { $in: Object.keys(WORKFLOW_STATE_MAP) };
  
  const workflows = await rhythmDb.collection('workflow_meta')
    .find(query)
    .sort({ updated_at: 1, _id: 1 })
    .limit(BATCH_SIZE)
    .toArray();
  
  return workflows;
}

/**
 * Get dataset ID from workflow
 * Workflows store dataset_id in their args array
 */
function getDatasetIdFromWorkflow(workflow) {
  // Workflow args format: [dataset_id, ...]
  if (workflow.args && Array.isArray(workflow.args) && workflow.args.length > 0) {
    return workflow.args[0];
  }
  
  return null;
}

/**
 * Process a single workflow and update dataset states
 */
async function processWorkflow(workflow, tx) {
  const datasetId = getDatasetIdFromWorkflow(workflow);
  
  if (!datasetId) {
    logger.warn(`[${POLLER_NAME}] Workflow ${workflow._id} has no dataset_id in args`);
    return;
  }
  
  // Verify dataset exists
  const dataset = await tx.dataset.findUnique({
    where: { id: datasetId }
  });
  
  if (!dataset) {
    logger.warn(`[${POLLER_NAME}] Dataset ${datasetId} not found for workflow ${workflow._id}`);
    return;
  }
  
  // Get states to assign based on workflow name
  const stateConfig = WORKFLOW_STATE_MAP[workflow.name];
  
  if (!stateConfig) {
    // Workflow doesn't assign states (shouldn't happen due to query filter)
    logger.debug(`[${POLLER_NAME}] Workflow ${workflow.name} doesn't assign states`);
    return;
  }
  
  // Assign all states for this workflow type
  for (const stateName of stateConfig.states) {
    // Check if state already exists
    const existing = await tx.dataset_state.findFirst({
      where: {
        dataset_id: datasetId,
        state: stateName
      }
    });
    
    if (!existing) {
      await tx.dataset_state.create({
        data: {
          dataset_id: datasetId,
          state: stateName,
          timestamp: workflow.updated_at || new Date()
        }
      });
      
      logger.info(`[${POLLER_NAME}] Added state '${stateName}' to dataset ${datasetId} from workflow ${workflow._id}`);
    } else {
      logger.debug(`[${POLLER_NAME}] State '${stateName}' already exists for dataset ${datasetId}`);
    }
  }
}

/**
 * Process a batch of workflows
 */
async function processBatch(workflows, prisma) {
  let processedCount = 0;
  let errorCount = 0;
  
  for (const workflow of workflows) {
    try {
      await prisma.$transaction(async (tx) => {
        await processWorkflow(workflow, tx);
      });
      
      processedCount++;
    } catch (error) {
      errorCount++;
      
      await logSyncError({
        poller: POLLER_NAME,
        operation: 'process_workflow',
        cmgCollection: 'workflow_meta (Rhythm)',
        cmgId: workflow._id.toString(),
        cmgDocument: workflow,
        error
      });
      
      logger.error(`[${POLLER_NAME}] Failed to process workflow ${workflow._id}:`, error);
      // Continue with next workflow
    }
  }
  
  return { processedCount, errorCount };
}

/**
 * Poll for workflow status updates
 */
async function pollWorkflows() {
  const prisma = getPrismaClient();
  let rhythmClient = null;
  
  try {
    // Acquire lock
    const cursor = await acquireLock(POLLER_NAME);
    
    if (!cursor) {
      logger.debug(`[${POLLER_NAME}] Lock not acquired, skipping this round`);
      return;
    }
    
    // Connect to Rhythm MongoDB
    rhythmClient = await getRhythmConnection();
    const rhythmDb = rhythmClient.db();
    
    // Fetch workflows
    const workflows = await fetchWorkflowBatch(rhythmDb, cursor);
    
    if (workflows.length === 0) {
      logger.debug(`[${POLLER_NAME}] No new workflows to process`);
      await releaseLock(POLLER_NAME, true, null, 0);
      return;
    }
    
    logger.info(`[${POLLER_NAME}] Processing ${workflows.length} workflows`);
    
    // Process workflows and update cursor in transaction
    const { processedCount, errorCount } = await processBatch(workflows, prisma);
    
    // Update cursor to last processed workflow
    if (workflows.length > 0) {
      const lastWorkflow = workflows[workflows.length - 1];
      
      await prisma.$transaction(async (tx) => {
        await updateCursor(tx, POLLER_NAME, {
          updatedAt: lastWorkflow.updated_at,
          _id: lastWorkflow._id
        });
      });
    }
    
    logger.info(`[${POLLER_NAME}] Processed ${processedCount} workflows, ${errorCount} errors`);
    
    // Release lock
    await releaseLock(POLLER_NAME, true, null, processedCount);
    
  } catch (error) {
    logger.error(`[${POLLER_NAME}] Poll round failed:`, error);
    await releaseLock(POLLER_NAME, false, error);
  } finally {
    if (rhythmClient) {
      await rhythmClient.close();
    }
  }
}

/**
 * Start polling loop
 */
async function startPolling() {
  logger.info(`[${POLLER_NAME}] Starting workflow status poller (interval: ${POLL_INTERVAL_MS}ms)`);
  
  while (true) {
    try {
      await pollWorkflows();
    } catch (error) {
      logger.error(`[${POLLER_NAME}] Unexpected error in polling loop:`, error);
    }
    
    // Wait before next poll
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

module.exports = {
  startPolling,
  pollWorkflows,
  POLLER_NAME,
  POLL_INTERVAL_MS
};

