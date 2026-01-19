const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Parse CMG event description and map to Bioloop action
 * Only process workflow "finish" events, ignore "start" events
 * 
 * Examples:
 *   "Stage - finish" → "staged"
 *   "Validate - finish" → "validated"
 *   "Archive - finish" → "archived"
 *   "Register - finish" → "registered"
 */
function parseCMGEventToAction(eventDescription) {
  if (!eventDescription) {
    return null;
  }
  
  const desc = eventDescription.trim();
  
  // Only process "finish" events (ignore "start" events to avoid duplicates)
  if (!desc.includes('finish')) {
    return null;
  }
  
  // Extract workflow name before " - finish"
  const match = desc.match(/^(.+?)\s*-\s*finish$/i);
  if (!match) {
    return null;
  }
  
  const workflowName = match[1].trim().toLowerCase();
  
  // Map workflow names to action names (past tense)
  const actionMap = {
    'stage': 'staged',
    'validate': 'validated',
    'archive': 'archived',
    'register': 'registered',
    'inspect': 'inspected',
    'convert': 'converted',
  };
  
  return actionMap[workflowName] || null;
}

/**
 * Convert CMG dataset events to Bioloop audit logs
 */
async function syncAuditLogs(prisma, cmgDb, cmgUserId) {
  logger.info('[BIGBANG] Converting dataset audit logs...');
  
  await datasetEventsToAuditLogs(prisma, cmgDb, cmgUserId, 'RAW_DATA');
  await datasetEventsToAuditLogs(prisma, cmgDb, cmgUserId, 'DATA_PRODUCT');
  
  logger.info('[BIGBANG] Audit logs conversion complete');
}

/**
 * Convert events for a specific dataset type
 */
async function datasetEventsToAuditLogs(prisma, cmgDb, cmgUserId, datasetType) {
  const collectionName = datasetType === 'DATA_PRODUCT' ? 'dataproducts' : 'datasets';
  const collection = cmgDb.collection(collectionName);
  
  logger.info(`[BIGBANG] Processing events for ${datasetType}`);
  
  // Get total count for progress tracking
  const totalCount = await collection.countDocuments({});
  logger.info(`[BIGBANG] Found ${totalCount} datasets to process for audit logs`);
  
  let processedCount = 0;
  const BATCH_SIZE = 100;
  
  // Use cursor to stream data instead of loading all at once
  const cursor = collection.find({}).batchSize(BATCH_SIZE);
  
  for await (const cmgDataset of cursor) {
    // Skip if no name
    if (!cmgDataset.name) {
      continue;
    }
    
    // Find corresponding Bioloop dataset
    const bioloopDataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgDataset._id.toString() },
    });
    
    if (!bioloopDataset) {
      continue;
    }
    
    // Convert each event to an audit log entry
    // Only process workflow "finish" events to avoid duplicate "start"/"finish" entries
    const events = cmgDataset.events || [];
    for (const event of events) {
      const timestamp = event.stamp;
      if (!timestamp) {
        continue;
      }
      
      // Parse CMG event description to Bioloop action (only "finish" events)
      const action = parseCMGEventToAction(event.description);
      
      if (action) {
        await prisma.dataset_audit.create({
          data: {
            action: action,
            timestamp: timestamp,
            dataset_id: bioloopDataset.id,
            user_id: cmgUserId,
          },
        });
      }
    }
    
    processedCount++;
    
    // Progress logging
    if (processedCount % 500 === 0) {
      logger.info(`[BIGBANG] Processed ${processedCount}/${totalCount} datasets (${Math.round(processedCount / totalCount * 100)}%)`);
    }
  }
  
  logger.info(`[BIGBANG] Processed events for ${processedCount} ${datasetType} datasets`);
}

module.exports = {
  syncAuditLogs,
  parseCMGEventToAction, // Exported for testing/reuse
};

