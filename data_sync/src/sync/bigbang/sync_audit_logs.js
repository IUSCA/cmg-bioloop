const { ObjectId } = require('mongodb');
const logger = require('../logger');

/**
 * Convert CMG dataset events to Bioloop audit logs
 * Equivalent to: db_conversion/src/convert/entity/audit_log.py::events_to_audit_logs()
 */
async function syncAuditLogs(prisma, cmgDb, cmgUserId) {
  logger.info('[BIGBANG] Converting dataset audit logs...');
  
  await datasetEventsToAuditLogs(prisma, cmgDb, cmgUserId, 'RAW_DATA');
  await datasetEventsToAuditLogs(prisma, cmgDb, cmgUserId, 'DATA_PRODUCT');
  
  logger.info('[BIGBANG] Audit logs conversion complete');
}

/**
 * Convert events for a specific dataset type
 * Equivalent to: db_conversion/src/convert/entity/audit_log.py::dataset_events_to_audit_logs()
 */
async function datasetEventsToAuditLogs(prisma, cmgDb, cmgUserId, datasetType) {
  const collectionName = datasetType === 'DATA_PRODUCT' ? 'dataproducts' : 'datasets';
  const collection = cmgDb.collection(collectionName);
  
  logger.info(`[BIGBANG] Processing events for ${datasetType}`);
  
  const cmgDatasets = await collection.find({}).toArray();
  let processedCount = 0;
  
  for (const cmgDataset of cmgDatasets) {
    // Skip if no name
    if (!cmgDataset.name) {
      continue;
    }
    
    // Find corresponding Bioloop dataset
    const bioloopDataset = await prisma.dataset.findUnique({
      where: { cmg_id: cmgDataset._id.toString() },
    });
    
    if (!bioloopDataset) {
      continue;
    }
    
    // Convert each event to an audit log entry
    const events = cmgDataset.events || [];
    for (const event of events) {
      const action = event.description;
      const timestamp = event.stamp;
      
      if (action && timestamp) {
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
  }
  
  logger.info(`[BIGBANG] Processed events for ${processedCount} ${datasetType} datasets`);
}

module.exports = {
  syncAuditLogs,
};

