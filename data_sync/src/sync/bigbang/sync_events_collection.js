const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Convert CMG events collection 'Download Copy' events to Bioloop data_access_log records
 * 
 * For historic CMG "Download Copy" events:
 * 1. Finds the corresponding Bioloop user and dataset
 * 2. Determines access_type from the dataset's staged path:
 *    - /N/project → SLATE_PROJECT
 *    - /N/scratch → SLATE_SCRATCH
 *    - Otherwise → DCWAN
 * 3. Creates data_access_log with file_id=null
 */
async function syncEventsCollection(prisma, cmgDb, cmgUserId) {
  logger.info('[BIGBANG] Converting CMG events collection to data_access_log...');
  
  const eventsCollection = cmgDb.collection('events');
  const datasetsCollection = cmgDb.collection('datasets');
  const dataproductsCollection = cmgDb.collection('dataproducts');
  
  // Get total count for progress tracking
  const totalCount = await eventsCollection.countDocuments({ action: 'Download Copy' });
  logger.info(`[BIGBANG] Found ${totalCount} "Download Copy" events to process`);
  
  let processedCount = 0;
  let createdLogsCount = 0;
  let skippedCount = {
    noUser: 0,
    noDataset: 0,
    noStagedPath: 0,
    noDatasetInBioloop: 0,
  };
  const BATCH_SIZE = 100;
  
  // Query for all 'Download Copy' events
  const cursor = eventsCollection.find({ action: 'Download Copy' }).batchSize(BATCH_SIZE);
  
  for await (const cmgEvent of cursor) {
    processedCount++;
    
    // Log progress every 100 events
    if (processedCount % 100 === 0) {
      logger.info(`[BIGBANG] Processed ${processedCount}/${totalCount} events (${createdLogsCount} logs created)`);
    }
    
    // 1. Find Bioloop user by CMG user ID
    if (!cmgEvent.user) {
      skippedCount.noUser++;
      continue;
    }
    
    const bioloopUser = await prisma.user.findFirst({
      where: { cmg_id: cmgEvent.user.toString() },
    });
    
    if (!bioloopUser) {
      skippedCount.noUser++;
      continue;
    }
    
    // 2. Get CMG dataset/dataproduct ID from event
    let cmgDataId = null;
    
    if (cmgEvent.dataset) {
      cmgDataId = cmgEvent.dataset;
    } else if (cmgEvent.dataproduct) {
      cmgDataId = cmgEvent.dataproduct;
    }
    
    if (!cmgDataId) {
      skippedCount.noDataset++;
      continue;
    }
    
    // 3. Find corresponding Bioloop dataset (CHECK BIOLOOP FIRST!)
    const bioloopDataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgDataId.toString() },
    });
    
    if (!bioloopDataset) {
      skippedCount.noDatasetInBioloop++;
      continue;
    }
    
    // 4. Look up CMG dataset/dataproduct to get staged path (for access_type determination)
    let cmgDataset = null;
    
    if (cmgEvent.dataset) {
      cmgDataset = await datasetsCollection.findOne({ _id: new ObjectId(cmgDataId) });
    } else if (cmgEvent.dataproduct) {
      cmgDataset = await dataproductsCollection.findOne({ _id: new ObjectId(cmgDataId) });
    }
    
    if (!cmgDataset) {
      skippedCount.noDataset++;
      continue;
    }
    
    // 5. Determine access_type from staged path
    const stagedPath = cmgDataset.paths?.staged;
    
    if (!stagedPath) {
      skippedCount.noStagedPath++;
      continue;
    }
    
    let accessType;
    if (stagedPath.startsWith('/N/project')) {
      accessType = 'SLATE_PROJECT';
    } else if (stagedPath.startsWith('/N/scratch')) {
      accessType = 'SLATE_SCRATCH';
    } else {
      accessType = 'DCWAN';
    }
    
    // 5. Create data_access_log
    try {
      await prisma.data_access_log.create({
        data: {
          access_type: accessType,
          file_id: null,
          dataset_id: bioloopDataset.id,
          user_id: bioloopUser.id,
          timestamp: cmgEvent.createdAt || new Date(),
        },
      });
      createdLogsCount++;
    } catch (error) {
      // Log error but continue processing
      logger.warn(`[BIGBANG] Error creating data_access_log for event ${cmgEvent._id}: ${error.message}`);
    }
  }
  
  // Final summary
  logger.info(`[BIGBANG] Events collection conversion complete:`);
  logger.info(`[BIGBANG]   Total events processed: ${processedCount}`);
  logger.info(`[BIGBANG]   Data access logs created: ${createdLogsCount}`);
  logger.info(`[BIGBANG]   Skipped (no user in Bioloop): ${skippedCount.noUser}`);
  logger.info(`[BIGBANG]   Skipped (no dataset in Bioloop): ${skippedCount.noDatasetInBioloop}`);
  logger.info(`[BIGBANG]   Skipped (no dataset in CMG / no staged path): ${skippedCount.noDataset}`);
  logger.info(`[BIGBANG]   Skipped (staged path empty): ${skippedCount.noStagedPath}`);
}

module.exports = syncEventsCollection;
