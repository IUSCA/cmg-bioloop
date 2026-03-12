const { ObjectId } = require('mongodb');
const logger = require('../../../logger');
const { hasEvent, getEventTimestamp } = require('../utils/event_parser');

/**
 * Convert CMG dataset stage/download events to Bioloop data_access_log, 
 * stage_request_log, and dataset_state records
 * 
 * For historic CMG "Stage - finish" events:
 * 1. Creates data_access_log with access_type='SLATE_PROJECT' (CMG downloads = staging to filesystem)
 * 2. Creates stage_request_log 
 * 3. Creates dataset_state entries for FETCHED and STAGED
 */
async function syncDownloadStageLogs(prisma, cmgDb, cmgUserId) {
  logger.info('[BIGBANG] Converting historic stage/download events to logs...');
  
  await processStageEvents(prisma, cmgDb, cmgUserId, 'RAW_DATA');
  await processStageEvents(prisma, cmgDb, cmgUserId, 'DATA_PRODUCT');
  
  logger.info('[BIGBANG] Stage/download logs conversion complete');
}

/**
 * Process stage events for a specific dataset type
 */
async function processStageEvents(prisma, cmgDb, cmgUserId, datasetType) {
  const collectionName = datasetType === 'DATA_PRODUCT' ? 'dataproducts' : 'datasets';
  const collection = cmgDb.collection(collectionName);
  
  logger.info(`[BIGBANG] Processing stage events for ${datasetType}`);
  
  // Get total count for progress tracking
  const totalCount = await collection.countDocuments({});
  logger.info(`[BIGBANG] Found ${totalCount} datasets to process for stage/download logs`);
  
  let processedCount = 0;
  let stageEventsCount = 0;
  const BATCH_SIZE = 100;
  
  // Use cursor to stream data
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
    
    // Check for "Stage - finish" events (case-insensitive)
    let events = cmgDataset.events || [];
    
    // Handle case where events is a string (malformed data in CMG)
    if (!Array.isArray(events)) {
      if (typeof events === 'string') {
        const trimmed = events.trim();
        // Try to parse as JSON array
        try {
          const parsed = JSON.parse(trimmed);
          if (Array.isArray(parsed)) {
            logger.info(`[BIGBANG] Recovered events array from JSON string for dataset ${cmgDataset.name}: ${parsed.length} events`);
            events = parsed;
          } else {
            logger.warn(`[BIGBANG] Parsed events but result is not an array for dataset ${cmgDataset.name}`);
            processedCount++;
            continue;
          }
        } catch (e) {
          logger.warn(`[BIGBANG] Could not parse events string for dataset ${cmgDataset.name}: ${trimmed.substring(0, 100)}`);
          processedCount++;
          continue;
        }
      } else {
        logger.warn(`[BIGBANG] [DIAGNOSTIC] Dataset events field is NOT an array - skipping stage/download logs`);
        logger.warn(`[BIGBANG] [DIAGNOSTIC]   Collection: ${collectionName}`);
        logger.warn(`[BIGBANG] [DIAGNOSTIC]   Dataset _id: ${cmgDataset._id}`);
        logger.warn(`[BIGBANG] [DIAGNOSTIC]   Dataset name: ${cmgDataset.name || '(no name)'}`);
        logger.warn(`[BIGBANG] [DIAGNOSTIC]   events type: ${typeof events}`);
        logger.warn(`[BIGBANG] [DIAGNOSTIC]   events value: ${JSON.stringify(events).substring(0, 200)}`);
        processedCount++;
        continue; // Skip to next dataset
      }
    }
    
    // Log first valid events array example for reference (only once)
    if (processedCount === 0 && events.length > 0) {
      logger.info(`[BIGBANG] [DIAGNOSTIC] Example of valid events array structure:`);
      logger.info(`[BIGBANG] [DIAGNOSTIC]   Dataset: ${cmgDataset.name}`);
      logger.info(`[BIGBANG] [DIAGNOSTIC]   events.length: ${events.length}`);
      logger.info(`[BIGBANG] [DIAGNOSTIC]   First event: ${JSON.stringify(events[0])}`);
    }
    
    const stageFinishEvents = events.filter(event => 
      event.description && 
      event.description.toLowerCase().includes('stage') && 
      event.description.toLowerCase().includes('finish')
    );
    
    if (stageFinishEvents.length > 0) {
      stageEventsCount++;
      
      // Process each stage finish event
      for (const event of stageFinishEvents) {
        const timestamp = event.stamp;
        if (!timestamp) {
          continue;
        }
        
        // 1. Create data_access_log (CMG staging = downloading to filesystem like slate-project)
        try {
          await prisma.data_access_log.create({
            data: {
              timestamp: timestamp,
              access_type: 'SLATE_PROJECT',
              dataset_id: bioloopDataset.id,
              user_id: cmgUserId,
              // file_id is left null for dataset-level staging
            },
          });
        } catch (error) {
          // Skip if duplicate or other error
          logger.debug(`[BIGBANG] Could not create data_access_log for dataset ${bioloopDataset.id}: ${error.message}`);
        }
        
        // 2. Create stage_request_log
        try {
          await prisma.stage_request_log.create({
            data: {
              timestamp: timestamp,
              dataset_id: bioloopDataset.id,
              user_id: cmgUserId,
            },
          });
        } catch (error) {
          // Skip if duplicate or other error
          logger.debug(`[BIGBANG] Could not create stage_request_log for dataset ${bioloopDataset.id}: ${error.message}`);
        }
        
        // 3. Create dataset_state entries (FETCHED and STAGED)
        // FETCHED timestamp = event timestamp minus 1ms
        // STAGED timestamp = event timestamp
        const fetchedTimestamp = new Date(timestamp.getTime() - 1);
        const stagedTimestamp = timestamp;
        
        // Create FETCHED state
        try {
          await prisma.dataset_state.create({
            data: {
              state: 'FETCHED',
              timestamp: fetchedTimestamp,
              dataset_id: bioloopDataset.id,
            },
          });
        } catch (error) {
          // Skip if duplicate (composite PK: timestamp, dataset_id, state)
          logger.debug(`[BIGBANG] Could not create FETCHED state for dataset ${bioloopDataset.id}: ${error.message}`);
        }
        
        // Create STAGED state
        try {
          await prisma.dataset_state.create({
            data: {
              state: 'STAGED',
              timestamp: stagedTimestamp,
              dataset_id: bioloopDataset.id,
            },
          });
        } catch (error) {
          // Skip if duplicate
          logger.debug(`[BIGBANG] Could not create STAGED state for dataset ${bioloopDataset.id}: ${error.message}`);
        }
      }
    }
    
    processedCount++;
    
    // Progress logging
    if (processedCount % 500 === 0) {
      logger.info(`[BIGBANG] Processed ${processedCount}/${totalCount} datasets (${Math.round(processedCount / totalCount * 100)}%) - Found ${stageEventsCount} with stage events`);
    }
  }
  
  logger.info(`[BIGBANG] Processed ${processedCount} ${datasetType} datasets, found ${stageEventsCount} with stage events`);
}

module.exports = {
  syncDownloadStageLogs,
};
