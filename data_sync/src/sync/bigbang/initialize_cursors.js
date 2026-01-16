const logger = require('../../logger');

/**
 * Initialize cursor tracking for all pollers
 * Sets initial cursor values based on current max updatedAt in CMG
 */
async function initializeCursors(prisma, cmgDb) {
  logger.info('[BIGBANG] Initializing poller cursors...');
  
  // Get max updatedAt and ObjectId from CMG collections
  const maxUserUpdatedAt = await getMaxUpdatedAt(cmgDb, 'users');
  const maxProjectUpdatedAt = await getMaxUpdatedAt(cmgDb, 'projects');
  
  // For datasets, get max from both datasets and dataproducts collections
  const maxDatasetsResult = await getMaxUpdatedAt(cmgDb, 'datasets');
  const maxDataproductsResult = await getMaxUpdatedAt(cmgDb, 'dataproducts');
  
  // Compare timestamps and use the newer one with its corresponding ObjectId
  const maxDatasetUpdatedAt = maxDatasetsResult.timestamp > maxDataproductsResult.timestamp 
    ? maxDatasetsResult 
    : maxDataproductsResult;
  
  const maxSessionUpdatedAt = await getMaxUpdatedAt(cmgDb, 'sessions');
  
  // Initialize cursors for each poller with both timestamp AND ObjectId
  // This prevents missing updates that occur during bigbang execution
  const cursors = [
    {
      poller_name: 'user_roles',
      last_updated_at: maxUserUpdatedAt.timestamp,
      last_cmg_objectid: maxUserUpdatedAt.objectId,
    },
    {
      poller_name: 'project_acl',
      last_updated_at: maxProjectUpdatedAt.timestamp,
      last_cmg_objectid: maxProjectUpdatedAt.objectId,
    },
    {
      poller_name: 'dataset_activity',
      last_updated_at: maxDatasetUpdatedAt.timestamp,
      last_cmg_objectid: maxDatasetUpdatedAt.objectId,
    },
    {
      poller_name: 'dataset_metadata',
      last_updated_at: maxDatasetUpdatedAt.timestamp,
      last_cmg_objectid: maxDatasetUpdatedAt.objectId,
    },
    {
      poller_name: 'project_metadata',
      last_updated_at: maxProjectUpdatedAt.timestamp,
      last_cmg_objectid: maxProjectUpdatedAt.objectId,
    },
    {
      poller_name: 'session_metadata',
      last_updated_at: maxSessionUpdatedAt.timestamp,
      last_cmg_objectid: maxSessionUpdatedAt.objectId,
    },
  ];
  
  for (const cursor of cursors) {
    await prisma.cmg_sync_cursor.create({
      data: cursor,
    });
    const objectIdDisplay = cursor.last_cmg_objectid ? ` (ObjectId: ${cursor.last_cmg_objectid.substring(0, 8)}...)` : ' (no ObjectId)';
    logger.info(`[BIGBANG] Initialized cursor for ${cursor.poller_name}: ${cursor.last_updated_at.toISOString()}${objectIdDisplay}`);
  }
  
  logger.info('[BIGBANG] Cursor initialization complete');
}

/**
 * Get the maximum updatedAt value and corresponding ObjectId from a CMG collection
 * 
 * Returns both the timestamp and ObjectId to properly initialize cursors.
 * This prevents missing updates that occur during bigbang execution.
 * 
 * @returns {Object} { timestamp: Date, objectId: string|null }
 */
async function getMaxUpdatedAt(db, collectionName) {
  const result = await db.collection(collectionName)
    .find({})
    .sort({ updatedAt: -1 })
    .limit(1)
    .toArray();
  
  if (result.length > 0 && result[0].updatedAt) {
    return {
      timestamp: result[0].updatedAt,
      objectId: result[0]._id.toString()
    };
  }
  
  // If no updatedAt field exists, use current time with null objectId
  return {
    timestamp: new Date(),
    objectId: null
  };
}

module.exports = {
  initializeCursors,
};

