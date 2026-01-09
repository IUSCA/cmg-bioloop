const logger = require('@/services/logger');

/**
 * Initialize cursor tracking for all pollers
 * Sets initial cursor values based on current max updatedAt in CMG
 */
async function initializeCursors(prisma, cmgDb) {
  logger.info('[BIGBANG] Initializing poller cursors...');
  
  // Get max updatedAt from CMG collections
  const maxUserUpdatedAt = await getMaxUpdatedAt(cmgDb, 'users');
  const maxProjectUpdatedAt = await getMaxUpdatedAt(cmgDb, 'projects');
  const maxDatasetUpdatedAt = await Math.max(
    await getMaxUpdatedAt(cmgDb, 'datasets'),
    await getMaxUpdatedAt(cmgDb, 'dataproducts')
  );
  const maxSessionUpdatedAt = await getMaxUpdatedAt(cmgDb, 'sessions');
  
  // Initialize cursors for each poller
  const cursors = [
    {
      poller_name: 'user_roles',
      last_updated_at: maxUserUpdatedAt ? new Date(maxUserUpdatedAt) : new Date(),
      last_cmg_objectid: null,
    },
    {
      poller_name: 'project_acl',
      last_updated_at: maxProjectUpdatedAt ? new Date(maxProjectUpdatedAt) : new Date(),
      last_cmg_objectid: null,
    },
    {
      poller_name: 'dataset_activity',
      last_updated_at: maxDatasetUpdatedAt ? new Date(maxDatasetUpdatedAt) : new Date(),
      last_cmg_objectid: null,
    },
    {
      poller_name: 'dataset_metadata',
      last_updated_at: maxDatasetUpdatedAt ? new Date(maxDatasetUpdatedAt) : new Date(),
      last_cmg_objectid: null,
    },
    {
      poller_name: 'project_metadata',
      last_updated_at: maxProjectUpdatedAt ? new Date(maxProjectUpdatedAt) : new Date(),
      last_cmg_objectid: null,
    },
    {
      poller_name: 'session_metadata',
      last_updated_at: maxSessionUpdatedAt ? new Date(maxSessionUpdatedAt) : new Date(),
      last_cmg_objectid: null,
    },
  ];
  
  for (const cursor of cursors) {
    await prisma.cmg_sync_cursor.create({
      data: cursor,
    });
    logger.info(`[BIGBANG] Initialized cursor for ${cursor.poller_name}: ${cursor.last_updated_at}`);
  }
  
  logger.info('[BIGBANG] Cursor initialization complete');
}

/**
 * Get the maximum updatedAt value from a CMG collection
 */
async function getMaxUpdatedAt(db, collectionName) {
  const result = await db.collection(collectionName)
    .find({})
    .sort({ updatedAt: -1 })
    .limit(1)
    .toArray();
  
  if (result.length > 0 && result[0].updatedAt) {
    return result[0].updatedAt;
  }
  
  // If no updatedAt field exists, use current time
  return new Date();
}

module.exports = {
  initializeCursors,
};

