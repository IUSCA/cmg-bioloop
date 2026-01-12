/**
 * CMG Sync - Cursor Manager
 * 
 * Handles cursor tracking and locking for pollers
 * Ensures no missed updates and prevents concurrent runs
 */

const logger = require('../logger');
const os = require('os');

const INSTANCE_ID = `${os.hostname()}-${process.pid}`;
const DEFAULT_LOCK_TTL_MS = 60000; // 60 seconds

/**
 * Acquire lock for a poller
 * 
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} pollerName - Name of the poller
 * @param {string} instanceId - Instance identifier
 * @param {number} lockTtlMs - Lock TTL in milliseconds
 * @returns {Object|null} Cursor object if lock acquired, null if already locked
 */
async function acquireLock(prisma, pollerName, instanceId, lockTtlMs = DEFAULT_LOCK_TTL_MS) {
  
  try {
    return await prisma.$transaction(async (tx) => {
      const cursor = await tx.cmg_sync_cursor.findUnique({
        where: { poller_name: pollerName }
      });
      
      if (!cursor) {
        throw new Error(`Cursor not found for poller: ${pollerName}. Run big-bang script first.`);
      }
      
      const now = new Date();
      const canAcquire = !cursor.locked_by || 
                         (cursor.lock_expires_at && cursor.lock_expires_at < now);
      
      if (!canAcquire) {
        logger.debug(`[${pollerName}] Lock held by ${cursor.locked_by}, expires at ${cursor.lock_expires_at}`);
        return null;
      }
      
      // Acquire lock
      const updated = await tx.cmg_sync_cursor.update({
        where: { poller_name: pollerName },
        data: {
          locked_by: instanceId,
          lock_expires_at: new Date(now.getTime() + lockTtlMs),
          last_started_at: now
        }
      });
      
      logger.debug(`[${pollerName}] Lock acquired by ${instanceId}`);
      return updated;
    });
  } catch (error) {
    logger.error(`[${pollerName}] Failed to acquire lock:`, error);
    throw error;
  }
}

/**
 * Release lock for a poller
 * 
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} pollerName - Name of the poller
 * @param {boolean} success - Whether the poll round succeeded
 * @param {Error|null} error - Error object if failed
 * @param {number} processedCount - Number of documents processed
 */
async function releaseLock(prisma, pollerName, success, error = null, processedCount = 0) {
  
  try {
    const updateData = {
      locked_by: null,
      lock_expires_at: null
    };
    
    if (success) {
      updateData.last_succeeded_at = new Date();
      updateData.last_run_count = processedCount;
    } else {
      updateData.last_failed_at = new Date();
      updateData.last_error = error ? error.message.substring(0, 500) : 'Unknown error';
    }
    
    await prisma.cmg_sync_cursor.update({
      where: { poller_name: pollerName },
      data: updateData
    });
    
    logger.debug(`[${pollerName}] Lock released. Success: ${success}, Processed: ${processedCount}`);
  } catch (lockError) {
    logger.error(`[${pollerName}] Failed to release lock:`, lockError);
    // Don't throw - we want to continue even if lock release fails
  }
}

/**
 * Update cursor position after successful batch processing
 * 
 * @param {Object} tx - Prisma transaction
 * @param {string} pollerName - Name of the poller
 * @param {Object} lastDoc - Last processed CMG document
 * @param {Date} lastDoc.updatedAt - updatedAt timestamp
 * @param {ObjectId} lastDoc._id - MongoDB _id
 */
async function updateCursor(tx, pollerName, lastDoc) {
  if (!lastDoc || !lastDoc.updatedAt || !lastDoc._id) {
    throw new Error(`Invalid lastDoc for cursor update: ${JSON.stringify(lastDoc)}`);
  }
  
  await tx.cmg_sync_cursor.update({
    where: { poller_name: pollerName },
    data: {
      last_updated_at: lastDoc.updatedAt,
      last_cmg_objectid: lastDoc._id.toString()
    }
  });
  
  logger.debug(`[${pollerName}] Cursor updated to updatedAt=${lastDoc.updatedAt.toISOString()}, _id=${lastDoc._id}`);
}

/**
 * Get cursor position for a poller
 * 
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} pollerName - Name of the poller
 * @returns {Object} Cursor object
 */
async function getCursor(prisma, pollerName) {
  
  const cursor = await prisma.cmg_sync_cursor.findUnique({
    where: { poller_name: pollerName }
  });
  
  if (!cursor) {
    throw new Error(`Cursor not found for poller: ${pollerName}`);
  }
  
  return cursor;
}

/**
 * Initialize cursor for a poller (called by big-bang script)
 * 
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} pollerName - Name of the poller
 * @param {Date|null} initialUpdatedAt - Initial updatedAt value (null for start from beginning)
 * @param {string|null} initialObjectId - Initial ObjectId value
 */
async function initializeCursor(prisma, pollerName, initialUpdatedAt = null, initialObjectId = null) {
  
  await prisma.cmg_sync_cursor.upsert({
    where: { poller_name: pollerName },
    create: {
      poller_name: pollerName,
      last_updated_at: initialUpdatedAt,
      last_cmg_objectid: initialObjectId
    },
    update: {
      last_updated_at: initialUpdatedAt,
      last_cmg_objectid: initialObjectId,
      locked_by: null,
      lock_expires_at: null
    }
  });
  
  logger.info(`[${pollerName}] Cursor initialized`);
}

/**
 * Get all cursor statuses (for monitoring)
 * 
 * @returns {Array<Object>} Array of cursor statuses
 */
async function getAllCursorStatuses() {
  const prisma = getPrismaClient();
  
  const cursors = await prisma.cmg_sync_cursor.findMany({
    orderBy: { poller_name: 'asc' }
  });
  
  return cursors.map(cursor => {
    const now = new Date();
    const minutesBehind = cursor.last_updated_at 
      ? (now - cursor.last_updated_at) / (1000 * 60)
      : null;
    
    return {
      poller_name: cursor.poller_name,
      last_updated_at: cursor.last_updated_at,
      last_cmg_objectid: cursor.last_cmg_objectid,
      locked: !!cursor.locked_by,
      locked_by: cursor.locked_by,
      lock_expires_at: cursor.lock_expires_at,
      last_succeeded_at: cursor.last_succeeded_at,
      last_failed_at: cursor.last_failed_at,
      last_run_count: cursor.last_run_count,
      minutes_behind: minutesBehind ? Math.round(minutesBehind) : null
    };
  });
}

/**
 * Force release all locks (emergency use only)
 */
async function forceReleaseAllLocks() {
  const prisma = getPrismaClient();
  
  const result = await prisma.cmg_sync_cursor.updateMany({
    where: {
      locked_by: { not: null }
    },
    data: {
      locked_by: null,
      lock_expires_at: null
    }
  });
  
  logger.warn(`[Cursor Manager] Force released ${result.count} locks`);
  return result.count;
}

module.exports = {
  acquireLock,
  releaseLock,
  updateCursor,
  getCursor,
  initializeCursor,
  getAllCursorStatuses,
  forceReleaseAllLocks,
  INSTANCE_ID
};

