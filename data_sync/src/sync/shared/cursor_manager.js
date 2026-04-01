/**
 * Sync Cursor Manager (Generic)
 *
 * Handles cursor tracking and row-level locking for pollers.
 * Supports both CMG-style (MongoDB ObjectId) and Xenium-style (PostgreSQL integer ID) cursors.
 *
 * Usage:
 *   const mgr = createCursorManager({
 *     cursorModel: 'cmg_sync_cursor',
 *     idField: 'last_cmg_objectid',
 *   });
 */

const logger = require('../../logger');
const os = require('os');

const INSTANCE_ID = `${os.hostname()}-${process.pid}`;
const DEFAULT_LOCK_TTL_MS = 60000; // 60 seconds

/**
 * Create a cursor manager bound to a specific cursor table and ID field.
 *
 * @param {Object} config
 * @param {string} config.cursorModel - Prisma model name (e.g. 'cmg_sync_cursor')
 * @param {string} config.idField - Name of the ID cursor field in the model
 *   (e.g. 'last_cmg_objectid' for CMG, 'last_xenium_id' for xenium)
 * @returns {Object} Cursor manager
 */
function createCursorManager({ cursorModel, idField }) {
  if (!cursorModel || !idField) {
    throw new Error('createCursorManager: cursorModel and idField are required');
  }

  async function acquireLock(prisma, pollerName, instanceId, lockTtlMs = DEFAULT_LOCK_TTL_MS) {
    try {
      return await prisma.$transaction(async (tx) => {
        const cursor = await tx[cursorModel].findUnique({
          where: { poller_name: pollerName },
        });

        if (!cursor) {
          throw new Error(`Cursor not found for poller: ${pollerName}. Run bigbang script first.`);
        }

        const now = new Date();
        const canAcquire = !cursor.locked_by
          || (cursor.lock_expires_at && cursor.lock_expires_at < now);

        if (!canAcquire) {
          logger.debug(`[${pollerName}] Lock held by ${cursor.locked_by}, expires at ${cursor.lock_expires_at}`);
          return null;
        }

        const updated = await tx[cursorModel].update({
          where: { poller_name: pollerName },
          data: {
            locked_by: instanceId,
            lock_expires_at: new Date(now.getTime() + lockTtlMs),
            last_started_at: now,
          },
        });

        logger.debug(`[${pollerName}] Lock acquired by ${instanceId}`);
        return updated;
      });
    } catch (error) {
      logger.error(`[${pollerName}] Failed to acquire lock:`, error);
      throw error;
    }
  }

  async function releaseLock(prisma, pollerName, success, error = null, processedCount = 0) {
    try {
      const updateData = {
        locked_by: null,
        lock_expires_at: null,
      };

      if (success) {
        updateData.last_succeeded_at = new Date();
        updateData.last_run_count = processedCount;
      } else {
        updateData.last_failed_at = new Date();
        updateData.last_error = error ? error.message.substring(0, 500) : 'Unknown error';
      }

      await prisma[cursorModel].update({
        where: { poller_name: pollerName },
        data: updateData,
      });

      logger.debug(`[${pollerName}] Lock released. Success: ${success}, Processed: ${processedCount}`);
    } catch (lockError) {
      logger.error(`[${pollerName}] Failed to release lock:`, lockError);
    }
  }

  /**
   * Update cursor to the last successfully processed record.
   *
   * @param {Object} tx - Prisma transaction client
   * @param {string} pollerName - Poller name
   * @param {Date} updatedAt - `updated_at` timestamp of the last processed record
   * @param {string|number|null} recordId - ID value to store in idField (or null)
   */
  async function updateCursor(tx, pollerName, updatedAt, recordId) {
    if (!updatedAt) {
      throw new Error(`Invalid cursor update args for ${pollerName}: updatedAt=${updatedAt}`);
    }

    await tx[cursorModel].update({
      where: { poller_name: pollerName },
      data: {
        last_updated_at: updatedAt,
        [idField]: recordId,
      },
    });

    logger.debug(`[${pollerName}] Cursor updated to updatedAt=${updatedAt.toISOString()}, ${idField}=${recordId}`);
  }

  async function getCursor(prisma, pollerName) {
    const cursor = await prisma[cursorModel].findUnique({
      where: { poller_name: pollerName },
    });

    if (!cursor) {
      throw new Error(`Cursor not found for poller: ${pollerName}`);
    }

    return cursor;
  }

  async function initializeCursor(prisma, pollerName, initialUpdatedAt = null, initialId = null) {
    await prisma[cursorModel].upsert({
      where: { poller_name: pollerName },
      create: {
        poller_name: pollerName,
        last_updated_at: initialUpdatedAt,
        [idField]: initialId,
      },
      update: {
        last_updated_at: initialUpdatedAt,
        [idField]: initialId,
        locked_by: null,
        lock_expires_at: null,
      },
    });

    logger.info(`[${pollerName}] Cursor initialized`);
  }

  return {
    acquireLock,
    releaseLock,
    updateCursor,
    getCursor,
    initializeCursor,
    INSTANCE_ID,
  };
}

// Pre-built managers for convenience
const cmgCursorManager = createCursorManager({
  cursorModel: 'cmg_sync_cursor',
  idField: 'last_cmg_objectid',
});

const xeniumCursorManager = createCursorManager({
  cursorModel: 'xenium_sync_cursor',
  idField: 'last_xenium_id',
});

module.exports = {
  createCursorManager,
  cmgCursorManager,
  xeniumCursorManager,
  // Named exports for backward compatibility with CMG poller base_poller.js
  acquireLock: cmgCursorManager.acquireLock,
  releaseLock: cmgCursorManager.releaseLock,
  updateCursor: cmgCursorManager.updateCursor,
  getCursor: cmgCursorManager.getCursor,
  initializeCursor: cmgCursorManager.initializeCursor,
  INSTANCE_ID,
  DEFAULT_LOCK_TTL_MS,
};
