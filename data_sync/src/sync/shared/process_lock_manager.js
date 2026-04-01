/**
 * Sync Process Lock Manager (Generic)
 *
 * Prevents multiple concurrent instances of the same process (bigbang or pollers)
 * from running against the same target database.
 *
 * Accepts a `lockTableModel` parameter so the same logic can be shared between
 * the CMG migration (cmg_sync_process_lock) and the xenium migration
 * (xenium_sync_process_lock).
 *
 * Usage:
 *   const mgr = createProcessLockManager({ lockTableModel: 'cmg_sync_process_lock' });
 *   await mgr.acquireProcessLock(prisma, 'bigbang', DEFAULT_LOCK_TTL_MS);
 */

const logger = require('../../logger');
const os = require('os');

const INSTANCE_ID = `${os.hostname()}-${process.pid}`;
const DEFAULT_LOCK_TTL_MS = 300000; // 5 minutes (for bigbang)
const POLLER_LOCK_TTL_MS = 120000; // 2 minutes (for poller process)

/**
 * Create a process lock manager bound to a specific lock table.
 *
 * @param {Object} config
 * @param {string} config.lockTableModel - Prisma model name for the lock table
 *   (e.g. 'cmg_sync_process_lock' or 'xenium_sync_process_lock')
 * @returns {Object} Lock manager with acquire/release/extend/check/forceRelease methods
 */
function createProcessLockManager({ lockTableModel }) {
  if (!lockTableModel) {
    throw new Error('createProcessLockManager: lockTableModel is required');
  }

  async function acquireProcessLock(prisma, processName, lockTtlMs = DEFAULT_LOCK_TTL_MS) {
    try {
      return await prisma.$transaction(async (tx) => {
        const lock = await tx[lockTableModel].findUnique({
          where: { process_name: processName },
        });

        const now = new Date();

        if (lock) {
          const canAcquire = !lock.locked_by || (lock.lock_expires_at && lock.lock_expires_at < now);

          if (!canAcquire) {
            logger.warn(
              `[${processName}] Already running (locked by ${lock.locked_by}, expires: ${lock.lock_expires_at})`,
            );
            return false;
          }

          await tx[lockTableModel].update({
            where: { process_name: processName },
            data: {
              locked_by: INSTANCE_ID,
              lock_expires_at: new Date(now.getTime() + lockTtlMs),
              last_started_at: now,
            },
          });
        } else {
          await tx[lockTableModel].create({
            data: {
              process_name: processName,
              locked_by: INSTANCE_ID,
              lock_expires_at: new Date(now.getTime() + lockTtlMs),
              last_started_at: now,
            },
          });
        }

        logger.info(`[${processName}] Process lock acquired by ${INSTANCE_ID}`);
        return true;
      });
    } catch (error) {
      logger.error(`[${processName}] Failed to acquire process lock:`, error);
      throw error;
    }
  }

  async function releaseProcessLock(prisma, processName) {
    try {
      await prisma[lockTableModel].update({
        where: { process_name: processName },
        data: {
          locked_by: null,
          lock_expires_at: null,
          last_completed_at: new Date(),
        },
      });

      logger.info(`[${processName}] Process lock released by ${INSTANCE_ID}`);
    } catch (error) {
      logger.error(`[${processName}] Failed to release process lock:`, error);
    }
  }

  async function extendProcessLock(prisma, processName, lockTtlMs = DEFAULT_LOCK_TTL_MS) {
    try {
      const now = new Date();
      await prisma[lockTableModel].update({
        where: {
          process_name: processName,
          locked_by: INSTANCE_ID,
        },
        data: {
          lock_expires_at: new Date(now.getTime() + lockTtlMs),
        },
      });

      logger.debug(
        `[${processName}] Process lock extended to ${new Date(now.getTime() + lockTtlMs).toISOString()}`,
      );
    } catch (error) {
      logger.error(`[${processName}] Failed to extend process lock:`, error);
      throw error;
    }
  }

  async function checkProcessLockStatus(prisma, processName) {
    try {
      const lock = await prisma[lockTableModel].findUnique({
        where: { process_name: processName },
      });

      if (!lock || !lock.locked_by) {
        return null;
      }

      const now = new Date();
      if (lock.lock_expires_at && lock.lock_expires_at < now) {
        return null;
      }

      return {
        locked_by: lock.locked_by,
        lock_expires_at: lock.lock_expires_at,
        last_started_at: lock.last_started_at,
      };
    } catch (error) {
      logger.error(`[${processName}] Failed to check process lock status:`, error);
      return null;
    }
  }

  async function forceReleaseAllProcessLocks(prisma) {
    const result = await prisma[lockTableModel].updateMany({
      where: {
        locked_by: { not: null },
      },
      data: {
        locked_by: null,
        lock_expires_at: null,
      },
    });

    logger.warn(`[Process Lock Manager] Force released ${result.count} process locks (table: ${lockTableModel})`);
    return result.count;
  }

  return {
    acquireProcessLock,
    releaseProcessLock,
    extendProcessLock,
    checkProcessLockStatus,
    forceReleaseAllProcessLocks,
  };
}

// Pre-built managers for convenience
const cmgProcessLockManager = createProcessLockManager({ lockTableModel: 'cmg_sync_process_lock' });
const xeniumProcessLockManager = createProcessLockManager({ lockTableModel: 'xenium_sync_process_lock' });

/**
 * Release every held lock in both CMG and Xenium sync lock tables (same target DB).
 */
async function forceReleaseAllSyncProcessLocks(prisma) {
  const cmgCount = await cmgProcessLockManager.forceReleaseAllProcessLocks(prisma);
  const xeniumCount = await xeniumProcessLockManager.forceReleaseAllProcessLocks(prisma);
  return cmgCount + xeniumCount;
}

module.exports = {
  createProcessLockManager,
  cmgProcessLockManager,
  xeniumProcessLockManager,
  forceReleaseAllSyncProcessLocks,
  DEFAULT_LOCK_TTL_MS,
  POLLER_LOCK_TTL_MS,
  INSTANCE_ID,
};
