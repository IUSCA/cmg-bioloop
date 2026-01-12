/**
 * CMG Sync - Process Lock Manager
 *
 * Prevents multiple instances of the same process from running
 * Uses database-level locking for distributed systems
 */

const logger = require('../logger');
const os = require('os');

const INSTANCE_ID = `${os.hostname()}-${process.pid}`;
const DEFAULT_LOCK_TTL_MS = 300000; // 5 minutes (for big-bang)
const POLLER_LOCK_TTL_MS = 120000; // 2 minutes (for poller process)

/**
 * Acquire process-level lock
 *
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} processName - Name of the process ('bigbang' or 'poller')
 * @param {number} lockTtlMs - Lock TTL in milliseconds
 * @returns {boolean} True if lock acquired, false if already locked
 */
async function acquireProcessLock(prisma, processName, lockTtlMs = DEFAULT_LOCK_TTL_MS) {
  try {
    return await prisma.$transaction(async (tx) => {
      const lock = await tx.cmg_sync_process_lock.findUnique({
        where: { process_name: processName },
      });

      const now = new Date();

      if (lock) {
        // Check if lock is held by another instance
        const canAcquire = !lock.locked_by || (lock.lock_expires_at && lock.lock_expires_at < now);

        if (!canAcquire) {
          logger.warn(
            `[${processName}] Already running (locked by ${lock.locked_by}, expires: ${lock.lock_expires_at})`,
          );
          return false;
        }

        // Lock expired or released, acquire it
        await tx.cmg_sync_process_lock.update({
          where: { process_name: processName },
          data: {
            locked_by: INSTANCE_ID,
            lock_expires_at: new Date(now.getTime() + lockTtlMs),
            last_started_at: now,
          },
        });
      } else {
        // Create lock
        await tx.cmg_sync_process_lock.create({
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

/**
 * Release process-level lock
 *
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} processName - Name of the process
 */
async function releaseProcessLock(prisma, processName) {
  try {
    await prisma.cmg_sync_process_lock.update({
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
    // Don't throw - we want to continue even if lock release fails
  }
}

/**
 * Extend process lock (for long-running operations)
 *
 * @param {PrismaClient} prisma - Prisma client instance
 * @param {string} processName - Name of the process
 * @param {number} lockTtlMs - Lock TTL in milliseconds
 */
async function extendProcessLock(
  prisma,
  processName,
  lockTtlMs = DEFAULT_LOCK_TTL_MS,
) {
  try {
    const now = new Date();
    await prisma.cmg_sync_process_lock.update({
      where: {
        process_name: processName,
        locked_by: INSTANCE_ID, // Only extend our own lock
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

/**
 * Force release all process locks (emergency use only)
 *
 * @param {PrismaClient} prisma - Prisma client instance
 * @returns {number} Number of locks released
 */
async function forceReleaseAllProcessLocks(prisma) {
  const result = await prisma.cmg_sync_process_lock.updateMany({
    where: {
      locked_by: { not: null },
    },
    data: {
      locked_by: null,
      lock_expires_at: null,
    },
  });

  logger.warn(`[Process Lock Manager] Force released ${result.count} process locks`);
  return result.count;
}

module.exports = {
  acquireProcessLock,
  releaseProcessLock,
  extendProcessLock,
  forceReleaseAllProcessLocks,
  DEFAULT_LOCK_TTL_MS,
  POLLER_LOCK_TTL_MS,
  INSTANCE_ID,
};
