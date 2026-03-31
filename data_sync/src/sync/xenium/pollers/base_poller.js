/**
 * Xenium Base Poller
 *
 * Abstract base class for all Xenium sync pollers.
 * Polls the Xenium PostgreSQL source database for rows updated since the last
 * cursor position, then applies the changes to cmg-bioloop's PostgreSQL.
 *
 * Key difference from CMG BasePoller:
 *   - Source is PostgreSQL (xeniumPrisma), not MongoDB (cmgDb)
 *   - Cursor uses `updated_at` (DateTime) + `id` (Integer), not MongoDB ObjectId
 *   - Retry table is `xenium_sync_retry` (integer xenium_id field)
 *
 * Fully implemented in Chat 3.
 */

const logger = require('../../../logger');
const { xeniumCursorManager } = require('../../shared/cursor_manager');
const { logSyncError } = require('../error_logger');

const { acquireLock, releaseLock, updateCursor } = xeniumCursorManager;

/**
 * @abstract
 */
class XeniumBasePoller {
  /**
   * @param {string}        pollerName   - Unique poller name (must match xenium_sync_cursor row)
   * @param {PrismaClient}  prisma       - Target cmg-bioloop Prisma client
   * @param {PrismaClient}  xeniumPrisma - Source Xenium Prisma client
   * @param {Object}        options
   * @param {number}        options.pollIntervalMs  - ms between polls (default: 10000)
   * @param {number}        options.batchSize       - rows per poll round (default: 200)
   * @param {number}        options.lockTtlMs       - cursor lock TTL in ms (default: 60000)
   * @param {number}        options.timeBudgetMs    - max ms per round (default: 30000)
   */
  constructor(pollerName, prisma, xeniumPrisma, options = {}) {
    if (new.target === XeniumBasePoller) {
      throw new Error('XeniumBasePoller is abstract and cannot be instantiated directly');
    }

    this.pollerName = pollerName;
    this.prisma = prisma;
    this.xeniumPrisma = xeniumPrisma;

    this.pollIntervalMs = options.pollIntervalMs || 10000;
    this.batchSize = options.batchSize || 200;
    this.lockTtlMs = options.lockTtlMs || 60000;
    this.timeBudgetMs = options.timeBudgetMs || 30000;
    this.transactionTimeoutMs = Number.isInteger(options.transactionTimeoutMs)
      ? options.transactionTimeoutMs
      : null;

    this.isRunning = false;
    this.instanceId = `${pollerName}-${process.pid}-${Date.now()}`;
    this.intervalHandle = null;

    this.metrics = {
      totalRuns: 0,
      successfulRuns: 0,
      failedRuns: 0,
      totalProcessed: 0,
      lastRunTime: null,
      lastSuccessTime: null,
      lastError: null,
    };
  }

  /**
   * Build a Prisma WHERE clause to fetch rows updated since the cursor.
   * Subclass can override for collection-specific filtering.
   *
   * @param {Object} cursor - Current xenium_sync_cursor row
   * @param {Date}   roundEnd - Upper timestamp bound (bounded window)
   * @returns {Object} Prisma where clause
   */
  buildWhere(cursor, roundEnd) {
    const where = {};

    if (cursor.last_updated_at) {
      where.OR = [
        { updated_at: { gt: cursor.last_updated_at } },
        {
          updated_at: cursor.last_updated_at,
          id: { gt: cursor.last_xenium_id },
        },
      ];
    }

    if (roundEnd) {
      where.updated_at = { ...where.updated_at, lte: roundEnd };
    }

    return where;
  }

  /**
   * Process a single xenium source row.
   * Must be implemented by subclass.
   *
   * @param {Object} row - Source row from xenium PostgreSQL
   * @param {Object} tx  - Target Prisma transaction client
   * @returns {Promise<void>}
   */
  // eslint-disable-next-line no-unused-vars
  async processRow(row, tx) {
    throw new Error('processRow() must be implemented by subclass');
  }

  /**
   * Return the xenium source Prisma model name to query (e.g. 'user', 'project').
   * Must be implemented by subclass.
   *
   * @returns {string}
   */
  getSourceModel() {
    throw new Error('getSourceModel() must be implemented by subclass');
  }

  start() {
    if (this.isRunning) {
      logger.warn(`[${this.pollerName}] Already running`);
      return;
    }

    this.isRunning = true;
    logger.info(`[${this.pollerName}] Starting xenium poller (interval: ${this.pollIntervalMs}ms)`);

    this.poll().catch((error) => {
      logger.error(`[${this.pollerName}] Initial poll failed:`, error);
    });

    this.intervalHandle = setInterval(() => {
      this.poll().catch((error) => {
        logger.error(`[${this.pollerName}] Poll failed:`, error);
      });
    }, this.pollIntervalMs);
  }

  stop() {
    if (!this.isRunning) return;

    logger.info(`[${this.pollerName}] Stopping xenium poller`);

    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }

    this.isRunning = false;
  }

  async poll() {
    const roundStart = Date.now();
    this.metrics.totalRuns++;
    this.metrics.lastRunTime = new Date();

    try {
      const cursor = await acquireLock(
        this.prisma, this.pollerName, this.instanceId, this.lockTtlMs,
      );

      if (!cursor) {
        logger.debug(`[${this.pollerName}] Lock held by another instance, skipping`);
        return;
      }

      try {
        const processedCount = await this.syncBatch(cursor);

        this.metrics.successfulRuns++;
        this.metrics.totalProcessed += processedCount;
        this.metrics.lastSuccessTime = new Date();

        const duration = Date.now() - roundStart;
        logger.info(`[${this.pollerName}] Round complete: ${processedCount} rows processed in ${duration}ms`);

        await releaseLock(this.prisma, this.pollerName, true, null, processedCount);
      } catch (error) {
        this.metrics.failedRuns++;
        this.metrics.lastError = error.message;

        logger.error(`[${this.pollerName}] Sync failed:`, error);
        await releaseLock(this.prisma, this.pollerName, false, error, 0);

        throw error;
      }
    } catch (error) {
      logger.error(`[${this.pollerName}] Poll error:`, error);
      throw error;
    }
  }

  async syncBatch(cursor) {
    const batchStart = Date.now();
    const roundEnd = new Date(batchStart);

    const where = this.buildWhere(cursor, roundEnd);
    const sourceModel = this.getSourceModel();

    // Fetch rows from xenium source, ordered by (updated_at ASC, id ASC)
    const rows = await this.xeniumPrisma[sourceModel].findMany({
      where,
      orderBy: [{ updated_at: 'asc' }, { id: 'asc' }],
      take: this.batchSize,
    });

    if (rows.length === 0) {
      logger.debug(`[${this.pollerName}] No rows to process`);
      return 0;
    }

    logger.debug(`[${this.pollerName}] Fetched ${rows.length} rows`);

    let processedCount = 0;
    let lastRow = null;

    const transactionBody = async (tx) => {
      for (const row of rows) {
        const elapsed = Date.now() - batchStart;
        if (elapsed > this.timeBudgetMs) {
          logger.warn(`[${this.pollerName}] Time budget exceeded at ${processedCount}/${rows.length}`);
          break;
        }

        try {
          await this.processRow(row, tx);
          processedCount++;
          lastRow = row;
        } catch (rowError) {
          await logSyncError({
            poller: this.pollerName,
            operation: 'processRow',
            sourceModel,
            xeniumId: row.id,
            sourceRow: row,
            error: rowError,
          });
          logger.warn(`[${this.pollerName}] Failed to process row ${row.id}: ${rowError.message}`);
          await this.trackRetry(tx, row.id, rowError);
        }
      }

      if (lastRow) {
        await updateCursor(tx, this.pollerName, lastRow.updated_at, lastRow.id);
      }
    };

    if (this.transactionTimeoutMs) {
      await this.prisma.$transaction(transactionBody, { timeout: this.transactionTimeoutMs });
    } else {
      await this.prisma.$transaction(transactionBody);
    }

    return processedCount;
  }

  async trackRetry(tx, xeniumId, error) {
    await tx.xenium_sync_retry.upsert({
      where: {
        poller_name_xenium_id: {
          poller_name: this.pollerName,
          xenium_id: xeniumId,
        },
      },
      create: {
        poller_name: this.pollerName,
        xenium_id: xeniumId,
        failure_count: 1,
        last_error: error.message.substring(0, 500),
        next_retry_at: new Date(Date.now() + 3600000),
      },
      update: {
        failure_count: { increment: 1 },
        last_error: error.message.substring(0, 500),
        next_retry_at: new Date(Date.now() + 3600000),
      },
    });
  }

  getMetrics() {
    return {
      pollerName: this.pollerName,
      isRunning: this.isRunning,
      ...this.metrics,
    };
  }
}

module.exports = XeniumBasePoller;
