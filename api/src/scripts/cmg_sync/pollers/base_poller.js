const { ObjectId } = require('mongodb');
const logger = require('@/services/logger');
const { acquireLock, releaseLock, updateCursor } = require('../cursor_manager');
const { logSyncError } = require('../error_logger');

/**
 * Base Poller Class
 * 
 * Abstract base class for all CMG sync pollers.
 * Provides common polling logic, lock management, cursor tracking, and error handling.
 */
class BasePoller {
  /**
   * @param {string} pollerName - Unique name of the poller (e.g., 'user_roles')
   * @param {Object} prisma - Prisma client instance
   * @param {Object} cmgDb - CMG MongoDB database instance
   * @param {Object} options - Poller configuration
   * @param {number} options.pollIntervalMs - Time between polls in milliseconds (default: 10000)
   * @param {number} options.batchSize - Number of documents to process per batch (default: 200)
   * @param {number} options.lockTtlMs - Lock time-to-live in milliseconds (default: 60000)
   * @param {number} options.timeBudgetMs - Max time allowed per polling round (default: 30000)
   */
  constructor(pollerName, prisma, cmgDb, options = {}) {
    if (new.target === BasePoller) {
      throw new Error('BasePoller is abstract and cannot be instantiated directly');
    }
    
    this.pollerName = pollerName;
    this.prisma = prisma;
    this.cmgDb = cmgDb;
    
    // Configuration
    this.pollIntervalMs = options.pollIntervalMs || 10000; // 10 seconds
    this.batchSize = options.batchSize || 200;
    this.lockTtlMs = options.lockTtlMs || 60000; // 60 seconds
    this.timeBudgetMs = options.timeBudgetMs || 30000; // 30 seconds
    
    // State
    this.isRunning = false;
    this.instanceId = `${pollerName}-${process.pid}-${Date.now()}`;
    this.intervalHandle = null;
    
    // Metrics
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
   * Get the CMG collection name to poll
   * Must be implemented by subclass
   * 
   * @returns {string} Collection name (e.g., 'users', 'projects')
   */
  getCollectionName() {
    throw new Error('getCollectionName() must be implemented by subclass');
  }
  
  /**
   * Process a single CMG document
   * Must be implemented by subclass
   * 
   * @param {Object} cmgDoc - CMG document
   * @param {Object} tx - Prisma transaction client
   * @returns {Promise<void>}
   */
  async processDocument(cmgDoc, tx) {
    throw new Error('processDocument() must be implemented by subclass');
  }
  
  /**
   * Build MongoDB query for fetching documents
   * Can be overridden by subclass to customize query
   * 
   * @param {Object} cursor - Current cursor state
   * @param {Date} roundEnd - End timestamp for bounded window
   * @returns {Object} MongoDB query object
   */
  buildQuery(cursor, roundEnd) {
    const query = {};
    
    if (cursor.last_updated_at) {
      // Cursor-based query: fetch documents updated after last sync
      query.$or = [
        { updatedAt: { $gt: cursor.last_updated_at } },
        {
          updatedAt: cursor.last_updated_at,
          _id: { $gt: new ObjectId(cursor.last_cmg_objectid) }
        }
      ];
    }
    
    // Bounded window to prevent processing documents that are being actively updated
    if (roundEnd) {
      if (!query.updatedAt) {
        query.updatedAt = {};
      }
      query.updatedAt.$lte = roundEnd;
    }
    
    return query;
  }
  
  /**
   * Start the poller
   */
  start() {
    if (this.isRunning) {
      logger.warn(`[${this.pollerName}] Already running`);
      return;
    }
    
    this.isRunning = true;
    logger.info(`[${this.pollerName}] Starting poller (interval: ${this.pollIntervalMs}ms)`);
    
    // Run immediately, then on interval
    this.poll().catch(error => {
      logger.error(`[${this.pollerName}] Initial poll failed:`, error);
    });
    
    this.intervalHandle = setInterval(() => {
      this.poll().catch(error => {
        logger.error(`[${this.pollerName}] Poll failed:`, error);
      });
    }, this.pollIntervalMs);
  }
  
  /**
   * Stop the poller
   */
  stop() {
    if (!this.isRunning) {
      return;
    }
    
    logger.info(`[${this.pollerName}] Stopping poller`);
    
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }
    
    this.isRunning = false;
  }
  
  /**
   * Single polling round
   */
  async poll() {
    const roundStart = Date.now();
    this.metrics.totalRuns++;
    this.metrics.lastRunTime = new Date();
    
    try {
      // Try to acquire lock
      const cursor = await acquireLock(this.prisma, this.pollerName, this.instanceId, this.lockTtlMs);
      
      if (!cursor) {
        // Lock already held by another instance
        logger.debug(`[${this.pollerName}] Lock held by another instance, skipping`);
        return;
      }
      
      logger.debug(`[${this.pollerName}] Lock acquired, starting sync round`);
      
      try {
        // Fetch and process batch
        const processedCount = await this.syncBatch(cursor);
        
        // Update metrics
        this.metrics.successfulRuns++;
        this.metrics.totalProcessed += processedCount;
        this.metrics.lastSuccessTime = new Date();
        
        const duration = Date.now() - roundStart;
        logger.info(`[${this.pollerName}] Round complete: ${processedCount} processed in ${duration}ms`);
        
        // Release lock with success status
        await releaseLock(this.prisma, this.pollerName, true, null, processedCount);
        
      } catch (error) {
        // Sync failed, release lock with error
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
  
  /**
   * Sync a batch of documents from CMG
   * 
   * @param {Object} cursor - Current cursor state
   * @returns {Promise<number>} Number of documents processed
   */
  async syncBatch(cursor) {
    const batchStart = Date.now();
    const roundEnd = new Date(batchStart); // Bounded window
    
    // Build query
    const query = this.buildQuery(cursor, roundEnd);
    const collectionName = this.getCollectionName();
    
    // Fetch documents
    const docs = await this.cmgDb.collection(collectionName)
      .find(query)
      .sort({ updatedAt: 1, _id: 1 }) // Ordered by cursor fields
      .limit(this.batchSize)
      .toArray();
    
    if (docs.length === 0) {
      logger.debug(`[${this.pollerName}] No documents to process`);
      return 0;
    }
    
    logger.debug(`[${this.pollerName}] Fetched ${docs.length} documents`);
    
    // Process documents in transaction
    let processedCount = 0;
    let lastDoc = null;
    
    await this.prisma.$transaction(async (tx) => {
      for (const doc of docs) {
        try {
          // Check time budget
          const elapsed = Date.now() - batchStart;
          if (elapsed > this.timeBudgetMs) {
            logger.warn(`[${this.pollerName}] Time budget exceeded, stopping batch at ${processedCount}/${docs.length}`);
            break;
          }
          
          await this.processDocument(doc, tx);
          processedCount++;
          lastDoc = doc;
          
        } catch (docError) {
          // Log individual document error
          await logSyncError({
            poller: this.pollerName,
            operation: 'processDocument',
            cmg_collection: collectionName,
            cmg_id: doc._id.toString(),
            error: docError,
            cmg_document: doc,
          });
          
          logger.warn(`[${this.pollerName}] Failed to process document ${doc._id}: ${docError.message}`);
          
          // Track retry
          await this.trackRetry(tx, doc._id.toString(), docError);
          
          // Continue with next document (don't throw - we want partial batch progress)
        }
      }
      
      // Update cursor to last successfully processed document
      if (lastDoc) {
        await updateCursor(tx, this.pollerName, {
          last_updated_at: lastDoc.updatedAt,
          last_cmg_objectid: lastDoc._id.toString(),
        });
      }
    });
    
    return processedCount;
  }
  
  /**
   * Track failed document in retry table
   * 
   * @param {Object} tx - Prisma transaction client
   * @param {string} cmgId - CMG document _id as string
   * @param {Error} error - Error that occurred
   */
  async trackRetry(tx, cmgId, error) {
    await tx.cmg_sync_retry.upsert({
      where: {
        poller_name_cmg_id: {
          poller_name: this.pollerName,
          cmg_id: cmgId,
        },
      },
      create: {
        poller_name: this.pollerName,
        cmg_id: cmgId,
        failure_count: 1,
        last_error: error.message.substring(0, 500),
        next_retry_at: new Date(Date.now() + 3600000), // 1 hour
      },
      update: {
        failure_count: { increment: 1 },
        last_error: error.message.substring(0, 500),
        next_retry_at: new Date(Date.now() + 3600000),
      },
    });
  }
  
  /**
   * Get poller metrics
   * 
   * @returns {Object} Metrics object
   */
  getMetrics() {
    return {
      pollerName: this.pollerName,
      isRunning: this.isRunning,
      ...this.metrics,
    };
  }
}

module.exports = BasePoller;

