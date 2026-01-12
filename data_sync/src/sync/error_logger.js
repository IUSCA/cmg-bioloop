/**
 * CMG Sync - Error Logger
 * 
 * Comprehensive error logging for sync failures
 * Logs to JSONL file with full context for debugging
 */

const fs = require('fs').promises;
const path = require('path');
const config = require('config');
const logger = require('../logger');

const LOG_DIR = path.join(config.get('data_root'), 'logs');
const ERROR_LOG_FILE = path.join(LOG_DIR, 'cmg_sync_errors.jsonl');

/**
 * Ensure log directory exists
 */
async function ensureLogDirectory() {
  try {
    await fs.mkdir(LOG_DIR, { recursive: true });
  } catch (error) {
    // Directory might already exist, ignore
  }
}

/**
 * Log a sync error with full context
 * 
 * @param {Object} options
 * @param {string} options.poller - Poller name
 * @param {string} options.operation - Operation being performed
 * @param {string} options.cmgCollection - CMG collection name
 * @param {string} options.cmgId - CMG document _id
 * @param {Object} options.cmgDocument - Full CMG document
 * @param {Error} options.error - Error object
 * @param {Object} options.prismaQuery - Prisma query that failed (optional)
 */
async function logSyncError({
  poller,
  operation,
  cmgCollection,
  cmgId,
  cmgDocument,
  error,
  prismaQuery = null
}) {
  const errorEntry = {
    timestamp: new Date().toISOString(),
    poller,
    operation,
    cmg_collection: cmgCollection,
    cmg_id: cmgId,
    error: {
      message: error.message,
      code: error.code || null,
      meta: error.meta || null,
      stack: error.stack
    },
    cmg_document: cmgDocument
  };

  // Add Prisma query details if provided
  if (prismaQuery) {
    errorEntry.prisma_query = prismaQuery;
  }

  try {
    await ensureLogDirectory();
    
    // Append to JSONL file (one JSON object per line)
    const logLine = JSON.stringify(errorEntry) + '\n';
    await fs.appendFile(ERROR_LOG_FILE, logLine, 'utf8');
    
    // Also log to console/winston for immediate visibility
    logger.error(`[${poller}] Sync error:`, {
      cmg_id: cmgId,
      error_code: error.code,
      error_message: error.message,
      log_file: ERROR_LOG_FILE
    });
  } catch (logError) {
    // If we can't write to log file, at least log to console
    logger.error(`[${poller}] Failed to write error log:`, logError);
    logger.error(`[${poller}] Original sync error:`, error);
  }
}

/**
 * Read recent sync errors
 * 
 * @param {Object} options
 * @param {number} options.limit - Max number of errors to return
 * @param {string} options.poller - Filter by poller name (optional)
 * @returns {Array<Object>} Array of error entries
 */
async function getRecentErrors({ limit = 100, poller = null } = {}) {
  try {
    const content = await fs.readFile(ERROR_LOG_FILE, 'utf8');
    const lines = content.trim().split('\n');
    
    let errors = lines
      .map(line => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(entry => entry !== null);
    
    // Filter by poller if specified
    if (poller) {
      errors = errors.filter(entry => entry.poller === poller);
    }
    
    // Return most recent first
    return errors.reverse().slice(0, limit);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return []; // File doesn't exist yet
    }
    throw error;
  }
}

/**
 * Get error statistics
 * 
 * @returns {Object} Error stats by poller
 */
async function getErrorStats() {
  try {
    const errors = await getRecentErrors({ limit: 10000 });
    
    const stats = {};
    
    for (const error of errors) {
      if (!stats[error.poller]) {
        stats[error.poller] = {
          total: 0,
          by_code: {},
          last_error: null
        };
      }
      
      stats[error.poller].total++;
      
      const code = error.error.code || 'UNKNOWN';
      stats[error.poller].by_code[code] = (stats[error.poller].by_code[code] || 0) + 1;
      
      if (!stats[error.poller].last_error || 
          error.timestamp > stats[error.poller].last_error.timestamp) {
        stats[error.poller].last_error = {
          timestamp: error.timestamp,
          message: error.error.message,
          cmg_id: error.cmg_id
        };
      }
    }
    
    return stats;
  } catch (error) {
    logger.error('[Error Logger] Failed to get error stats:', error);
    return {};
  }
}

/**
 * Clear old errors (keep last N days)
 * 
 * @param {number} daysToKeep - Number of days of errors to keep
 */
async function clearOldErrors(daysToKeep = 30) {
  try {
    const errors = await getRecentErrors({ limit: 100000 });
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
    
    const recentErrors = errors.filter(error => {
      return new Date(error.timestamp) > cutoffDate;
    });
    
    // Rewrite file with only recent errors
    const content = recentErrors.map(e => JSON.stringify(e)).join('\n') + '\n';
    await fs.writeFile(ERROR_LOG_FILE, content, 'utf8');
    
    logger.info(`[Error Logger] Cleared errors older than ${daysToKeep} days. Kept ${recentErrors.length} errors.`);
    
    return {
      removed: errors.length - recentErrors.length,
      kept: recentErrors.length
    };
  } catch (error) {
    logger.error('[Error Logger] Failed to clear old errors:', error);
    throw error;
  }
}

module.exports = {
  logSyncError,
  getRecentErrors,
  getErrorStats,
  clearOldErrors
};

