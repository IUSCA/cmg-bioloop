/**
 * Xenium Sync - Error Logger
 *
 * Structured error logging for xenium poller failures.
 * Logs JSONL entries with optional source-row payload snapshots.
 */

const fs = require('fs').promises;
const path = require('path');
const config = require('config');
const logger = require('../logger');

const LOG_DIR = path.join(config.get('data_root'), 'logs');
const ERROR_LOG_FILE = path.join(LOG_DIR, 'xenium_sync_errors.jsonl');

function shouldLogFullPayload() {
  const value = String(process.env.XENIUM_SYNC_LOG_FULL_PAYLOAD || '').toLowerCase();
  return value === '1' || value === 'true' || value === 'yes';
}

function sanitizeForJson(value) {
  if (value === undefined) return null;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (error) {
    return String(value);
  }
}

async function ensureLogDirectory() {
  try {
    await fs.mkdir(LOG_DIR, { recursive: true });
  } catch (error) {
    // Directory may already exist.
  }
}

async function logSyncError({
  poller,
  operation,
  sourceModel,
  xeniumId,
  sourceRow,
  error,
}) {
  const errorEntry = {
    timestamp: new Date().toISOString(),
    poller,
    operation,
    source_model: sourceModel,
    xenium_id: xeniumId,
    error: {
      message: error?.message || String(error),
      code: error?.code || null,
      meta: error?.meta || null,
      stack: error?.stack || null,
    },
  };

  if (shouldLogFullPayload()) {
    errorEntry.source_row = sanitizeForJson(sourceRow);
  }

  try {
    await ensureLogDirectory();
    await fs.appendFile(ERROR_LOG_FILE, `${JSON.stringify(errorEntry)}\n`, 'utf8');
    logger.error(`[${poller}] Sync error`, {
      xenium_id: xeniumId,
      source_model: sourceModel,
      error_code: error?.code || null,
      error_message: error?.message || String(error),
      payload_snapshot: shouldLogFullPayload() ? 'enabled' : 'disabled',
      log_file: ERROR_LOG_FILE,
    });
  } catch (logError) {
    logger.error(`[${poller}] Failed to write xenium error log:`, logError);
    logger.error(`[${poller}] Original sync error:`, error);
  }
}

module.exports = {
  logSyncError,
};

