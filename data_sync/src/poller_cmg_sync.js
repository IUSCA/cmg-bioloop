#!/usr/bin/env node

/**
 * CMG to Bioloop Incremental Poller Sync Script (CMG source)
 *
 * Continuous synchronization of CMG changes into Bioloop.
 * Runs after big-bang migration to keep data in sync.
 *
 * Usage:
 *   node src/poller_sync.js [options]
 *
 * Options:
 *   --clear-locks    Clear any existing process locks before starting
 *   --help, -h       Show help message
 *
 * Environment Variables (via config system):
 *   CMG_MONGO_HOST, CMG_MONGO_PORT, CMG_MONGO_DB, etc.
 *
 * Examples:
 *   # Normal run (will fail if another instance is running)
 *   node src/poller_sync.js
 *
 *   # Clear stale locks before starting
 *   node src/poller_sync.js --clear-locks
 *
 * Pollers:
 * - user_roles: Syncs user role changes
 * - project_acl: Syncs project access control (users, datasets)
 * - dataset_activity: Syncs dataset paths and lifecycle flags (DEPRECATED - does nothing)
 * - dataset_metadata: Syncs dataset metadata (description only)
 * - project_metadata: Syncs project metadata (name, description, funding, browser_enabled)
 */

require('module-alias/register');
const config = require('config');
// eslint-disable-next-line import/no-unresolved
const { MongoClient } = require('mongodb');
const { PrismaClient } = require('@prisma/client');
const logger = require('./logger');
const { setDatabaseUrl } = require('./utils/db_config');

// Poller classes
const UserRolesPoller = require('./sync/cmg/pollers/user_roles_poller');
const ProjectACLPoller = require('./sync/cmg/pollers/project_acl_poller');
const DatasetActivityPoller = require('./sync/cmg/pollers/dataset_activity_poller');
const DatasetMetadataPoller = require('./sync/cmg/pollers/dataset_metadata_poller');
const ProjectMetadataPoller = require('./sync/cmg/pollers/project_metadata_poller');

// Process lock manager
const {
  acquireProcessLock,
  releaseProcessLock,
  forceReleaseAllProcessLocks,
  POLLER_LOCK_TTL_MS,
} = require('./sync/shared/process_lock_manager');

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    clearLocks: false,
  };

  args.forEach((arg) => {
    if (arg.startsWith('--target-db=')) {
      [, options.targetDb] = arg.split('=');
    } else if (arg === '--clear-locks') {
      options.clearLocks = true;
    } else if (arg === '--help' || arg === '-h') {
      // eslint-disable-next-line no-console
      console.log('');
      // eslint-disable-next-line no-console
      console.log('CMG to Bioloop Incremental Poller Sync');
      // eslint-disable-next-line no-console
      console.log('');
      // eslint-disable-next-line no-console
      console.log('Usage:');
      // eslint-disable-next-line no-console
      console.log('  node src/poller_sync.js [options]');
      // eslint-disable-next-line no-console
      console.log('');
      // eslint-disable-next-line no-console
      console.log('Options:');
      // eslint-disable-next-line no-console
      console.log('  --target-db=<target>   Target database: sandbox (default), app, or custom');
      // eslint-disable-next-line no-console
      console.log('                         - sandbox: Use data_sync\'s isolated PostgreSQL');
      // eslint-disable-next-line no-console
      console.log('                         - app: Read from ../api/.env and use app\'s database');
      // eslint-disable-next-line no-console
      console.log('                         - custom: Use DATABASE_URL from environment');
      // eslint-disable-next-line no-console
      console.log('  --clear-locks          Clear any existing process locks before starting');
      // eslint-disable-next-line no-console
      console.log('  --help, -h             Show this help message');
      // eslint-disable-next-line no-console
      console.log('');
      process.exit(0);
    }
  });

  return options;
}

/**
 * Sanitize URIs in strings to hide credentials
 * Replaces mongodb://user:pass@host with mongodb://<credentials>@host
 * Replaces postgresql://user:pass@host with postgresql://<credentials>@host
 */
function sanitizeUri(str) {
  if (!str) return str;
  if (typeof str !== 'string') {
    str = JSON.stringify(str);
  }
  // Sanitize MongoDB URIs
  str = str.replace(/mongodb:\/\/[^:]+:[^@]+@/g, 'mongodb://<credentials>@');
  // Sanitize PostgreSQL URIs
  str = str.replace(/postgresql:\/\/[^:]+:[^@]+@/g, 'postgresql://<credentials>@');
  return str;
}

/**
 * Build CMG MongoDB connection URI from config
 */
function buildMongoUri() {
  const dbConfig = config.get('cmg_mongodb');

  const {
    host, port, database, username, password,
  } = dbConfig;

  if (!host || !database) {
    throw new Error('CMG MongoDB configuration missing. Please set CMG_MONGO_* environment variables.');
  }

  let uri = 'mongodb://';

  if (username && password) {
    uri += `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`;
  }

  uri += `${host}:${port}/${database}`;

  return uri;
}

/**
 * Setup graceful shutdown handlers
 */
function setupGracefulShutdown(pollers, cmgClient, prisma, lockAcquired) {
  const shutdown = async (signal) => {
    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`Received ${signal}, shutting down gracefully...`);
    logger.info('='.repeat(80));

    // Stop all pollers
    logger.info('Stopping pollers...');
    pollers.forEach((poller) => {
      poller.stop();
      logger.info(`  - ${poller.pollerName} stopped`);
    });

    // Release process lock
    if (lockAcquired && prisma) {
      await releaseProcessLock(prisma, 'poller');
    }

    // Close connections
    if (cmgClient) {
      await cmgClient.close();
      logger.info('Closed CMG MongoDB connection');
    }

    if (prisma) {
      await prisma.$disconnect();
      logger.info('Closed Prisma connection');
    }

    logger.info('');
    logger.info('[OK] Shutdown complete');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // Handle uncaught errors
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception:', sanitizeUri(error.stack || error.message || String(error)));
    shutdown('UNCAUGHT_EXCEPTION');
  });

  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled rejection at:', promise, 'reason:', sanitizeUri(String(reason)));
    shutdown('UNHANDLED_REJECTION');
  });
}

/**
 * Start metrics reporter
 * Logs metrics every 60 seconds
 */
function startMetricsReporter(pollers) {
  setInterval(() => {
    logger.info('');
    logger.info('--- Poller Metrics ---');

    pollers.forEach((poller) => {
      const metrics = poller.getMetrics();
      logger.info(`[${metrics.pollerName}]`);
      logger.info(`  Running: ${metrics.isRunning}`);
      logger.info(`  Total runs: ${metrics.totalRuns}`);
      logger.info(`  Successful: ${metrics.successfulRuns}`);
      logger.info(`  Failed: ${metrics.failedRuns}`);
      logger.info(`  Total processed: ${metrics.totalProcessed}`);
      logger.info(`  Last run: ${metrics.lastRunTime || 'Never'}`);
      logger.info(`  Last success: ${metrics.lastSuccessTime || 'Never'}`);
      if (metrics.lastError) {
        logger.info(`  Last error: ${metrics.lastError}`);
      }
    });

    logger.info('');
  }, 60000); // Every 60 seconds
}

/**
 * Main poller function
 */
async function main() {
  const options = parseArgs();

  logger.info('='.repeat(80));
  logger.info('CMG to Bioloop Incremental Poller Sync');
  logger.info('='.repeat(80));

  let cmgClient;
  let prisma;
  let lockAcquired = false;
  const pollers = [];

  try {
    // Set target database URL based on --target-db flag
    const targetDb = options.targetDb || 'sandbox';
    const databaseUrl = setDatabaseUrl(targetDb);
    logger.info(`[OK] Target database: ${targetDb}`);
    logger.info(`[OK] Database URL: ${sanitizeUri(databaseUrl)}`);

    // Create dedicated Prisma instance for pollers (shared across all pollers)
    prisma = new PrismaClient();
    logger.info('[OK] Prisma client created (shared by all pollers)');

    // Clear existing locks if requested
    if (options.clearLocks) {
      logger.warn('[CLEAR-LOCKS] Clearing all existing process locks...');
      const count = await forceReleaseAllProcessLocks(prisma);
      logger.warn(`[CLEAR-LOCKS] Released ${count} process lock(s)`);
    }

    // Acquire process lock BEFORE starting pollers
    lockAcquired = await acquireProcessLock(prisma, 'poller', POLLER_LOCK_TTL_MS);

    if (!lockAcquired) {
      logger.error('');
      logger.error('='.repeat(80));
      logger.error('[FAILED] Another poller process is already running');
      logger.error('='.repeat(80));
      logger.error(
        'If you are certain no other instance is running, you can restart with:',
      );
      logger.error('  node src/poller_sync.js --clear-locks');
      logger.error('');
      logger.error('Or manually clear the lock via Prisma:');
      logger.error(
        '  prisma.cmg_sync_process_lock.update({ where: { process_name: "poller" },',
      );
      logger.error('    data: { locked_by: null, lock_expires_at: null } })');
      logger.error('');
      process.exit(1);
    }

    // Build connection URIs
    const cmgUri = buildMongoUri();

    logger.info('Connecting to databases...');
    logger.info(`CMG MongoDB: ${cmgUri.replace(/\/\/.*@/, '//<credentials>@')}`);

    // Connect to CMG MongoDB
    cmgClient = new MongoClient(cmgUri, {
      maxPoolSize: 10,
      minPoolSize: 2,
      serverSelectionTimeoutMS: 5000,
    });
    await cmgClient.connect();
    const cmgDb = cmgClient.db();
    logger.info('[OK] Connected to CMG MongoDB');
    logger.info('');

    // Initialize pollers
    logger.info('Initializing pollers...');

    // 1. User Roles Poller
    const userRolesPoller = new UserRolesPoller(prisma, cmgDb);
    pollers.push(userRolesPoller);
    logger.info('  - user_roles (10s interval)');

    // 2. Project ACL Poller
    const projectACLPoller = new ProjectACLPoller(prisma, cmgDb);
    pollers.push(projectACLPoller);
    logger.info('  - project_acl (10s interval)');

    // 3. Dataset Activity Poller
    const datasetActivityPoller = new DatasetActivityPoller(prisma, cmgDb);
    pollers.push(datasetActivityPoller);
    logger.info('  - dataset_activity (10s interval)');

    // 4. Dataset Metadata Poller
    const datasetMetadataPoller = new DatasetMetadataPoller(prisma, cmgDb);
    pollers.push(datasetMetadataPoller);
    logger.info('  - dataset_metadata (15s interval)');

    // 5. Project Metadata Poller
    const projectMetadataPoller = new ProjectMetadataPoller(prisma, cmgDb);
    pollers.push(projectMetadataPoller);
    logger.info('  - project_metadata (20s interval)');

    logger.info('');
    logger.info('Starting pollers...');

    // Start all pollers
    pollers.forEach((poller) => {
      poller.start();
    });

    logger.info('');
    logger.info('='.repeat(80));
    logger.info('[SUCCESS] All pollers started successfully');
    logger.info('='.repeat(80));
    logger.info('');
    logger.info('Press Ctrl+C to stop');
    logger.info('');

    // Setup graceful shutdown
    setupGracefulShutdown(pollers, cmgClient, prisma, lockAcquired);

    // Log metrics periodically
    startMetricsReporter(pollers);

    // Keep process alive
    await new Promise(() => {}); // Never resolves
  } catch (error) {
    logger.error('');
    logger.error('='.repeat(80));
    logger.error('[FAILED] Poller initialization failed');
    logger.error('='.repeat(80));
    logger.error('Error Message:', sanitizeUri(error.message));
    logger.error('Error Name:', error.name);
    if (error.code) {
      logger.error('Error Code:', error.code);
    }
    if (error.stack) {
      logger.error('Stack Trace:');
      logger.error(sanitizeUri(error.stack));
    }
    // Log full error object for debugging (sanitized)
    logger.error('Full Error Object:', sanitizeUri(JSON.stringify(error, Object.getOwnPropertyNames(error), 2)));
    logger.error('');

    // Cleanup
    pollers.forEach((poller) => {
      poller.stop();
    });

    // Release process lock on failure
    if (lockAcquired && prisma) {
      await releaseProcessLock(prisma, 'poller');
    }

    if (cmgClient) {
      await cmgClient.close();
    }

    if (prisma) {
      await prisma.$disconnect();
    }

    process.exit(1);
  }
}

// Run the main function
if (require.main === module) {
  main().catch((error) => {
    logger.error('Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = { main };
