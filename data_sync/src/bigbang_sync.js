#!/usr/bin/env node

/**
 * CMG to Bioloop Big-Bang Synchronization Script
 *
 * One-time initial population of all CMG data into Bioloop.
 *
 * Usage:
 *   node src/bigbang_sync.js [options]
 *
 * Options:
 *   --cmg-uri=<uri>     MongoDB connection string for CMG database
 *   --skip-sessions     Skip genome browser session conversion
 *   --clear-locks       Clear any existing process locks before starting
 *   --help, -h          Show help message
 *
 * Environment Variables (alternative to --cmg-uri):
 *   CMG_MONGO_HOST, CMG_MONGO_PORT, CMG_MONGO_DB, CMG_MONGO_USERNAME,
 *   CMG_MONGO_PASSWORD
 *
 * Examples:
 *   # Using environment variables (via config system)
 *   node src/bigbang_sync.js
 *
 *   # Using command-line URI
 *   node src/bigbang_sync.js --cmg-uri="mongodb://user:pass@host:27017/cmg"
 *
 *   # Skip sessions and clear stale locks
 *   node src/bigbang_sync.js --skip-sessions --clear-locks
 *
 * Order of operations:
 * 1. Create roles
 * 2. Create CMG system user
 * 3. Populate pipeline definitions (cmd_line_programs, conversion_definitions, arguments)
 * 4. Convert users
 * 5. Convert datasets (RAW_DATA and DATA_PRODUCT)
 * 6. Convert dataset audit logs (from events)
 * 7. Convert dataset import logs (CMG upload history)
 * 8. Convert dataset hierarchies
 * 9. Convert projects
 * 10. Convert conversions
 * 11. Convert sessions (optional - many will be skipped)
 * 12. Initialize cursors for pollers
 */

require('module-alias/register');
const config = require('config');
// eslint-disable-next-line import/no-unresolved
const { MongoClient } = require('mongodb');
const { PrismaClient } = require('@prisma/client');
const logger = require('./logger');
const { setDatabaseUrl } = require('./utils/db_config');

// Bigbang modules
const { createRoles, createCMGUser, populatePipelineDefinitions } = require('./sync/bigbang/seed_constants');
const { syncUsers } = require('./sync/bigbang/sync_users');
const { syncAllDatasets } = require('./sync/bigbang/sync_datasets');
const { syncAuditLogs } = require('./sync/bigbang/sync_audit_logs');
const { syncImportLogs } = require('./sync/bigbang/sync_import_logs');
const { syncDatasetHierarchies } = require('./sync/bigbang/sync_dataset_hierarchies');
const { syncProjects } = require('./sync/bigbang/sync_projects');
const { syncConversions } = require('./sync/bigbang/sync_conversions');
const { syncSessions } = require('./sync/bigbang/sync_sessions');
const { initializeCursors } = require('./sync/bigbang/initialize_cursors');
const {
  acquireProcessLock,
  releaseProcessLock,
  extendProcessLock,
  forceReleaseAllProcessLocks,
  DEFAULT_LOCK_TTL_MS,
} = require('./sync/process_lock_manager');

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};

  // eslint-disable-next-line no-restricted-syntax
  for (const arg of args) {
    if (arg.startsWith('--cmg-uri=')) {
      [, options.cmgUri] = arg.split('=');
    } else if (arg.startsWith('--target-db=')) {
      [, options.targetDb] = arg.split('=');
    } else if (arg === '--skip-sessions') {
      options.skipSessions = true;
    } else if (arg === '--clear-locks') {
      options.clearLocks = true;
    } else if (arg === '--help' || arg === '-h') {
      // eslint-disable-next-line no-console
      console.log(`
Usage: node src/bigbang_sync.js [options]

Options:
  --cmg-uri=<uri>        MongoDB connection string for CMG database
                         Format: mongodb://username:password@host:port/database
  
  --target-db=<target>   Target database: sandbox (default), app, or custom
                         - sandbox: Use data_sync's isolated PostgreSQL
                         - app: Read from ../api/.env and use app's database
                         - custom: Use DATABASE_URL from environment
                         
  --skip-sessions        Skip genome browser session conversion (recommended for initial run)
  
  --clear-locks          Clear any existing process locks before starting
  
  --help, -h             Show this help message

Environment Variables (alternative to --cmg-uri):
  CMG_MONGO_HOST, CMG_MONGO_PORT, CMG_MONGO_DB, CMG_MONGO_USERNAME,
  CMG_MONGO_PASSWORD

Examples:
  # Using command-line URI (sandbox DB)
  node src/bigbang_sync.js --cmg-uri="mongodb://cmg:pass@localhost:27017/cmg"
  
  # Using environment variables (via config system)
  node src/bigbang_sync.js
  
  # Target app's production database
  node src/bigbang_sync.js --target-db=app
  
  # Skip sessions (recommended for first run)
  node src/bigbang_sync.js --skip-sessions
  
  # Sync to app DB with all options
  node src/bigbang_sync.js --target-db=app --skip-sessions --clear-locks
`);
      process.exit(0);
    }
  }

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
 * Build CMG MongoDB connection URI from config or command line
 */
function buildMongoUri(cmdLineUri) {
  if (cmdLineUri) {
    return cmdLineUri;
  }

  // Get from config system
  const dbConfig = config.get('cmg_mongodb');

  const {
    host, port, database, username, password,
  } = dbConfig;

  if (!host || !database) {
    const errorMsg = 'CMG MongoDB configuration missing. '
      + 'Please set CMG_MONGO_* environment variables or use --cmg-uri flag.';
    throw new Error(errorMsg);
  }

  let uri = 'mongodb://';

  if (username && password) {
    uri += `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`;
  }

  uri += `${host}:${port}/${database}`;

  return uri;
}

/**
 * Main big-bang synchronization function
 */
async function main() {
  const startTime = Date.now();
  const options = parseArgs();

  logger.info('='.repeat(80));
  logger.info('CMG to Bioloop Big-Bang Synchronization');
  logger.info('='.repeat(80));

  let cmgClient;
  let prisma;
  let lockAcquired = false;
  let lockExtender;

  try {
    // Set target database URL based on --target-db flag
    const targetDb = options.targetDb || 'sandbox';
    const databaseUrl = setDatabaseUrl(targetDb);
    logger.info(`[OK] Target database: ${targetDb}`);
    logger.info(`[OK] Database URL: ${sanitizeUri(databaseUrl)}`);

    // Create dedicated Prisma instance for this script
    prisma = new PrismaClient();
    logger.info('[OK] Prisma client created');

    // Test Bioloop PostgreSQL connection
    await prisma.$connect();
    logger.info('[OK] Successfully connected to Bioloop PostgreSQL');

    // Clear existing locks if requested
    if (options.clearLocks) {
      logger.warn('[CLEAR-LOCKS] Clearing all existing process locks...');
      const count = await forceReleaseAllProcessLocks(prisma);
      logger.warn(`[CLEAR-LOCKS] Released ${count} process lock(s)`);
    }

    // Acquire process lock BEFORE starting migration
    lockAcquired = await acquireProcessLock(prisma, 'bigbang', DEFAULT_LOCK_TTL_MS);

    if (!lockAcquired) {
      logger.error('');
      logger.error('='.repeat(80));
      logger.error('[FAILED] Another big-bang process is already running');
      logger.error('='.repeat(80));
      logger.error(
        'If you are certain no other instance is running, you can restart with:',
      );
      logger.error('  node src/bigbang_sync.js --clear-locks');
      logger.error('');
      logger.error('Or manually clear the lock via Prisma:');
      logger.error(
        '  prisma.cmg_sync_process_lock.update({ where: { process_name: "bigbang" },',
      );
      logger.error('    data: { locked_by: null, lock_expires_at: null } })');
      logger.error('');
      process.exit(1);
    }

    // Build connection URIs
    const cmgUri = buildMongoUri(options.cmgUri);

    logger.info('Connecting to databases...');
    logger.info(`CMG MongoDB: ${cmgUri.replace(/\/\/.*@/, '//<credentials>@')}`);

    // Connect to MongoDB databases
    cmgClient = new MongoClient(cmgUri);
    await cmgClient.connect();
    const cmgDb = cmgClient.db();
    logger.info('[OK] Successfully connected to CMG MongoDB');
    logger.info('');

    // Setup periodic lock extension (every 2 minutes)
    lockExtender = setInterval(async () => {
      try {
        await extendProcessLock(prisma, 'bigbang', DEFAULT_LOCK_TTL_MS);
      } catch (error) {
        logger.error('Failed to extend process lock:', error);
        clearInterval(lockExtender);
      }
    }, 120000); // Every 2 minutes

    // Execute migration in order
    logger.info('Starting big-bang migration...');
    logger.info('');

    // 1. Create roles
    logger.info('[1/12] Creating roles...');
    await createRoles(prisma);

    // 2. Create CMG system user
    logger.info('[2/12] Creating CMG system user...');
    const cmgUserId = await createCMGUser(prisma);

    // 3. Populate pipeline definitions
    logger.info('[3/12] Populating pipeline definitions...');
    await populatePipelineDefinitions(prisma, cmgUserId);

    // 4. Convert users
    logger.info('[4/12] Converting users...');
    await syncUsers(prisma, cmgDb);

    // 5. Convert datasets
    logger.info('[5/12] Converting datasets...');
    await syncAllDatasets(prisma, cmgDb);

    // 6. Convert dataset audit logs
    logger.info('[6/12] Converting dataset audit logs...');
    await syncAuditLogs(prisma, cmgDb, cmgUserId);

    // 7. Convert dataset import logs (CMG upload history -> Bioloop import logs)
    logger.info('[7/12] Converting CMG upload history to import logs...');
    await syncImportLogs(prisma, cmgDb, cmgUserId);

    // 8. Convert dataset hierarchies
    logger.info('[8/12] Converting dataset hierarchies...');
    await syncDatasetHierarchies(prisma, cmgDb);

    // 9. Convert projects
    logger.info('[9/12] Converting projects...');
    await syncProjects(prisma, cmgDb);

    // 10. Convert conversions
    logger.info('[10/12] Converting conversions...');
    await syncConversions(prisma, cmgDb);

    // 11. Convert sessions (optional)
    if (options.skipSessions) {
      logger.info('[11/12] Skipping sessions (--skip-sessions flag provided)');
    } else {
      logger.info('[11/12] Converting genome browser sessions...');
      await syncSessions(prisma, cmgDb);
    }

    // 12. Initialize cursors
    logger.info('[12/12] Initializing poller cursors...');
    await initializeCursors(prisma, cmgDb);

    // Clear lock extender
    if (lockExtender) {
      clearInterval(lockExtender);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`[SUCCESS] Big-bang migration completed successfully in ${duration}s`);
    logger.info('='.repeat(80));
    logger.info('');
    logger.info('Next steps:');
    logger.info('  1. Verify data integrity in Bioloop database');
    logger.info('  2. Start the poller sync script: node src/poller_sync.js');
    logger.info('  3. Monitor logs for any sync issues');
    logger.info('');
  } catch (error) {
    // Clear lock extender
    if (lockExtender) {
      clearInterval(lockExtender);
    }

    logger.error('');
    logger.error('='.repeat(80));
    logger.error('[FAILED] Big-bang migration FAILED');
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
    logger.error('NOTE: Data inserted before the error occurred has been retained in the database.');
    logger.error('The script is idempotent - you can re-run it after fixing the error.');
    logger.error('');

    process.exit(1);
  } finally {
    // Release process lock
    if (lockAcquired && prisma) {
      await releaseProcessLock(prisma, 'bigbang');
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
