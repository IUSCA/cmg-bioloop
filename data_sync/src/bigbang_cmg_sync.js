#!/usr/bin/env node

/**
 * CMG to Bioloop Big-Bang Synchronization Script (CMG source)
 *
 * One-time initial population of all CMG data into Bioloop.
 *
 * Usage:
 *   node src/bigbang_sync.js [options]
 *
 * Options:
 *   --cmg-uri=<uri>             MongoDB connection string for CMG database
 *   --target-db=<target>        Target database: sandbox (default), app, or custom
 *   --skip-sessions             Skip genome browser session conversion
 *   --skip-conversion-logs      Skip conversion logs migration (from filesystem)
 *   --clear-locks               Clear any existing process locks before starting
 *   --clear-cmg-target-data     Clear CMG-originated rows from target database before migration
 *   --help, -h                  Show help message
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
 * 4. Seed analysis types (file types from CMG)
 * 5. Seed import sources (production /N/... paths)
 * 6. Populate Bioloop users (from api/*.json files)
 * 7. Convert CMG users (skip if already exists as Bioloop user)
 * 8. Convert datasets (RAW_DATA and DATA_PRODUCT)
 * 9. Convert dataset audit logs (from events)
 * 10. Convert stage/download logs (data_access_log, stage_request_log, dataset_state)
 * 11. Convert events collection (Download Copy events -> data_access_log)
 * 12. Convert dataset import logs (CMG upload history)
 * 13. Convert conversions
 * 14. Convert projects
 * 15. Convert dataset hierarchies
 * 16. Convert conversion logs (from filesystem - production only)
 * 17. Convert sessions (optional - many will be skipped)
 * 18. Initialize cursors for pollers
 */

require('module-alias/register');
const config = require('config');
// eslint-disable-next-line import/no-unresolved
const { MongoClient } = require('mongodb');
const { PrismaClient } = require('@prisma/client');
const originalLogger = require('./logger');
const { setDatabaseUrl } = require('./utils/db_config');

// Wrap logger to count log statements
let logStatementCount = 0;
const logger = {};

['info', 'warn', 'error', 'debug'].forEach((level) => {
  logger[level] = (...args) => {
    logStatementCount += 1;
    originalLogger[level](...args);
  };
});

// Bigbang modules
const {
  createRoles,
  createCMGUser,
  populatePipelineDefinitions,
  seedAnalysisTypes,
  seedImportSources,
  seedAboutContent,
} = require('./sync/cmg/bigbang/seed_constants');
const { populateBioloopUsers } = require('./sync/cmg/bigbang/populate_bioloop_users');
const { syncUsers } = require('./sync/cmg/bigbang/sync_users');
const { syncAllDatasets } = require('./sync/cmg/bigbang/sync_datasets');
const { syncAuditLogs } = require('./sync/cmg/bigbang/sync_audit_logs');
const { syncDownloadStageLogs } = require('./sync/cmg/bigbang/sync_download_stage_logs');
const syncEventsCollection = require('./sync/cmg/bigbang/sync_events_collection');
const { syncImportLogs } = require('./sync/cmg/bigbang/sync_import_logs');
const { syncDatasetHierarchies } = require('./sync/cmg/bigbang/sync_dataset_hierarchies');
const { syncProjects } = require('./sync/cmg/bigbang/sync_projects');
const { syncConversions } = require('./sync/cmg/bigbang/sync_conversions');
const { syncAllConversionLogs } = require('./sync/cmg/bigbang/sync_conversion_logs');
const { syncSessions } = require('./sync/cmg/bigbang/sync_sessions');
const { initializeCursors } = require('./sync/cmg/bigbang/initialize_cursors');
const {
  acquireProcessLock,
  releaseProcessLock,
  extendProcessLock,
  checkProcessLockStatus,
  forceReleaseAllProcessLocks,
  DEFAULT_LOCK_TTL_MS,
} = require('./sync/shared/process_lock_manager');

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
    } else if (arg === '--skip-conversion-logs') {
      options.skipConversionLogs = true;
    } else if (arg === '--clear-locks') {
      options.clearLocks = true;
    } else if (arg === '--clear-cmg-target-data') {
      options.clearCmgTargetData = true;
    } else if (arg === '--help' || arg === '-h') {
      // eslint-disable-next-line no-console
      console.log(`
Usage: node src/bigbang_sync.js [options]

Options:
  --cmg-uri=<uri>        MongoDB connection string for CMG database
                         Format: mongodb://username:password@host:port/database
  
  --target-db=<target>        Target database: sandbox (default), app, or custom
                              - sandbox: Use data_sync's isolated PostgreSQL
                              - app: Read from ../api/.env and use app's database
                              - custom: Use DATABASE_URL from environment
                         
  --skip-sessions             Skip genome browser session conversion (recommended for initial run)
  
  --skip-conversion-logs      Skip conversion logs migration from filesystem (useful for local/dev)
  
  --clear-locks               Clear any existing process locks before starting
  
  --clear-cmg-target-data     Clear CMG-originated rows from target database (keeps xenium rows)
  
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
  
  # Skip conversion logs (useful for local/dev without filesystem access)
  node src/bigbang_sync.js --skip-conversion-logs
  
  # Sync to app DB with all options
  node src/bigbang_sync.js --target-db=app --skip-sessions --clear-locks
  
  # Clear CMG-originated target rows and run fresh CMG migration
  node src/bigbang_sync.js --clear-cmg-target-data --skip-sessions --skip-conversion-logs
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
 * Clear CMG-originated data from target database (keeps schema intact)
 * @param {PrismaClient} prisma - Prisma client instance
 */
async function clearCmgTargetData(prisma) {
  logger.warn('='.repeat(80));
  logger.warn('CLEARING CMG-ORIGINATED TARGET DATA');
  logger.warn('='.repeat(80));
  logger.warn('This will delete CMG-originated business rows and CMG sync cursors/retries.');
  logger.warn('Xenium rows and schema are preserved.');
  logger.warn('');

  try {
    const cmgConversionIds = (await prisma.conversion.findMany({
      where: { cmg_id: { not: null } },
      select: { id: true },
    })).map((row) => row.id);

    const cmgDatasetIds = (await prisma.dataset.findMany({
      where: { cmg_id: { not: null } },
      select: { id: true },
    })).map((row) => row.id);

    if (cmgConversionIds.length > 0) {
      await prisma.argument_value.deleteMany({
        where: { conversion_id: { in: cmgConversionIds } },
      });
      await prisma.process_request.deleteMany({
        where: { conversion_id: { in: cmgConversionIds } },
      });
    }

    if (cmgDatasetIds.length > 0) {
      await prisma.data_access_log.deleteMany({
        where: { dataset_id: { in: cmgDatasetIds } },
      });
      await prisma.stage_request_log.deleteMany({
        where: { dataset_id: { in: cmgDatasetIds } },
      });
    }

    await prisma.project.deleteMany({ where: { cmg_id: { not: null } } });
    await prisma.genome_browser_session.deleteMany({ where: { cmg_id: { not: null } } });
    await prisma.conversion.deleteMany({ where: { cmg_id: { not: null } } });
    await prisma.dataset_import_log.deleteMany({ where: { cmg_id: { not: null } } });
    await prisma.dataset.deleteMany({ where: { cmg_id: { not: null } } });
    await prisma.user.deleteMany({
      where: {
        cmg_id: { not: null },
        username: { not: 'cmguser' },
      },
    });

    await prisma.cmg_sync_retry.deleteMany({});
    await prisma.cmg_sync_cursor.deleteMany({});

    logger.info('CMG-originated target data cleared successfully');
    logger.info('');
  } catch (error) {
    logger.error('Failed to clear CMG target rows:', error.message);
    throw error;
  }
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

    // Check if pollers are running
    const pollerLockStatus = await checkProcessLockStatus(prisma, 'poller');
    if (pollerLockStatus) {
      logger.warn('');
      logger.warn('='.repeat(80));
      logger.warn('⚠️  WARNING: Continuous sync pollers are currently running!');
      logger.warn('='.repeat(80));
      logger.warn(`Locked by: ${pollerLockStatus.locked_by}`);
      logger.warn(`Started at: ${pollerLockStatus.last_started_at}`);
      logger.warn(`Lock expires: ${pollerLockStatus.lock_expires_at}`);
      logger.warn('');
      logger.warn('Running bigbang while pollers are active can cause:');
      logger.warn('  • Data inconsistencies');
      logger.warn('  • Race conditions');
      logger.warn('  • Duplicate records');
      logger.warn('');
      logger.warn('RECOMMENDED: Stop pollers before running bigbang migration.');
      logger.warn('');
      logger.warn('To stop pollers, you have two options:');
      logger.warn('  1. Use --clear-locks flag to force release the lock (if poller crashed)');
      logger.warn('  2. Gracefully stop the poller process (preferred):');
      logger.warn('     - Find process: ps aux | grep poller_sync');
      logger.warn('     - Kill gracefully: kill -SIGTERM <PID>');
      logger.warn('');
      logger.error('Exiting to prevent data corruption. Fix the issue and try again.');
      logger.error('');
      process.exit(1);
    }

    if (options.clearCmgTargetData) {
      await clearCmgTargetData(prisma);
      logger.info('[CLEAR-CMG-DATA] Clearing CMG process locks for fresh start...');
      await forceReleaseAllProcessLocks(prisma);
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
    logger.info('[1/18] Creating roles...');
    await createRoles(prisma);

    // 2. Create CMG system user
    logger.info('[2/18] Creating CMG system user...');
    const cmgUserId = await createCMGUser(prisma);

    // 3. Populate pipeline definitions
    logger.info('[3/18] Populating pipeline definitions...');
    await populatePipelineDefinitions(prisma, cmgUserId);

    // 4. Seed analysis types
    logger.info('[4/18] Seeding analysis types...');
    await seedAnalysisTypes(prisma);

    // 5. Seed import sources and about content
    logger.info('[5/18] Seeding import sources and About content...');
    await seedImportSources(prisma);
    await seedAboutContent(prisma, cmgUserId);

    // 6. Populate Bioloop users (from JSON files)
    logger.info('[6/18] Populating Bioloop users from JSON files...');
    await populateBioloopUsers(prisma);

    // 7. Convert CMG users
    logger.info('[7/18] Converting CMG users...');
    await syncUsers(prisma, cmgDb);

    // 8. Convert datasets
    logger.info('[8/18] Converting datasets...');
    await syncAllDatasets(prisma, cmgDb);

    // 9. Convert dataset audit logs
    logger.info('[9/18] Converting dataset audit logs...');
    await syncAuditLogs(prisma, cmgDb, cmgUserId);

    // 10. Convert stage/download logs
    logger.info('[10/18] Converting historic stage/download events to logs...');
    await syncDownloadStageLogs(prisma, cmgDb, cmgUserId);

    // 11. Convert events collection (Download Copy events -> data_access_log)
    logger.info('[11/18] Converting CMG events collection to data access logs...');
    await syncEventsCollection(prisma, cmgDb, cmgUserId);

    // 12. Convert dataset import logs (CMG upload history -> Bioloop import logs)
    logger.info('[12/18] Converting CMG upload history to import logs...');
    await syncImportLogs(prisma, cmgDb, cmgUserId);

    // 13. Convert conversions
    logger.info('[13/18] Converting conversions...');
    await syncConversions(prisma, cmgDb);

    // 14. Convert projects
    logger.info('[14/18] Converting projects...');
    await syncProjects(prisma, cmgDb);

    // 15. Convert dataset hierarchies (must run after conversions so conversion_id can be stored)
    logger.info('[15/18] Converting dataset hierarchies...');
    await syncDatasetHierarchies(prisma, cmgDb);

    // 16. Convert conversion logs (filesystem - production only)
    if (options.skipConversionLogs) {
      logger.info('[16/18] Skipping conversion logs (--skip-conversion-logs flag provided)');
    } else {
      logger.info('[16/18] Converting historic conversion logs...');
      await syncAllConversionLogs(prisma, cmgDb);
    }

    // 17. Convert sessions (optional)
    if (options.skipSessions) {
      logger.info('[17/18] Skipping sessions (--skip-sessions flag provided)');
    } else {
      logger.info('[17/18] Converting genome browser sessions...');
      await syncSessions(prisma, cmgDb);
    }

    // 18. Initialize cursors
    logger.info('[18/18] Initializing poller cursors...');
    await initializeCursors(prisma, cmgDb);

    // Clear lock extender
    if (lockExtender) {
      clearInterval(lockExtender);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const minutes = Math.floor(duration / 60);
    const seconds = (duration % 60).toFixed(2);
    const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;

    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`[SUCCESS] Big-bang migration completed in ${timeStr}`);
    logger.info(`          Executed ${logStatementCount} logging statements`);
    logger.info('='.repeat(80));
    logger.info('');
    logger.info('Next steps:');
    logger.info('  1. Verify data integrity in Bioloop database');
    logger.info('  2. Start continuous sync: ./bin/start_pollers.sh');
    logger.info('     Or use: ./bin/migrate.sh (and select option 3)');
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
    logger.warn('⚠️  IMPORTANT: If pollers were running before this migration:');
    logger.warn('');
    logger.warn('  1. Check if pollers are still running:');
    logger.warn('     ps aux | grep poller_sync');
    logger.warn('');
    logger.warn('  2. If pollers stopped, restart them after you fix the error:');
    logger.warn('     ./bin/start_pollers.sh --target-db=<your-target>');
    logger.warn('     Or: ./bin/migrate.sh (select option 3)');
    logger.warn('');
    logger.warn('  3. Review the data state before restarting pollers to ensure consistency.');
    logger.warn('');

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
