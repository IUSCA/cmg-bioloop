#!/usr/bin/env node

/**
 * CMG to Bioloop Big-Bang Synchronization Script
 *
 * One-time initial population of all CMG data into Bioloop.
 * Follows the exact same order as: db_conversion/src/convert/scripts/convert.py
 *
 * Usage:
 *   node src/scripts/cmg_bigbang_sync.js --cmg-uri="mongodb://..."
 *
 * Or with environment variables (using config system):
 *   node src/scripts/cmg_bigbang_sync.js
 *
 * Order of operations (same as convert.py):
 * 1. Create roles
 * 2. Create CMG system user
 * 3. Populate pipeline definitions (cmd_line_programs, conversion_definitions, arguments)
 * 4. Convert users
 * 5. Convert datasets (RAW_DATA and DATA_PRODUCT)
 * 6. Convert dataset audit logs (from events)
 * 7. Convert dataset hierarchies
 * 8. Convert projects
 * 9. Convert conversions
 * 10. Convert sessions (optional - many will be skipped)
 * 11. Initialize cursors for pollers
 */

const config = require('config');
const { MongoClient } = require('mongodb');
const prisma = require('@/db');
const logger = require('@/services/logger');

// Bigbang modules
const { createRoles, createCMGUser, populatePipelineDefinitions } = require('./cmg_sync/bigbang/seed_constants');
const { syncUsers, getBioloopCMGUserId } = require('./cmg_sync/bigbang/sync_users');
const { syncAllDatasets } = require('./cmg_sync/bigbang/sync_datasets');
const { syncAuditLogs } = require('./cmg_sync/bigbang/sync_audit_logs');
const { syncDatasetHierarchies } = require('./cmg_sync/bigbang/sync_dataset_hierarchies');
const { syncProjects } = require('./cmg_sync/bigbang/sync_projects');
const { syncConversions } = require('./cmg_sync/bigbang/sync_conversions');
const { syncSessions } = require('./cmg_sync/bigbang/sync_sessions');
const { initializeCursors } = require('./cmg_sync/bigbang/initialize_cursors');

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};

  for (const arg of args) {
    if (arg.startsWith('--cmg-uri=')) {
      options.cmgUri = arg.split('=')[1];
    } else if (arg.startsWith('--rhythm-uri=')) {
      options.rhythmUri = arg.split('=')[1];
    } else if (arg === '--skip-sessions') {
      options.skipSessions = true;
    } else if (arg === '--help' || arg === '-h') {
      console.log(`
Usage: node src/scripts/cmg_bigbang_sync.js [options]

Options:
  --cmg-uri=<uri>        MongoDB connection string for CMG database
                         Format: mongodb://username:password@host:port/database?authSource=admin
                         
  --rhythm-uri=<uri>     MongoDB connection string for Rhythm database
                         (Optional if using environment variables)
                         
  --skip-sessions        Skip genome browser session conversion (recommended for initial run)
  
  --help, -h             Show this help message

Environment Variables (alternative to --cmg-uri and --rhythm-uri):
  CMG_MONGO_HOST, CMG_MONGO_PORT, CMG_MONGO_DB, CMG_MONGO_USERNAME,
  CMG_MONGO_PASSWORD, CMG_MONGO_AUTH_SOURCE
  
  RHYTHM_MONGO_HOST, RHYTHM_MONGO_PORT, RHYTHM_MONGO_DB, RHYTHM_MONGO_USERNAME,
  RHYTHM_MONGO_PASSWORD, RHYTHM_MONGO_AUTH_SOURCE

Examples:
  # Using command-line URI
  node src/scripts/cmg_bigbang_sync.js --cmg-uri="mongodb://cmg:pass@localhost:27017/cmg?authSource=admin"
  
  # Using environment variables (via config system)
  node src/scripts/cmg_bigbang_sync.js
  
  # Skip sessions (recommended for first run)
  node src/scripts/cmg_bigbang_sync.js --skip-sessions
`);
      process.exit(0);
    }
  }

  return options;
}

/**
 * Build MongoDB connection URI from config or command line
 */
function buildMongoUri(dbType, cmdLineUri) {
  if (cmdLineUri) {
    return cmdLineUri;
  }

  // Get from config system
  const configKey = dbType === 'cmg' ? 'cmg_mongodb' : 'rhythm_mongodb';
  const dbConfig = config.get(configKey);

  const {
    host, port, database, username, password, authSource,
  } = dbConfig;

  if (!host || !database) {
    throw new Error(`${dbType.toUpperCase()} MongoDB configuration missing. Please set environment variables or use --${dbType}-uri flag.`);
  }

  let uri = 'mongodb://';

  if (username && password) {
    uri += `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`;
  }

  uri += `${host}:${port}/${database}`;

  if (authSource) {
    uri += `?authSource=${authSource}`;
  }

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

  let cmgClient; let
    rhythmClient;

  try {
    // Build connection URIs
    const cmgUri = buildMongoUri('cmg', options.cmgUri);
    const rhythmUri = buildMongoUri('rhythm', options.rhythmUri);

    logger.info('Connecting to databases...');
    logger.info(`CMG MongoDB: ${cmgUri.replace(/\/\/.*@/, '//<credentials>@')}`);
    logger.info(`Rhythm MongoDB: ${rhythmUri.replace(/\/\/.*@/, '//<credentials>@')}`);

    // Connect to MongoDB databases
    cmgClient = new MongoClient(cmgUri);
    await cmgClient.connect();
    const cmgDb = cmgClient.db();
    logger.info('✓ Connected to CMG MongoDB');

    rhythmClient = new MongoClient(rhythmUri);
    await rhythmClient.connect();
    const rhythmDb = rhythmClient.db();
    logger.info('✓ Connected to Rhythm MongoDB');

    logger.info('✓ Prisma client ready');
    logger.info('');

    // Execute migration in order (matching convert.py)
    logger.info('Starting big-bang migration...');
    logger.info('');

    // 1. Create roles
    logger.info('[1/11] Creating roles...');
    await createRoles(prisma);

    // 2. Create CMG system user
    logger.info('[2/11] Creating CMG system user...');
    const cmgUserId = await createCMGUser(prisma);

    // 3. Populate pipeline definitions
    logger.info('[3/11] Populating pipeline definitions...');
    await populatePipelineDefinitions(prisma, cmgUserId);

    // 4. Convert users
    logger.info('[4/11] Converting users...');
    await syncUsers(prisma, cmgDb);

    // 5. Convert datasets
    logger.info('[5/11] Converting datasets...');
    await syncAllDatasets(prisma, cmgDb);

    // 6. Convert dataset audit logs
    logger.info('[6/11] Converting dataset audit logs...');
    await syncAuditLogs(prisma, cmgDb, cmgUserId);

    // 7. Convert dataset hierarchies
    logger.info('[7/11] Converting dataset hierarchies...');
    await syncDatasetHierarchies(prisma, cmgDb);

    // 8. Convert projects
    logger.info('[8/11] Converting projects...');
    await syncProjects(prisma, cmgDb);

    // 9. Convert conversions
    logger.info('[9/11] Converting conversions...');
    await syncConversions(prisma, cmgDb);

    // 10. Convert sessions (optional)
    if (options.skipSessions) {
      logger.info('[10/11] Skipping sessions (--skip-sessions flag provided)');
    } else {
      logger.info('[10/11] Converting genome browser sessions...');
      await syncSessions(prisma, cmgDb);
    }

    // 11. Initialize cursors
    logger.info('[11/11] Initializing poller cursors...');
    await initializeCursors(prisma, cmgDb, rhythmDb);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`✓ Big-bang migration completed successfully in ${duration}s`);
    logger.info('='.repeat(80));
    logger.info('');
    logger.info('Next steps:');
    logger.info('  1. Verify data integrity in Bioloop database');
    logger.info('  2. Start the poller sync script: node src/scripts/cmg_poller_sync.js');
    logger.info('  3. Monitor logs for any sync issues');
    logger.info('');
  } catch (error) {
    logger.error('');
    logger.error('='.repeat(80));
    logger.error('✗ Big-bang migration FAILED');
    logger.error('='.repeat(80));
    logger.error('Error:', error);
    logger.error('Stack:', error.stack);
    logger.error('');
    logger.error('The migration has been rolled back. Please fix the error and try again.');
    logger.error('');

    process.exit(1);
  } finally {
    // Close connections
    if (cmgClient) {
      await cmgClient.close();
      logger.info('Closed CMG MongoDB connection');
    }

    if (rhythmClient) {
      await rhythmClient.close();
      logger.info('Closed Rhythm MongoDB connection');
    }

    await prisma.$disconnect();
    logger.info('Closed Prisma connection');
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
