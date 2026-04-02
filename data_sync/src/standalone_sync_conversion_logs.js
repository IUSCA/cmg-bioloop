#!/usr/bin/env node

/**
 * Standalone CMG Conversion Logs Sync Script
 * 
 * Populates historic CMG conversion logs from filesystem into Bioloop's
 * worker_process and log tables.
 * 
 * Usage:
 *   node src/standalone_sync_conversion_logs.js [options]
 * 
 * Options:
 *   --target-db=<app|sandbox|custom>  Target database (default: sandbox)
 *   --dry-run                          Discover logs without inserting to DB
 *   --help                             Show help message
 */

const { PrismaClient } = require('@prisma/client');
const { MongoClient } = require('mongodb');
const config = require('config');
const fs = require('fs').promises;
const path = require('path');
const logger = require('./logger');
const { setDatabaseUrl } = require('./utils/db_config');
const { sanitizeUri } = require('./utils/uri_sanitizer');
const { syncConversionLogs } = require('./sync/cmg/bigbang/sync_conversion_logs');

// Parse command line arguments
function parseArgs() {
  const args = {
    targetDb: 'sandbox',
    dryRun: false,
    overwriteExisting: false,
    help: false,
  };

  process.argv.slice(2).forEach((arg) => {
    if (arg.startsWith('--target-db=')) {
      args.targetDb = arg.split('=')[1];
    } else if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--overwrite-existing') {
      args.overwriteExisting = true;
    } else if (arg === '--help') {
      args.help = true;
    } else {
      logger.warn(`Unknown argument: ${arg}`);
    }
  });

  return args;
}

// Show help message
function showHelp() {
  console.log(`
CMG Conversion Logs Sync - Standalone Script

Populates historic CMG conversion logs from filesystem into Bioloop's
worker_process and log tables.

Usage:
  node src/standalone_sync_conversion_logs.js [options]

Options:
  --target-db=<app|sandbox|custom>  Target database (default: sandbox)
                                     app     = Main application database
                                     sandbox = Isolated sync database
                                     custom  = Use DATABASE_URL env var

  --dry-run                          Discover and list log files without
                                     writing to database. Shows:
                                     - Conversions to process
                                     - Expected log file paths
                                     - Which files exist/missing

  --overwrite-existing               Re-process conversions that already
                                     have logs populated. WARNING: This
                                     will delete existing worker_process
                                     and log entries and recreate them.

  --help                             Show this help message

Examples:
  # Dry run to see what logs would be processed
  node src/standalone_sync_conversion_logs.js --dry-run

  # Sync logs to sandbox database (testing)
  node src/standalone_sync_conversion_logs.js --target-db=sandbox

  # Sync logs to main application database (production)
  node src/standalone_sync_conversion_logs.js --target-db=app

  # Re-process all conversions (overwrite existing logs)
  node src/standalone_sync_conversion_logs.js --target-db=app --overwrite-existing

Environment Variables:
  CMG_LEGACY_CONVERSIONS_LOGS_DIR   Path to CMG conversion logs directory
                                     Container: /opt/sca/project/ingestion_source_dir/CMG-SCA/production/runlogs
                                     Host: /N/project/CMG-SCA/production/runlogs

  DATABASE_URL                       PostgreSQL connection string
                                     (required for --target-db=custom)

Configuration:
  See data_sync/.env.default for all environment variables
  See data_sync/config/default.json for default configuration
`);
}

// Dry run mode: discover and list logs without inserting
async function dryRunDiscovery(prisma) {
  logger.info('');
  logger.info('='.repeat(80));
  logger.info('[DRY RUN] Conversion Logs Discovery');
  logger.info('='.repeat(80));
  logger.info('');

  // Get logs directory
  const logsDir = config.get('cmg.legacyConversionsLogsDir');
  logger.info(`Logs Directory: ${logsDir}`);
  logger.info('');

  // Check if directory exists
  let dirExists = false;
  try {
    await fs.access(logsDir);
    dirExists = true;
    logger.info('✓ Logs directory is accessible');
  } catch (error) {
    logger.error('✗ Logs directory NOT accessible');
    logger.error('  This is expected in non-production environments.');
    logger.info('');
    return;
  }

  logger.info('');

  // Get all conversions with CMG IDs
  const conversions = await prisma.conversion.findMany({
    where: {
      cmg_id: { not: null },
      dataset_id: { not: null },
    },
    include: {
      dataset: {
        select: {
          id: true,
          name: true,
        },
      },
    },
    orderBy: {
      dataset_id: 'asc',
    },
  });

  if (conversions.length === 0) {
    logger.warn('No conversions with CMG IDs found in database');
    logger.info('Run bigbang sync first to populate conversions');
    return;
  }

  logger.info(`Found ${conversions.length} conversions to process`);
  logger.info('');

  // Group conversions by dataset
  const conversionsByDataset = new Map();
  for (const conversion of conversions) {
    const datasetId = conversion.dataset_id;
    if (!conversionsByDataset.has(datasetId)) {
      conversionsByDataset.set(datasetId, []);
    }
    conversionsByDataset.get(datasetId).push(conversion);
  }

  logger.info(`Grouped into ${conversionsByDataset.size} datasets`);
  logger.info('');

  // Discover log files
  const discovered = {
    total: 0,
    exists: 0,
    missing: 0,
    alreadyProcessed: 0,
    toProcess: 0,
  };

  const logFiles = [];

  for (const [datasetId, datasetConversions] of conversionsByDataset) {
    const datasetName = datasetConversions[0].dataset?.name || `dataset-${datasetId}`;
    const logFileName = `convert_${datasetName}.log`;
    const logFilePath = path.join(logsDir, logFileName);

    discovered.total += 1;

    // Check if file exists
    let exists = false;
    let fileSize = 0;
    try {
      const stats = await fs.stat(logFilePath);
      exists = true;
      fileSize = stats.size;
      discovered.exists += 1;
    } catch (error) {
      discovered.missing += 1;
    }

    // Check if conversions already processed
    const processedCount = datasetConversions.filter(c => c.workflow_id).length;
    const unprocessedCount = datasetConversions.length - processedCount;

    if (processedCount === datasetConversions.length) {
      discovered.alreadyProcessed += 1;
    } else {
      discovered.toProcess += 1;
    }

    logFiles.push({
      datasetName,
      logFilePath,
      exists,
      fileSize,
      conversionsTotal: datasetConversions.length,
      conversionsProcessed: processedCount,
      conversionsToProcess: unprocessedCount,
    });
  }

  // Sort by status (to process first, then missing, then exists, then processed)
  logFiles.sort((a, b) => {
    if (a.conversionsToProcess > 0 && b.conversionsToProcess === 0) return -1;
    if (a.conversionsToProcess === 0 && b.conversionsToProcess > 0) return 1;
    if (!a.exists && b.exists) return -1;
    if (a.exists && !b.exists) return 1;
    return 0;
  });

  // Display results
  logger.info('─'.repeat(80));
  logger.info('DISCOVERED LOG FILES:');
  logger.info('─'.repeat(80));
  logger.info('');

  for (const logFile of logFiles) {
    const status = [];
    
    if (logFile.conversionsToProcess > 0) {
      status.push(`✓ ${logFile.conversionsToProcess} to process`);
    }
    if (logFile.conversionsProcessed > 0) {
      status.push(`✓ ${logFile.conversionsProcessed} already processed`);
    }
    if (!logFile.exists) {
      status.push('✗ FILE MISSING');
    }

    const statusStr = status.length > 0 ? ` (${status.join(', ')})` : '';
    const sizeStr = logFile.exists ? ` [${(logFile.fileSize / 1024).toFixed(1)} KB]` : '';

    logger.info(`Dataset: ${logFile.datasetName}`);
    logger.info(`  Path: ${logFile.logFilePath}${sizeStr}`);
    logger.info(`  Conversions: ${logFile.conversionsTotal} total${statusStr}`);
    logger.info('');
  }

  // Summary
  logger.info('─'.repeat(80));
  logger.info('SUMMARY:');
  logger.info('─'.repeat(80));
  logger.info(`Total datasets: ${discovered.total}`);
  logger.info(`  Log files exist: ${discovered.exists}`);
  logger.info(`  Log files missing: ${discovered.missing}`);
  logger.info('');
  logger.info(`Datasets with conversions to process: ${discovered.toProcess}`);
  logger.info(`Datasets already processed (skip): ${discovered.alreadyProcessed}`);
  logger.info('');
  logger.info('To run actual sync, remove --dry-run flag');
  logger.info('='.repeat(80));
  logger.info('');
}

// Main execution
async function main() {
  const args = parseArgs();

  if (args.help) {
    showHelp();
    process.exit(0);
  }

  logger.info('');
  logger.info('='.repeat(80));
  logger.info('CMG Conversion Logs Sync - Standalone');
  logger.info('='.repeat(80));
  logger.info('');

  if (args.dryRun) {
    logger.info('[MODE] DRY RUN - Discovery only, no database writes');
  } else {
    logger.info('[MODE] SYNC - Will populate worker_process and log tables');
  }
  logger.info(`[TARGET] Database: ${args.targetDb}`);
  
  if (args.overwriteExisting) {
    logger.warn('[OPTION] ⚠️  OVERWRITE MODE: Will re-process conversions with existing logs');
  }
  
  logger.info('');

  // Set database URL based on target
  try {
    setDatabaseUrl(args.targetDb);
  } catch (error) {
    logger.error(`Failed to set database URL: ${error.message}`);
    process.exit(1);
  }

  // Initialize Prisma client
  const prisma = new PrismaClient();

  try {
    // Test Prisma connection
    await prisma.$connect();
    logger.info('[OK] Connected to Bioloop PostgreSQL');
  } catch (error) {
    logger.error(`[FAILED] Could not connect to Bioloop PostgreSQL: ${error.message}`);
    process.exit(1);
  }

  // If dry run, do discovery and exit
  if (args.dryRun) {
    try {
      await dryRunDiscovery(prisma);
      await prisma.$disconnect();
      process.exit(0);
    } catch (error) {
      logger.error(`[DRY RUN FAILED] ${error.message}`);
      logger.error(error.stack);
      await prisma.$disconnect();
      process.exit(1);
    }
  }

  // Build MongoDB connection string
  function buildMongoUri() {
    const host = config.get('cmg_mongodb.host');
    const port = config.get('cmg_mongodb.port');
    const database = config.get('cmg_mongodb.database');
    const username = config.get('cmg_mongodb.username');
    const password = config.get('cmg_mongodb.password');

    if (username && password) {
      return `mongodb://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${database}`;
    }
    return `mongodb://${host}:${port}/${database}`;
  }

  const mongoUri = buildMongoUri();
  logger.info(`CMG MongoDB: ${sanitizeUri(mongoUri)}`);

  // Connect to CMG MongoDB
  let mongoClient;
  let cmgDb;

  try {
    mongoClient = new MongoClient(mongoUri);
    await mongoClient.connect();
    cmgDb = mongoClient.db(config.get('cmg_mongodb.database'));
    logger.info('[OK] Connected to CMG MongoDB');
  } catch (error) {
    logger.error(`[FAILED] Could not connect to CMG MongoDB: ${error.message}`);
    await prisma.$disconnect();
    process.exit(1);
  }

  logger.info('');
  logger.info('Starting conversion logs sync...');
  logger.info('');

  // Run sync
  try {
    const stats = await syncConversionLogs(prisma, cmgDb, { overwriteExisting: args.overwriteExisting });

    logger.info('');
    logger.info('='.repeat(80));
    logger.info('[SUCCESS] Conversion logs sync completed');
    logger.info('='.repeat(80));
    logger.info('');
    logger.info(`Datasets processed: ${stats.datasetsProcessed}`);
    logger.info(`Conversions updated: ${stats.conversionsUpdated}`);
    logger.info(`Conversions already processed (skipped): ${stats.alreadyProcessed}`);
    logger.info(`Worker processes created: ${stats.workerProcessesCreated}`);
    logger.info(`Log entries created: ${stats.logEntriesCreated}`);
    
    if (stats.missingLogFiles.length > 0) {
      logger.info(`Missing log files: ${stats.missingLogFiles.length}`);
    }
    if (stats.errors.length > 0) {
      logger.info(`Errors encountered: ${stats.errors.length}`);
    }
    
    logger.info('');

    // Cleanup
    await mongoClient.close();
    await prisma.$disconnect();

    process.exit(0);
  } catch (error) {
    logger.error('');
    logger.error('='.repeat(80));
    logger.error('[FAILED] Conversion logs sync FAILED');
    logger.error('='.repeat(80));
    logger.error(`Error: ${error.message}`);
    logger.error('');
    logger.error('Stack trace:');
    logger.error(error.stack);
    logger.error('');

    // Cleanup
    await mongoClient.close();
    await prisma.$disconnect();

    process.exit(1);
  }
}

// Run main function
main().catch((error) => {
  logger.error(`Unexpected error: ${error.message}`);
  logger.error(error.stack);
  process.exit(1);
});

