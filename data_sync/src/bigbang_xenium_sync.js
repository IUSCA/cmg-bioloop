#!/usr/bin/env node

/**
 * Xenium to Bioloop Big-Bang Synchronization Script
 *
 * One-time initial population of all Xenium data into Bioloop.
 * Source: Xenium PostgreSQL database
 * Target: cmg-bioloop PostgreSQL database
 *
 * Unlike the CMG migration (MongoDB → PostgreSQL), this is a
 * PostgreSQL-to-PostgreSQL migration. Both source and target are
 * Prisma-accessible relational databases.
 *
 * Usage:
 *   node src/bigbang_xenium_sync.js [options]
 *
 * Options:
 *   --target-db=<target>        Target database: sandbox (default), app, or custom
 *   --clear-locks               Clear any existing process locks before starting
 *   --clear-xenium-target-data  Clear xenium-originated rows from target before migration
 *   --help, -h                  Show help message
 *
 * Environment Variables:
 *   XENIUM_DATABASE_URL   Connection URL for the Xenium source PostgreSQL database
 *
 * Order of operations:
 * 1. Seed constants (roles, xenium system user, analysis types, import sources)
 * 2. Sync users
 * 3. Sync datasets (RAW_DATA and DATA_PRODUCT)
 * 4. Sync dataset audit logs
 * 5. Sync dataset import logs
 * 6. Sync dataset hierarchies (RAW_DATA → DATA_PRODUCT)
 * 7. Sync projects
 * 8. Initialize xenium poller cursors
 */

require('module-alias/register');
const config = require('config');
const { PrismaClient } = require('@prisma/client');
const originalLogger = require('./logger');
const { setDatabaseUrl } = require('./utils/db_config');

const { seedConstants } = require('./sync/xenium/bigbang/seed_constants');
const { syncUsers } = require('./sync/xenium/bigbang/sync_users');
const { syncAllDatasets } = require('./sync/xenium/bigbang/sync_datasets');
const { syncAuditLogs } = require('./sync/xenium/bigbang/sync_audit_logs');
const { syncImportLogs } = require('./sync/xenium/bigbang/sync_import_logs');
const { syncDatasetHierarchies } = require('./sync/xenium/bigbang/sync_dataset_hierarchies');
const { syncProjects } = require('./sync/xenium/bigbang/sync_projects');
const { initializeCursors } = require('./sync/xenium/bigbang/initialize_cursors');

const {
  xeniumProcessLockManager,
  DEFAULT_LOCK_TTL_MS,
} = require('./sync/shared/process_lock_manager');

const {
  acquireProcessLock,
  releaseProcessLock,
  extendProcessLock,
  checkProcessLockStatus,
  forceReleaseAllProcessLocks,
} = xeniumProcessLockManager;

// Wrap logger to count log statements
let logStatementCount = 0;
const logger = {};

['info', 'warn', 'error', 'debug'].forEach((level) => {
  logger[level] = (...args) => {
    logStatementCount += 1;
    originalLogger[level](...args);
  };
});

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};

  for (const arg of args) {
    if (arg.startsWith('--target-db=')) {
      [, options.targetDb] = arg.split('=');
    } else if (arg === '--clear-locks') {
      options.clearLocks = true;
    } else if (arg === '--clear-xenium-target-data') {
      options.clearXeniumTargetData = true;
    } else if (arg === '--help' || arg === '-h') {
      // eslint-disable-next-line no-console
      console.log(`
Usage: node src/bigbang_xenium_sync.js [options]

Options:
  --target-db=<target>   Target database: sandbox (default), app, or custom
  --clear-locks          Clear any existing process locks before starting
  --clear-xenium-target-data  Clear xenium-originated rows from target before migration
  --help, -h             Show this help message

Environment Variables:
  XENIUM_DATABASE_URL    PostgreSQL connection URL for the Xenium source database
`);
      process.exit(0);
    }
  }

  return options;
}

function sanitizeUri(str) {
  if (!str) return str;
  if (typeof str !== 'string') str = JSON.stringify(str);
  str = str.replace(/postgresql:\/\/[^:]+:[^@]+@/g, 'postgresql://<credentials>@');
  return str;
}

async function clearXeniumTargetRows(prisma) {
  logger.warn('[CLEAR-TARGET-DB] Clearing xenium-originated rows from target');

  await prisma.$transaction(async (tx) => {
    await tx.project.deleteMany({ where: { xenium_id: { not: null } } });
    await tx.dataset.deleteMany({ where: { xenium_id: { not: null } } });
    await tx.user.deleteMany({ where: { xenium_id: { not: null } } });
    await tx.xenium_sync_retry.deleteMany({});
    await tx.xenium_sync_cursor.deleteMany({});
  });

  logger.warn('[CLEAR-TARGET-DB] Xenium rows cleared');
}

async function main() {
  const startTime = Date.now();
  const options = parseArgs();

  logger.info('='.repeat(80));
  logger.info('Xenium to Bioloop Big-Bang Synchronization');
  logger.info('='.repeat(80));

  let prisma;
  let xeniumPrisma;
  let lockAcquired = false;
  let lockExtender;

  try {
    const targetDb = options.targetDb || 'sandbox';
    const databaseUrl = setDatabaseUrl(targetDb);
    logger.info(`[OK] Target database: ${targetDb}`);
    logger.info(`[OK] Database URL: ${sanitizeUri(databaseUrl)}`);

    // Target database (cmg-bioloop)
    prisma = new PrismaClient();
    await prisma.$connect();
    logger.info('[OK] Connected to cmg-bioloop PostgreSQL (target)');

    // Source database (Xenium)
    const xeniumDatabaseUrl = process.env.XENIUM_DATABASE_URL || config.get('xenium_postgresql.url');
    if (!xeniumDatabaseUrl) {
      throw new Error(
        'Xenium source database URL not configured. '
        + 'Set XENIUM_DATABASE_URL environment variable or xenium_postgresql.url in config.',
      );
    }
    xeniumPrisma = new PrismaClient({ datasources: { db: { url: xeniumDatabaseUrl } } });
    await xeniumPrisma.$connect();
    logger.info('[OK] Connected to Xenium PostgreSQL (source)');

    if (options.clearLocks) {
      logger.warn('[CLEAR-LOCKS] Clearing all existing xenium process locks...');
      const count = await forceReleaseAllProcessLocks(prisma);
      logger.warn(`[CLEAR-LOCKS] Released ${count} process lock(s)`);
    }

    const pollerLockStatus = await checkProcessLockStatus(prisma, 'xenium_poller');
    if (pollerLockStatus) {
      logger.error('');
      logger.error('='.repeat(80));
      logger.error('Xenium pollers are currently running. Stop them before running bigbang.');
      logger.error('='.repeat(80));
      process.exit(1);
    }

    lockAcquired = await acquireProcessLock(prisma, 'xenium_bigbang', DEFAULT_LOCK_TTL_MS);
    if (!lockAcquired) {
      logger.error('[FAILED] Another xenium bigbang process is already running');
      logger.error('Use --clear-locks to force release locks.');
      process.exit(1);
    }

    lockExtender = setInterval(async () => {
      try {
        await extendProcessLock(prisma, 'xenium_bigbang', DEFAULT_LOCK_TTL_MS);
      } catch (error) {
        logger.error('Failed to extend xenium bigbang process lock:', error);
        clearInterval(lockExtender);
      }
    }, 120000);

    if (options.clearXeniumTargetData) {
      await clearXeniumTargetRows(prisma);
    }

    logger.info('Starting xenium bigbang migration...');
    logger.info('');

    logger.info('[1/8] Seeding constants (roles, xenium system user, analysis types, import sources)...');
    await seedConstants(prisma);

    logger.info('[2/8] Syncing users...');
    await syncUsers(prisma, xeniumPrisma);

    logger.info('[3/8] Syncing datasets (RAW_DATA and DATA_PRODUCT)...');
    await syncAllDatasets(prisma, xeniumPrisma);

    logger.info('[4/8] Syncing dataset audit logs...');
    await syncAuditLogs(prisma, xeniumPrisma);

    logger.info('[5/8] Syncing dataset import logs...');
    await syncImportLogs(prisma, xeniumPrisma);

    logger.info('[6/8] Syncing dataset hierarchies (RAW_DATA → DATA_PRODUCT)...');
    await syncDatasetHierarchies(prisma, xeniumPrisma);

    logger.info('[7/8] Syncing projects...');
    await syncProjects(prisma, xeniumPrisma);

    logger.info('[8/8] Initializing xenium poller cursors...');
    await initializeCursors(prisma, xeniumPrisma);

    if (lockExtender) clearInterval(lockExtender);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const minutes = Math.floor(duration / 60);
    const seconds = (duration % 60).toFixed(2);
    const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;

    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`[SUCCESS] Xenium bigbang migration completed in ${timeStr}`);
    logger.info(`          Executed ${logStatementCount} logging statements`);
    logger.info('='.repeat(80));
    logger.info('');
    logger.info('Next steps:');
    logger.info('  1. Verify data integrity in Bioloop database');
    logger.info('  2. Start xenium pollers: ./bin/start_pollers_xenium.sh');
  } catch (error) {
    if (lockExtender) clearInterval(lockExtender);

    logger.error('');
    logger.error('='.repeat(80));
    logger.error('[FAILED] Xenium bigbang migration FAILED');
    logger.error('='.repeat(80));
    logger.error('Error:', sanitizeUri(error.message));
    if (error.stack) logger.error('Stack:', sanitizeUri(error.stack));

    process.exit(1);
  } finally {
    if (lockAcquired && prisma) {
      await releaseProcessLock(prisma, 'xenium_bigbang');
    }
    if (xeniumPrisma) {
      await xeniumPrisma.$disconnect();
      logger.info('Closed Xenium PostgreSQL connection');
    }
    if (prisma) {
      await prisma.$disconnect();
      logger.info('Closed cmg-bioloop PostgreSQL connection');
    }
  }
}

if (require.main === module) {
  main().catch((error) => {
    // eslint-disable-next-line no-console
    console.error('Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = { main };
