#!/usr/bin/env node

/**
 * Xenium to Bioloop Incremental Poller Sync Script
 *
 * Continuously polls the Xenium PostgreSQL source database for changes and
 * applies them incrementally to cmg-bioloop's PostgreSQL.
 *
 * Unlike the CMG pollers (MongoDB → PostgreSQL), these pollers are
 * PostgreSQL-to-PostgreSQL and use timestamp + integer ID cursors.
 *
 * Must be run after bigbang_xenium_sync.js has completed and initialized cursors.
 *
 * Usage:
 *   node src/poller_xenium_sync.js [options]
 *
 * Options:
 *   --target-db=<target>   Target database: sandbox (default), app, or custom
 *   --clear-locks          Clear any existing process locks before starting
 *   --help, -h             Show help message
 *
 * Active Pollers (4 total — no sessions or conversions in xenium):
 *   1. xenium_user_roles     - User role assignment changes
 *   2. xenium_project_acl    - Project user + dataset ACL changes
 *   3. xenium_dataset_metadata - Dataset description changes
 *   4. xenium_project_metadata - Project name/description/browser_enabled changes
 */

require('module-alias/register');
const config = require('config');
const { PrismaClient } = require('@prisma/client');
const logger = require('./logger');
const { setDatabaseUrl } = require('./utils/db_config');

const XeniumUserRolesPoller = require('./sync/xenium/pollers/user_roles_poller');
const XeniumProjectACLPoller = require('./sync/xenium/pollers/project_acl_poller');
const XeniumDatasetMetadataPoller = require('./sync/xenium/pollers/dataset_metadata_poller');
const XeniumProjectMetadataPoller = require('./sync/xenium/pollers/project_metadata_poller');

const {
  xeniumProcessLockManager,
  POLLER_LOCK_TTL_MS,
} = require('./sync/shared/process_lock_manager');

const {
  acquireProcessLock,
  releaseProcessLock,
  forceReleaseAllProcessLocks,
  checkProcessLockStatus,
} = xeniumProcessLockManager;

function parseArgs() {
  const args = process.argv.slice(2);
  const options = { clearLocks: false };

  args.forEach((arg) => {
    if (arg.startsWith('--target-db=')) {
      [, options.targetDb] = arg.split('=');
    } else if (arg === '--clear-locks') {
      options.clearLocks = true;
    } else if (arg === '--help' || arg === '-h') {
      // eslint-disable-next-line no-console
      console.log(`
Usage: node src/poller_xenium_sync.js [options]

Options:
  --target-db=<target>   Target database: sandbox (default), app, or custom
  --clear-locks          Clear any existing process locks before starting
  --help, -h             Show this help message
`);
      process.exit(0);
    }
  });

  return options;
}

function sanitizeUri(str) {
  if (!str) return str;
  if (typeof str !== 'string') str = JSON.stringify(str);
  str = str.replace(/postgresql:\/\/[^:]+:[^@]+@/g, 'postgresql://<credentials>@');
  return str;
}

function setupGracefulShutdown(pollers, xeniumPrisma, prisma, lockAcquired) {
  const shutdown = async (signal) => {
    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`Received ${signal}, shutting down xenium pollers gracefully...`);
    logger.info('='.repeat(80));

    pollers.forEach((poller) => {
      poller.stop();
      logger.info(`  - ${poller.pollerName} stopped`);
    });

    if (lockAcquired && prisma) {
      await releaseProcessLock(prisma, 'xenium_poller');
    }

    if (xeniumPrisma) {
      await xeniumPrisma.$disconnect();
      logger.info('Closed Xenium PostgreSQL connection');
    }

    if (prisma) {
      await prisma.$disconnect();
      logger.info('Closed cmg-bioloop PostgreSQL connection');
    }

    logger.info('[OK] Xenium poller shutdown complete');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception:', sanitizeUri(error.stack || error.message || String(error)));
    shutdown('UNCAUGHT_EXCEPTION');
  });

  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled rejection at:', promise, 'reason:', sanitizeUri(String(reason)));
    shutdown('UNHANDLED_REJECTION');
  });
}

function startMetricsReporter(pollers) {
  setInterval(() => {
    logger.info('');
    logger.info('--- Xenium Poller Metrics ---');
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
  }, 60000);
}

async function main() {
  const options = parseArgs();

  logger.info('='.repeat(80));
  logger.info('Xenium to Bioloop Incremental Poller Sync');
  logger.info('='.repeat(80));

  let prisma;
  let xeniumPrisma;
  let lockAcquired = false;
  const pollers = [];

  try {
    const targetDb = options.targetDb || 'sandbox';
    const databaseUrl = setDatabaseUrl(targetDb);
    logger.info(`[OK] Target database: ${targetDb}`);
    logger.info(`[OK] Database URL: ${sanitizeUri(databaseUrl)}`);

    prisma = new PrismaClient();
    logger.info('[OK] Prisma client created (target: cmg-bioloop)');

    if (options.clearLocks) {
      logger.warn('[CLEAR-LOCKS] Clearing xenium process locks...');
      const count = await forceReleaseAllProcessLocks(prisma);
      logger.warn(`[CLEAR-LOCKS] Released ${count} process lock(s)`);
    }

    const bigbangLockStatus = await checkProcessLockStatus(prisma, 'xenium_bigbang');
    if (bigbangLockStatus) {
      logger.error('[FAILED] Xenium bigbang is currently running.');
      logger.error('Start xenium pollers only after bigbang completes.');
      process.exit(1);
    }

    lockAcquired = await acquireProcessLock(prisma, 'xenium_poller', POLLER_LOCK_TTL_MS);
    if (!lockAcquired) {
      logger.error('[FAILED] Another xenium poller process is already running');
      logger.error('Use --clear-locks to force release locks.');
      process.exit(1);
    }

    const xeniumDatabaseUrl = process.env.XENIUM_DATABASE_URL || config.get('xenium_postgresql.url');
    if (!xeniumDatabaseUrl) {
      throw new Error(
        'Xenium source database URL not configured. '
        + 'Set XENIUM_DATABASE_URL or xenium_postgresql.url in config.',
      );
    }
    xeniumPrisma = new PrismaClient({ datasources: { db: { url: xeniumDatabaseUrl } } });
    await xeniumPrisma.$connect();
    logger.info('[OK] Connected to Xenium PostgreSQL (source)');

    pollers.push(new XeniumUserRolesPoller(prisma, xeniumPrisma));
    pollers.push(new XeniumProjectACLPoller(prisma, xeniumPrisma));
    pollers.push(new XeniumDatasetMetadataPoller(prisma, xeniumPrisma));
    pollers.push(new XeniumProjectMetadataPoller(prisma, xeniumPrisma));
    pollers.forEach((poller) => poller.start());

    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`[OK] Started ${pollers.length} xenium pollers`);
    logger.info('='.repeat(80));
    logger.info('');

    setupGracefulShutdown(pollers, xeniumPrisma, prisma, lockAcquired);
    startMetricsReporter(pollers);

    await new Promise(() => {}); // Keep process alive
  } catch (error) {
    logger.error('');
    logger.error('='.repeat(80));
    logger.error('[FAILED] Xenium poller initialization failed');
    logger.error('='.repeat(80));
    logger.error('Error:', sanitizeUri(error.message));
    if (error.stack) logger.error('Stack:', sanitizeUri(error.stack));

    pollers.forEach((poller) => poller.stop());

    if (lockAcquired && prisma) await releaseProcessLock(prisma, 'xenium_poller');
    if (xeniumPrisma) await xeniumPrisma.$disconnect();
    if (prisma) await prisma.$disconnect();

    process.exit(1);
  }
}

if (require.main === module) {
  main().catch((error) => {
    logger.error('Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = { main };
