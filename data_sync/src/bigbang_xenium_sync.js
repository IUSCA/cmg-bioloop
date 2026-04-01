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
 *   --clear-target-db           Clear all CMG- and Xenium-originated rows before migration
 *   --help, -h                  Show help message
 *
 * Xenium Source Properties (CMG-style discrete config):
 *   XENIUM_PG_HOST
 *   XENIUM_PG_PORT
 *   XENIUM_PG_DATABASE
 *   XENIUM_PG_USERNAME
 *   XENIUM_PG_PASSWORD
 *
 * These are loaded through node-config:
 *   - defaults: data_sync/config/default.json (xenium_postgresql.*)
 *   - env mapping: data_sync/config/custom-environment-variables.json
 *   - commonly set in: data_sync/.env
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
 * 9. Bootstrap production users/roles from mounted api JSON files
 */

require('module-alias/register');
const config = require('config');
const { PrismaClient } = require('@prisma/client');
const { PrismaClient: XeniumSourcePrismaClient } = require('.prisma/xenium-client');
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
const { bootstrapProdUsers } = require('./sync/shared/bootstrap_prod_users');

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
const { forceReleaseAllSyncProcessLocks } = require('./sync/shared/process_lock_manager');

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
    } else if (arg === '--clear-target-db') {
      options.clearTargetDb = true;
    } else if (arg === '--help' || arg === '-h') {
      // eslint-disable-next-line no-console
      console.log(`
Usage: node src/bigbang_xenium_sync.js [options]

Options:
  --target-db=<target>   Target database: sandbox (default), app, or custom
  --clear-locks          Clear any existing process locks before starting
  --clear-target-db      Clear all CMG- and Xenium-originated rows before migration
  --help, -h             Show this help message

Environment Variables:
  XENIUM_PG_HOST         Xenium source PostgreSQL host
  XENIUM_PG_PORT         Xenium source PostgreSQL port
  XENIUM_PG_DATABASE     Xenium source PostgreSQL database name
  XENIUM_PG_USERNAME     Xenium source PostgreSQL username
  XENIUM_PG_PASSWORD     Xenium source PostgreSQL password

Config Source:
  data_sync/config/default.json (xenium_postgresql.* defaults)
  data_sync/config/custom-environment-variables.json (env mapping)
  data_sync/.env (recommended place to set XENIUM_PG_* values)
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

function buildXeniumSourceUri() {
  const dbConfig = config.get('xenium_postgresql');
  const {
    host, port, database, username, password,
  } = dbConfig;

  if (!host || !database) {
    throw new Error(
      'Xenium PostgreSQL source configuration missing. '
      + 'Set XENIUM_PG_HOST, XENIUM_PG_PORT, XENIUM_PG_DATABASE, '
      + 'XENIUM_PG_USERNAME, XENIUM_PG_PASSWORD in data_sync/.env '
      + '(or xenium_postgresql.* via node-config).',
    );
  }

  let uri = 'postgresql://';
  if (username) {
    uri += encodeURIComponent(username);
    if (password !== undefined && password !== null && String(password).length > 0) {
      uri += `:${encodeURIComponent(password)}`;
    }
    uri += '@';
  }

  uri += `${host}:${port || 5432}/${database}`;
  uri += '?schema=public';
  return uri;
}

async function clearCmgOriginatedRows(prisma) {
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
}

async function clearXeniumOriginatedRows(prisma) {
  await prisma.project.deleteMany({ where: { xenium_id: { not: null } } });
  await prisma.dataset.deleteMany({ where: { xenium_id: { not: null } } });
  await prisma.user.deleteMany({ where: { xenium_id: { not: null } } });
  await prisma.xenium_sync_retry.deleteMany({});
  await prisma.xenium_sync_cursor.deleteMany({});
}

async function clearAllLegacyMigrationTargetData(prisma) {
  logger.warn('='.repeat(80));
  logger.warn('CLEARING ALL LEGACY MIGRATION DATA (CMG + XENIUM)');
  logger.warn('='.repeat(80));
  logger.warn('Deletes CMG- and Xenium-originated business rows and both sync cursor/retry tables.');
  logger.warn('');

  try {
    await clearCmgOriginatedRows(prisma);
    logger.info('CMG-originated rows cleared.');
    await clearXeniumOriginatedRows(prisma);
    logger.info('Xenium-originated rows cleared.');
    logger.info('');
  } catch (error) {
    logger.error('Failed to clear legacy migration rows:', error.message);
    throw error;
  }
}

async function getSourceTableSet(xeniumPrisma) {
  const rows = await xeniumPrisma.$queryRawUnsafe(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
  );
  return new Set(rows.map((row) => row.table_name));
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
    const xeniumDatabaseUrl = buildXeniumSourceUri();
    xeniumPrisma = new XeniumSourcePrismaClient({ datasources: { db: { url: xeniumDatabaseUrl } } });
    await xeniumPrisma.$connect();
    logger.info('[OK] Connected to Xenium PostgreSQL (source)');
    const sourceTables = await getSourceTableSet(xeniumPrisma);

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

    if (options.clearTargetDb) {
      await clearAllLegacyMigrationTargetData(prisma);
      logger.info('[CLEAR-TARGET-DB] Resetting CMG and Xenium process locks...');
      await forceReleaseAllSyncProcessLocks(prisma);
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

    logger.info('Starting xenium bigbang migration...');
    logger.info('');

    logger.info('[1/9] Seeding constants (roles, xenium system user, analysis types, import sources)...');
    await seedConstants(prisma);

    logger.info('[2/9] Syncing users...');
    await syncUsers(prisma, xeniumPrisma);

    logger.info('[3/9] Syncing datasets (RAW_DATA and DATA_PRODUCT)...');
    await syncAllDatasets(prisma, xeniumPrisma);

    logger.info('[4/9] Syncing dataset audit logs...');
    await syncAuditLogs(prisma, xeniumPrisma);

    if (sourceTables.has('dataset_import_log')) {
      logger.info('[5/9] Syncing dataset import logs...');
      await syncImportLogs(prisma, xeniumPrisma);
    } else {
      logger.warn('[5/9] Skipping dataset import logs (source table dataset_import_log not present).');
    }

    logger.info('[6/9] Syncing dataset hierarchies (RAW_DATA → DATA_PRODUCT)...');
    await syncDatasetHierarchies(prisma, xeniumPrisma);

    logger.info('[7/9] Syncing projects...');
    await syncProjects(prisma, xeniumPrisma);

    logger.info('[8/9] Initializing xenium poller cursors...');
    await initializeCursors(prisma, xeniumPrisma);

    logger.info('[9/9] Bootstrapping production users/roles from API JSON...');
    await bootstrapProdUsers(prisma, logger, 'XENIUM');

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
    const errorMessage = error instanceof Error
      ? error.message
      : (typeof error === 'string' ? error : JSON.stringify(error));
    logger.error(`Error: ${sanitizeUri(errorMessage || String(error))}`);
    if (error && error.stack) logger.error(`Stack: ${sanitizeUri(error.stack)}`);

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
