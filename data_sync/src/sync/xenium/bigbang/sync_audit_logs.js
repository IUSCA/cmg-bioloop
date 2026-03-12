/**
 * Xenium Bigbang — Sync Dataset Audit Logs
 *
 * Reads dataset_audit rows from the Xenium source and inserts them into
 * cmg-bioloop's `dataset_audit` table. Maps xenium dataset IDs to Bioloop
 * dataset IDs via the `xenium_id` field set during sync_datasets.
 *
 * Idempotent: skips audit logs for datasets not found in Bioloop (e.g., if
 * sync_datasets has not run yet or was skipped).
 *
 * Implemented in Chat 2.
 */

const logger = require('../../../logger');
const { coerceIntegerId, splitIntoChunks, toDateOrNow } = require('./helpers');

/**
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function syncAuditLogs(prisma, xeniumPrisma) {
  logger.info('[XENIUM][sync_audit_logs] Starting audit log synchronization');

  const [sourceAuditLogs, targetDatasets, targetUsers, systemUser] = await Promise.all([
    xeniumPrisma.dataset_audit.findMany({ orderBy: { id: 'asc' } }),
    prisma.dataset.findMany({
      where: { xenium_id: { not: null } },
      select: { id: true, xenium_id: true },
    }),
    prisma.user.findMany({
      where: { xenium_id: { not: null } },
      select: { id: true, xenium_id: true },
    }),
    prisma.user.findUnique({ where: { username: 'cmguser' }, select: { id: true } }),
  ]);

  const datasetByXeniumId = new Map(targetDatasets.map((row) => [row.xenium_id, row.id]));
  const userByXeniumId = new Map(targetUsers.map((row) => [row.xenium_id, row.id]));
  logger.info(`[XENIUM][sync_audit_logs] Found ${sourceAuditLogs.length} source audit rows`);

  let createdCount = 0;
  let skippedCount = 0;

  const chunks = splitIntoChunks(sourceAuditLogs, 300);
  for (const chunk of chunks) {
    // eslint-disable-next-line no-restricted-syntax
    for (const sourceLog of chunk) {
      const sourceDatasetId = coerceIntegerId(sourceLog.dataset_id);
      if (!sourceDatasetId || !datasetByXeniumId.has(sourceDatasetId)) {
        skippedCount += 1;
        continue;
      }

      const datasetId = datasetByXeniumId.get(sourceDatasetId);
      const sourceUserId = coerceIntegerId(sourceLog.user_id);
      const userId = sourceUserId && userByXeniumId.has(sourceUserId)
        ? userByXeniumId.get(sourceUserId)
        : systemUser?.id || null;
      const timestamp = toDateOrNow(sourceLog.timestamp);

      // eslint-disable-next-line no-await-in-loop
      const existing = await prisma.dataset_audit.findFirst({
        where: {
          dataset_id: datasetId,
          action: sourceLog.action,
          timestamp,
          user_id: userId,
        },
        select: { id: true },
      });
      if (existing) {
        skippedCount += 1;
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      await prisma.dataset_audit.create({
        data: {
          action: sourceLog.action || 'updated',
          timestamp,
          old_data: sourceLog.old_data || null,
          new_data: sourceLog.new_data || null,
          dataset_id: datasetId,
          user_id: userId,
        },
      });
      createdCount += 1;
    }
  }

  logger.info(
    `[XENIUM][sync_audit_logs] Complete (${createdCount} created, ${skippedCount} skipped)`,
  );
}

module.exports = { syncAuditLogs };
