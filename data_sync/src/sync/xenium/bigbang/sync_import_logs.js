/**
 * Xenium Bigbang — Sync Dataset Import Logs
 *
 * Reads dataset_import_log rows from the Xenium source and inserts them into
 * cmg-bioloop's `dataset_import_log` table. Maps xenium dataset IDs to Bioloop
 * dataset IDs and sets `xenium_id` on each inserted log row.
 *
 * Idempotent: checks by `xenium_id` before inserting.
 *
 * Implemented in Chat 2.
 */

const logger = require('../../../logger');
const {
  withDatasetOrigin,
  coerceIntegerId,
  splitIntoChunks,
  toDateOrNow,
} = require('./helpers');

/**
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function syncImportLogs(prisma, xeniumPrisma) {
  logger.info('[XENIUM][sync_import_logs] Starting import-log synchronization');

  const [sourceLogs, targetDatasets] = await Promise.all([
    xeniumPrisma.dataset_import_log.findMany({
      orderBy: { id: 'asc' },
      select: {
        id: true,
        dataset_id: true,
        source_run: true,
        notes: true,
        metadata: true,
        created_at: true,
        updated_at: true,
      },
    }),
    prisma.dataset.findMany({
      where: { xenium_id: { not: null } },
      select: { id: true, xenium_id: true, name: true },
    }),
  ]);

  const datasetByXeniumId = new Map(targetDatasets.map((row) => [row.xenium_id, row]));

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  const chunks = splitIntoChunks(sourceLogs, 300);
  for (const chunk of chunks) {
    // eslint-disable-next-line no-restricted-syntax
    for (const sourceLog of chunk) {
      const xeniumLogId = coerceIntegerId(sourceLog.id);
      const sourceDatasetId = coerceIntegerId(sourceLog.dataset_id);
      if (!xeniumLogId || !sourceDatasetId || !datasetByXeniumId.has(sourceDatasetId)) {
        skippedCount += 1;
        continue;
      }

      const targetDataset = datasetByXeniumId.get(sourceDatasetId);
      const metadata = withDatasetOrigin(sourceLog.metadata);
      const payload = {
        xenium_id: xeniumLogId,
        dataset_id: targetDataset.id,
        source_run: sourceLog.source_run || targetDataset.name,
        notes: sourceLog.notes || null,
        metadata,
        created_at: toDateOrNow(sourceLog.created_at),
        updated_at: toDateOrNow(sourceLog.updated_at),
      };

      // eslint-disable-next-line no-await-in-loop
      const existing = await prisma.dataset_import_log.findFirst({
        where: { xenium_id: xeniumLogId },
        select: { id: true },
      });

      if (!existing) {
        // eslint-disable-next-line no-await-in-loop
        await prisma.dataset_import_log.create({ data: payload });
        createdCount += 1;
      } else {
        // eslint-disable-next-line no-await-in-loop
        await prisma.dataset_import_log.update({
          where: { id: existing.id },
          data: {
            dataset_id: payload.dataset_id,
            source_run: payload.source_run,
            notes: payload.notes,
            metadata: payload.metadata,
          },
        });
        updatedCount += 1;
      }
    }
  }

  logger.info(
    `[XENIUM][sync_import_logs] Complete (${createdCount} created, ${updatedCount} updated, ${skippedCount} skipped)`,
  );
}

module.exports = { syncImportLogs };
