/**
 * Xenium Bigbang — Sync Dataset Hierarchies
 *
 * Reads dataset_hierarchy rows from the Xenium source (RAW_DATA → DATA_PRODUCT
 * relationships) and recreates them in cmg-bioloop's `dataset_hierarchy` table.
 *
 * Maps source_id and derived_id from xenium dataset IDs to Bioloop dataset IDs
 * using the `xenium_id` field. Must run after sync_datasets.
 *
 * Idempotent: checks for existing [source_id, derived_id] pairs before inserting.
 *
 * Implemented in Chat 2.
 */

const logger = require('../../../logger');
const { coerceIntegerId, splitIntoChunks } = require('./helpers');

/**
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function syncDatasetHierarchies(prisma, xeniumPrisma) {
  logger.info('[XENIUM][sync_dataset_hierarchies] Starting hierarchy synchronization');

  const [sourceHierarchies, targetDatasets] = await Promise.all([
    xeniumPrisma.dataset_hierarchy.findMany({
      orderBy: [{ source_id: 'asc' }, { derived_id: 'asc' }],
    }),
    prisma.dataset.findMany({
      where: { xenium_id: { not: null } },
      select: { id: true, xenium_id: true },
    }),
  ]);

  const targetDatasetByXeniumId = new Map(targetDatasets.map((row) => [row.xenium_id, row.id]));
  let createdCount = 0;
  let skippedCount = 0;

  const chunks = splitIntoChunks(sourceHierarchies, 400);
  for (const chunk of chunks) {
    // eslint-disable-next-line no-restricted-syntax
    for (const sourceLink of chunk) {
      const sourceXeniumId = coerceIntegerId(sourceLink.source_id);
      const derivedXeniumId = coerceIntegerId(sourceLink.derived_id);
      if (!sourceXeniumId || !derivedXeniumId) {
        skippedCount += 1;
        continue;
      }

      const sourceId = targetDatasetByXeniumId.get(sourceXeniumId);
      const derivedId = targetDatasetByXeniumId.get(derivedXeniumId);
      if (!sourceId || !derivedId) {
        skippedCount += 1;
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      const existing = await prisma.dataset_hierarchy.findUnique({
        where: {
          source_id_derived_id: {
            source_id: sourceId,
            derived_id: derivedId,
          },
        },
      });
      if (existing) {
        skippedCount += 1;
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      await prisma.dataset_hierarchy.create({
        data: {
          source_id: sourceId,
          derived_id: derivedId,
          metadata: sourceLink.metadata || null,
        },
      });
      createdCount += 1;
    }
  }

  logger.info(
    `[XENIUM][sync_dataset_hierarchies] Complete (${createdCount} created, ${skippedCount} skipped)`,
  );
}

module.exports = { syncDatasetHierarchies };
