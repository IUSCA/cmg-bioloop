/**
 * Xenium Bigbang — Sync Datasets
 *
 * Reads RAW_DATA and DATA_PRODUCT dataset rows from the Xenium PostgreSQL source
 * and inserts them into cmg-bioloop's `dataset` table.
 *
 * Key differences from CMG dataset sync:
 *   - Source is PostgreSQL (xeniumPrisma), not MongoDB
 *   - Sets `xenium_id` (not cmg_id) on each inserted row
 *   - Sets `metadata.origin = 'legacy_xenium'`
 *   - DATA_PRODUCT datasets may have `metadata.analysis_summary_file_dir` — preserve it
 *   - No `cmg_id` is set on xenium datasets
 *
 * Idempotent: checks by `xenium_id` and `[name, type]` before inserting.
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

function toBigIntOrNull(value) {
  if (value === null || value === undefined) return null;
  try {
    return BigInt(value);
  } catch (error) {
    return null;
  }
}

async function ensureUniqueDatasetName(prisma, desiredName, datasetType, isDeleted) {
  const baseName = String(desiredName || '').trim() || 'UNKNOWN';
  let candidate = baseName;
  let suffix = 1;
  const maxAttempts = 1000;

  while (suffix <= maxAttempts) {
    // eslint-disable-next-line no-await-in-loop
    const existing = await prisma.dataset.findFirst({
      where: {
        name: candidate,
        type: datasetType,
        is_deleted: Boolean(isDeleted),
      },
      select: { id: true },
    });
    if (!existing) return candidate;
    candidate = `${baseName}--xenium-${suffix}`;
    suffix += 1;
  }

  throw new Error(
    `[XENIUM][sync_datasets] Failed to generate unique dataset name `
    + `for base="${baseName}", type="${datasetType}" after ${maxAttempts} attempts`,
  );
}

async function resolveAnalysisTypeId(prisma, sourceDataset) {
  if (!sourceDataset.analysis_type?.name || !sourceDataset.analysis_type?.extension) {
    return null;
  }

  const name = sourceDataset.analysis_type.name;
  const extension = sourceDataset.analysis_type.extension;

  let targetAnalysisType = await prisma.analysis_type.findFirst({
    where: {
      name: { equals: name, mode: 'insensitive' },
      extension: { equals: extension, mode: 'insensitive' },
    },
  });

  if (!targetAnalysisType) {
    targetAnalysisType = await prisma.analysis_type.create({
      data: {
        name,
        extension,
        metadata: withDatasetOrigin(sourceDataset.analysis_type.metadata),
      },
    });
  }

  return targetAnalysisType.id;
}

/**
 * Sync all datasets (RAW_DATA + DATA_PRODUCT) from Xenium into Bioloop.
 *
 * Strategy:
 *   - Query xeniumPrisma.dataset for all non-deleted datasets, ordered by id
 *   - For each dataset:
 *     - Check if already exists in prisma.dataset where xenium_id = dataset.id
 *     - If not, check for name+type collision and handle duplicates
 *     - Insert with xenium_id set, metadata.origin = 'legacy_xenium', and all other
 *       fields mapped directly (origin_path, archive_path, is_staged, etc.)
 *     - Preserve metadata JSONB including analysis_summary_file_dir on DATA_PRODUCTs
 *   - Process in batches of 50 to avoid memory pressure
 *
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function syncAllDatasets(prisma, xeniumPrisma) {
  logger.info('[XENIUM][sync_datasets] Starting dataset synchronization');

  const sourceDatasets = await xeniumPrisma.dataset.findMany({
    where: { type: { in: ['RAW_DATA', 'DATA_PRODUCT'] } },
    include: {
      analysis_type: true,
      genomic_details: true,
    },
    orderBy: { id: 'asc' },
  });

  logger.info(`[XENIUM][sync_datasets] Found ${sourceDatasets.length} source datasets`);

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  const chunks = splitIntoChunks(sourceDatasets, 100);
  for (const chunk of chunks) {
    // eslint-disable-next-line no-restricted-syntax
    for (const sourceDataset of chunk) {
      const xeniumId = coerceIntegerId(sourceDataset.id);
      if (!xeniumId) {
        skippedCount += 1;
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      const analysisTypeId = await resolveAnalysisTypeId(prisma, sourceDataset);
      const metadata = withDatasetOrigin(sourceDataset.metadata);
      const baseName = String(sourceDataset.name || '').trim() || `UNKNOWN-${xeniumId}`;

      // eslint-disable-next-line no-await-in-loop
      let targetDataset = await prisma.dataset.findFirst({
        where: { xenium_id: xeniumId },
      });

      if (!targetDataset) {
        // eslint-disable-next-line no-await-in-loop
        const uniqueName = await ensureUniqueDatasetName(
          prisma,
          baseName,
          sourceDataset.type,
          sourceDataset.is_deleted,
        );
        // eslint-disable-next-line no-await-in-loop
        targetDataset = await prisma.dataset.create({
          data: {
            name: uniqueName,
            type: sourceDataset.type,
            is_deleted: Boolean(sourceDataset.is_deleted),
            xenium_id: xeniumId,
            description: sourceDataset.description || null,
            num_directories: sourceDataset.num_directories || 0,
            num_files: sourceDataset.num_files || 0,
            du_size: toBigIntOrNull(sourceDataset.du_size),
            size: toBigIntOrNull(sourceDataset.size),
            bundle_size: toBigIntOrNull(sourceDataset.bundle_size),
            created_at: toDateOrNow(sourceDataset.created_at),
            updated_at: toDateOrNow(sourceDataset.updated_at),
            origin_path: sourceDataset.origin_path || null,
            archive_path: sourceDataset.archive_path || null,
            staged_path: sourceDataset.staged_path || null,
            is_staged: Boolean(sourceDataset.is_staged),
            create_method: sourceDataset.create_method || null,
            file_type: sourceDataset.file_type || null,
            analysis_type_id: analysisTypeId,
            metadata,
          },
        });
        createdCount += 1;
      } else {
        // eslint-disable-next-line no-await-in-loop
        await prisma.dataset.update({
          where: { id: targetDataset.id },
          data: {
            description: sourceDataset.description || null,
            num_directories: sourceDataset.num_directories || 0,
            num_files: sourceDataset.num_files || 0,
            du_size: toBigIntOrNull(sourceDataset.du_size),
            size: toBigIntOrNull(sourceDataset.size),
            bundle_size: toBigIntOrNull(sourceDataset.bundle_size),
            origin_path: sourceDataset.origin_path || null,
            archive_path: sourceDataset.archive_path || null,
            staged_path: sourceDataset.staged_path || null,
            is_deleted: Boolean(sourceDataset.is_deleted),
            is_staged: Boolean(sourceDataset.is_staged),
            create_method: sourceDataset.create_method || null,
            file_type: sourceDataset.file_type || null,
            analysis_type_id: analysisTypeId,
            metadata,
          },
        });
        updatedCount += 1;
      }

      if (sourceDataset.genomic_details) {
        // eslint-disable-next-line no-await-in-loop
        await prisma.dataset_genomic_attributes.upsert({
          where: { dataset_id: targetDataset.id },
          create: {
            dataset_id: targetDataset.id,
            genome_type: sourceDataset.genomic_details.genome_type || null,
            genome_value: sourceDataset.genomic_details.genome_value || null,
          },
          update: {
            genome_type: sourceDataset.genomic_details.genome_type || null,
            genome_value: sourceDataset.genomic_details.genome_value || null,
          },
        });
      }
    }
  }

  logger.info(
    `[XENIUM][sync_datasets] Complete (${createdCount} created, ${updatedCount} updated, ${skippedCount} skipped)`,
  );
}

module.exports = { syncAllDatasets };
