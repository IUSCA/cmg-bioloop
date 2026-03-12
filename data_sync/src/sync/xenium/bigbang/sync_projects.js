/**
 * Xenium Bigbang — Sync Projects
 *
 * Reads project rows from the Xenium source and inserts them into
 * cmg-bioloop's `project` table, along with project_user and project_dataset
 * associations.
 *
 * Key differences from CMG sync:
 *   - Source is PostgreSQL (xeniumPrisma), not MongoDB
 *   - Sets `xenium_id` on each project
 *   - Sets `metadata.origin = 'legacy_xenium'`
 *   - Must resolve user_id references via xenium_id lookups
 *
 * Idempotent: checks by `xenium_id` and `slug` before inserting.
 *
 * Implemented in Chat 2.
 */

const logger = require('../../../logger');
const {
  withDatasetOrigin,
  coerceIntegerId,
  splitIntoChunks,
  toDateOrNow,
  generateUniqueProjectSlug,
} = require('./helpers');

async function syncProjectAssociations(prisma, xeniumPrisma, sourceProjectId, targetProjectId, maps) {
  const [sourceProjectUsers, sourceProjectDatasets] = await Promise.all([
    xeniumPrisma.project_user.findMany({ where: { project_id: sourceProjectId } }),
    xeniumPrisma.project_dataset.findMany({ where: { project_id: sourceProjectId } }),
  ]);

  const resolvedProjectUsers = sourceProjectUsers
    .map((entry) => maps.userByXeniumId.get(coerceIntegerId(entry.user_id)))
    .filter(Boolean)
    .map((userId) => ({ project_id: targetProjectId, user_id: userId }));

  const resolvedProjectDatasets = sourceProjectDatasets
    .map((entry) => maps.datasetByXeniumId.get(coerceIntegerId(entry.dataset_id)))
    .filter(Boolean)
    .map((datasetId) => ({ project_id: targetProjectId, dataset_id: datasetId }));

  await prisma.project_user.deleteMany({ where: { project_id: targetProjectId } });
  await prisma.project_dataset.deleteMany({ where: { project_id: targetProjectId } });

  if (resolvedProjectUsers.length > 0) {
    await prisma.project_user.createMany({
      data: resolvedProjectUsers,
      skipDuplicates: true,
    });
  }
  if (resolvedProjectDatasets.length > 0) {
    await prisma.project_dataset.createMany({
      data: resolvedProjectDatasets,
      skipDuplicates: true,
    });
  }
}

/**
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function syncProjects(prisma, xeniumPrisma) {
  logger.info('[XENIUM][sync_projects] Starting project synchronization');

  const [sourceProjects, targetUsers, targetDatasets] = await Promise.all([
    xeniumPrisma.project.findMany({ orderBy: { updated_at: 'asc' } }),
    prisma.user.findMany({
      where: { xenium_id: { not: null } },
      select: { id: true, xenium_id: true },
    }),
    prisma.dataset.findMany({
      where: { xenium_id: { not: null } },
      select: { id: true, xenium_id: true },
    }),
  ]);

  const maps = {
    userByXeniumId: new Map(targetUsers.map((row) => [row.xenium_id, row.id])),
    datasetByXeniumId: new Map(targetDatasets.map((row) => [row.xenium_id, row.id])),
  };

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  const chunks = splitIntoChunks(sourceProjects, 100);
  for (const chunk of chunks) {
    // eslint-disable-next-line no-restricted-syntax
    for (const sourceProject of chunk) {
      const xeniumId = coerceIntegerId(sourceProject.id);
      if (!xeniumId) {
        skippedCount += 1;
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      let targetProject = await prisma.project.findFirst({
        where: { xenium_id: xeniumId },
      });

      const metadata = withDatasetOrigin(sourceProject.metadata);
      const ownerId = maps.userByXeniumId.get(coerceIntegerId(sourceProject.owner_id)) || null;
      const name = sourceProject.name || `xenium-project-${xeniumId}`;
      // eslint-disable-next-line no-await-in-loop
      const slug = targetProject
        ? targetProject.slug
        : await generateUniqueProjectSlug(prisma, name, xeniumId);

      if (!targetProject) {
        // eslint-disable-next-line no-await-in-loop
        targetProject = await prisma.project.create({
          data: {
            xenium_id: xeniumId,
            slug,
            name,
            description: sourceProject.description || null,
            funding: sourceProject.funding || null,
            browser_enabled: Boolean(sourceProject.browser_enabled),
            owner_id: ownerId,
            created_at: toDateOrNow(sourceProject.created_at),
            updated_at: toDateOrNow(sourceProject.updated_at),
            metadata,
          },
        });
        createdCount += 1;
      } else {
        // eslint-disable-next-line no-await-in-loop
        await prisma.project.update({
          where: { id: targetProject.id },
          data: {
            name,
            description: sourceProject.description || null,
            funding: sourceProject.funding || null,
            browser_enabled: Boolean(sourceProject.browser_enabled),
            owner_id: ownerId,
            metadata,
          },
        });
        updatedCount += 1;
      }

      // eslint-disable-next-line no-await-in-loop
      await syncProjectAssociations(prisma, xeniumPrisma, sourceProject.id, targetProject.id, maps);
    }
  }

  logger.info(
    `[XENIUM][sync_projects] Complete (${createdCount} created, ${updatedCount} updated, ${skippedCount} skipped)`,
  );
}

module.exports = { syncProjects };
