/**
 * Xenium Bigbang — Sync Users
 *
 * Reads user rows from the Xenium PostgreSQL source and upserts them into
 * cmg-bioloop's `user` table, setting `xenium_id` and `metadata.origin = 'legacy_xenium'`.
 *
 * Idempotent: checks for existing users by `xenium_id` and `username` before inserting.
 *
 * Implemented in Chat 2.
 */

const logger = require('../../../logger');
const { withDatasetOrigin, coerceIntegerId, splitIntoChunks, toDateOrNow } = require('./helpers');
const { mapXeniumRolesToBioloop } = require('../utils/role_mapper');

async function getSourceRoleNames(xeniumPrisma, sourceUserId) {
  const sourceUserRoles = await xeniumPrisma.user_role.findMany({
    where: { user_id: sourceUserId },
    select: { role_id: true },
  });

  const roleIds = sourceUserRoles
    .map((entry) => Number(entry.role_id))
    .filter((id) => Number.isInteger(id) && id > 0);

  if (roleIds.length === 0) return [];

  const sql = `SELECT id, name FROM "role" WHERE id IN (${roleIds.join(',')})`;
  const rows = await xeniumPrisma.$queryRawUnsafe(sql);
  return rows.map((row) => row.name).filter(Boolean);
}

async function syncUserRoles(prisma, xeniumPrisma, sourceUserId, targetUserId) {
  const sourceRoleNames = await getSourceRoleNames(xeniumPrisma, sourceUserId);
  const mappedRoleNames = mapXeniumRolesToBioloop(sourceRoleNames);

  const targetRoleRows = await prisma.role.findMany({
    where: { name: { in: mappedRoleNames } },
  });
  const targetRoleIds = new Set(targetRoleRows.map((role) => role.id));

  const existingLinks = await prisma.user_role.findMany({
    where: { user_id: targetUserId },
  });
  const existingRoleIds = new Set(existingLinks.map((entry) => entry.role_id));

  for (const roleId of targetRoleIds) {
    if (!existingRoleIds.has(roleId)) {
      // eslint-disable-next-line no-await-in-loop
      await prisma.user_role.create({
        data: { user_id: targetUserId, role_id: roleId },
      });
    }
  }

  for (const roleId of existingRoleIds) {
    if (!targetRoleIds.has(roleId)) {
      // eslint-disable-next-line no-await-in-loop
      await prisma.user_role.delete({
        where: { user_id_role_id: { user_id: targetUserId, role_id: roleId } },
      });
    }
  }
}

/**
 * Sync all users from Xenium PostgreSQL into Bioloop.
 *
 * Strategy:
 *   - Stream all non-deleted users from xeniumPrisma.user
 *   - For each: upsert into prisma.user where xenium_id matches OR username matches
 *   - Set metadata.origin = 'legacy_xenium'
 *   - Map xenium role names to Bioloop roles via role_mapper
 *   - Skip the xenium system admin user (will be handled by seed_constants)
 *
 * @param {PrismaClient} prisma       Target Bioloop database
 * @param {PrismaClient} xeniumPrisma Source Xenium database
 * @returns {Promise<void>}
 */
async function syncUsers(prisma, xeniumPrisma) {
  logger.info('[XENIUM][sync_users] Starting user synchronization');

  const sourceUsers = await xeniumPrisma.user.findMany({
    orderBy: { id: 'asc' },
    select: {
      id: true,
      username: true,
      email: true,
      name: true,
      cas_id: true,
      is_deleted: true,
      created_at: true,
      updated_at: true,
      metadata: true,
    },
  });
  logger.info(`[XENIUM][sync_users] Found ${sourceUsers.length} source users`);

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  const chunks = splitIntoChunks(sourceUsers, 200);
  for (const chunk of chunks) {
    // eslint-disable-next-line no-restricted-syntax
    for (const sourceUser of chunk) {
      const xeniumId = coerceIntegerId(sourceUser.id);
      if (!xeniumId) {
        skippedCount += 1;
        continue;
      }

      if (sourceUser.username === 'cmguser' || sourceUser.username === 'xeniumuser') {
        logger.warn(
          `[XENIUM][sync_users] Skipping reserved username "${sourceUser.username}" (xenium_id=${xeniumId})`,
        );
        skippedCount += 1;
        continue;
      }

      const metadata = withDatasetOrigin(sourceUser.metadata);
      const createData = {
        username: sourceUser.username || `xenium-user-${xeniumId}`,
        email: sourceUser.email || `xenium-user-${xeniumId}@system.local`,
        name: sourceUser.name || sourceUser.username || null,
        cas_id: sourceUser.cas_id || sourceUser.username || null,
        is_deleted: Boolean(sourceUser.is_deleted),
        xenium_id: xeniumId,
        created_at: toDateOrNow(sourceUser.created_at),
        updated_at: toDateOrNow(sourceUser.updated_at),
        metadata,
      };

      // eslint-disable-next-line no-await-in-loop
      let targetUser = await prisma.user.findFirst({
        where: {
          OR: [{ xenium_id: xeniumId }, { username: sourceUser.username }],
        },
      });

      if (!targetUser) {
        // eslint-disable-next-line no-await-in-loop
        targetUser = await prisma.user.create({ data: createData });
        createdCount += 1;
      } else {
        // eslint-disable-next-line no-await-in-loop
        await prisma.user.update({
          where: { id: targetUser.id },
          data: {
            email: createData.email,
            name: createData.name,
            cas_id: createData.cas_id,
            is_deleted: createData.is_deleted,
            xenium_id: xeniumId,
            metadata,
          },
        });
        updatedCount += 1;
      }

      // eslint-disable-next-line no-await-in-loop
      await syncUserRoles(prisma, xeniumPrisma, xeniumId, targetUser.id);
    }
  }

  logger.info(
    `[XENIUM][sync_users] Complete (${createdCount} created, ${updatedCount} updated, ${skippedCount} skipped)`,
  );
}

module.exports = { syncUsers };
