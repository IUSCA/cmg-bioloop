/**
 * Xenium User Roles Poller
 *
 * Watches the Xenium `user` table for role assignment changes and syncs them
 * into cmg-bioloop's `user_role` table.
 *
 * Cursor: tracks `user.updated_at` + `user.id` (integer).
 *
 * Implemented in Chat 3.
 */

const logger = require('../../../logger');
const XeniumBasePoller = require('./base_poller');
const { XENIUM_POLLER_NAMES } = require('../constants');
const { mapXeniumRolesToBioloop } = require('../utils/role_mapper');

class XeniumUserRolesPoller extends XeniumBasePoller {
  constructor(prisma, xeniumPrisma, options = {}) {
    super(XENIUM_POLLER_NAMES.USER_ROLES, prisma, xeniumPrisma, options);
  }

  getSourceModel() {
    return 'user';
  }

  /**
   * Sync role changes for a single xenium user row.
   *
   * Strategy:
   *   - Find the Bioloop user where xenium_id = row.id
   *   - If not found, skip with warning (user not migrated yet)
   *   - Compare current Bioloop user roles against xenium roles
   *   - Add/remove user_role rows to match xenium's current role set
   *   - Map xenium role names to Bioloop role names
   *
   * @param {Object} row - Xenium user row
   * @param {Object} tx  - Target Prisma transaction
   */
  async processRow(row, tx) {
    const bioloopUser = await tx.user.findFirst({
      where: { xenium_id: row.id },
      include: {
        user_role: {
          include: { roles: true },
        },
      },
    });

    if (!bioloopUser) {
      logger.debug(`[${this.pollerName}] User not found for xenium_id=${row.id}, skipping`);
      return;
    }

    const newIsDeleted = typeof row.is_deleted === 'boolean'
      ? row.is_deleted
      : !row.active;

    if (typeof newIsDeleted === 'boolean' && bioloopUser.is_deleted !== newIsDeleted) {
      await tx.user.update({
        where: { id: bioloopUser.id },
        data: { is_deleted: newIsDeleted },
      });
    }

    const xeniumUserRoles = await this.xeniumPrisma.user_role.findMany({
      where: { user_id: row.id },
      include: { roles: true },
    });
    const targetRoleNames = mapXeniumRolesToBioloop(
      xeniumUserRoles.map((r) => r.roles?.name).filter(Boolean),
    );

    const currentRoleNames = bioloopUser.user_role.map((r) => r.roles?.name).filter(Boolean);
    const rolesToAdd = targetRoleNames.filter((name) => !currentRoleNames.includes(name));
    const rolesToRemove = currentRoleNames.filter((name) => !targetRoleNames.includes(name));

    for (const roleName of rolesToAdd) {
      // eslint-disable-next-line no-await-in-loop
      const role = await tx.role.findFirst({ where: { name: roleName } });
      if (!role) continue;
      // eslint-disable-next-line no-await-in-loop
      await tx.user_role.upsert({
        where: {
          user_id_role_id: {
            user_id: bioloopUser.id,
            role_id: role.id,
          },
        },
        create: {
          user_id: bioloopUser.id,
          role_id: role.id,
        },
        update: {},
      });
    }

    for (const roleName of rolesToRemove) {
      // eslint-disable-next-line no-await-in-loop
      const role = await tx.role.findFirst({ where: { name: roleName } });
      if (!role) continue;
      // eslint-disable-next-line no-await-in-loop
      await tx.user_role.deleteMany({
        where: {
          user_id: bioloopUser.id,
          role_id: role.id,
        },
      });
    }
  }
}

module.exports = XeniumUserRolesPoller;
