const BasePoller = require('./base_poller');
const { mapCMGRolesToBioloop } = require('../utils/role_mapper');
const logger = require('../../logger');

/**
 * User Roles Poller
 * 
 * Polls CMG users collection for role changes.
 * Updates user metadata (NOT identity fields) and syncs roles.
 * 
 * Immutable fields (never updated after big-bang):
 * - username
 * - name
 * - email
 * - cas_id
 */
class UserRolesPoller extends BasePoller {
  constructor(prisma, cmgDb, options = {}) {
    super('user_roles', prisma, cmgDb, {
      pollIntervalMs: options.pollIntervalMs || 10000, // 10 seconds
      batchSize: options.batchSize || 200,
      ...options,
    });
  }
  
  getCollectionName() {
    return 'users';
  }
  
  /**
   * Process a single user document
   * Updates: is_deleted status, roles
   * Does NOT update: username, name, email, cas_id
   */
  async processDocument(cmgUser, tx) {
    // Find user by cmg_id
    const bioloopUser = await tx.user.findFirst({
      where: { cmg_id: cmgUser._id.toString() },
    });
    
    if (!bioloopUser) {
      logger.debug(`[${this.pollerName}] User not found for CMG ID: ${cmgUser._id}, skipping`);
      return;
    }
    
    // Update metadata only (NOT identity fields)
    const existingMetadata = bioloopUser.metadata || {};
    
    await tx.user.update({
      where: { id: bioloopUser.id },
      data: {
        is_deleted: !cmgUser.active,
        metadata: {
          ...existingMetadata,
          cmg_sync_state: {
            cmg_updated_at: cmgUser.updatedAt,
            last_sync_time: new Date(),
          },
        },
      },
    });
    
    // Sync roles (diff-based: add missing, remove extra)
    await this.syncUserRoles(tx, bioloopUser.id, cmgUser.roles || []);
  }
  
  /**
   * Sync user roles (diff-based)
   * Add roles that are missing, remove roles that shouldn't be there
   */
  async syncUserRoles(tx, userId, cmgRoles) {
    // Map CMG roles to Bioloop roles
    const targetRoleNames = mapCMGRolesToBioloop(cmgRoles);
    
    // Get current roles
    const existingUserRoles = await tx.user_role.findMany({
      where: { user_id: userId },
      include: { role: true },
    });
    
    const existingRoleNames = existingUserRoles.map(ur => ur.role.name);
    
    // Add missing roles
    const rolesToAdd = targetRoleNames.filter(name => !existingRoleNames.includes(name));
    for (const roleName of rolesToAdd) {
      const role = await tx.role.findFirst({ where: { name: roleName } });
      if (role) {
        await tx.user_role.create({
          data: {
            user_id: userId,
            role_id: role.id,
          },
        });
        logger.debug(`[${this.pollerName}] Added role ${roleName} to user ${userId}`);
      }
    }
    
    // Remove extra roles
    const rolesToRemove = existingRoleNames.filter(name => !targetRoleNames.includes(name));
    for (const roleName of rolesToRemove) {
      const userRole = existingUserRoles.find(ur => ur.role.name === roleName);
      if (userRole) {
        await tx.user_role.delete({
          where: { id: userRole.id },
        });
        logger.debug(`[${this.pollerName}] Removed role ${roleName} from user ${userId}`);
      }
    }
  }
}

module.exports = UserRolesPoller;

