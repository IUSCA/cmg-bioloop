/**
 * CMG Sync - Role Mapper
 * 
 * Maps CMG roles to Bioloop roles
 */

const { ROLE_MAPPING } = require('../constants');
const logger = require('../logger');

/**
 * Map CMG roles to Bioloop role names
 * 
 * @param {Array<string>} cmgRoles - Array of CMG role names
 * @returns {Array<string>} Array of Bioloop role names
 */
function mapCMGRolesToBioloop(cmgRoles) {
  if (!Array.isArray(cmgRoles)) {
    logger.warn('[Role Mapper] Invalid CMG roles (not an array):', cmgRoles);
    return [];
  }
  
  const bioloopRoles = [];
  const uniqueRoles = new Set();
  
  for (const cmgRole of cmgRoles) {
    const bioloopRole = ROLE_MAPPING[cmgRole];
    
    if (bioloopRole) {
      uniqueRoles.add(bioloopRole);
    } else {
      logger.warn(`[Role Mapper] Unknown CMG role: ${cmgRole}, defaulting to 'user'`);
      uniqueRoles.add('user');
    }
  }
  
  return Array.from(uniqueRoles);
}

/**
 * Get Bioloop role IDs from CMG roles
 * 
 * @param {Array<string>} cmgRoles - Array of CMG role names
 * @param {Object} prisma - Prisma client or transaction
 * @returns {Promise<Array<number>>} Array of Bioloop role IDs
 */
async function getBioloopRoleIds(cmgRoles, prisma) {
  const bioloopRoleNames = mapCMGRolesToBioloop(cmgRoles);
  
  const roles = await prisma.role.findMany({
    where: {
      name: {
        in: bioloopRoleNames
      }
    }
  });
  
  return roles.map(role => role.id);
}

module.exports = {
  mapCMGRolesToBioloop,
  getBioloopRoleIds
};

