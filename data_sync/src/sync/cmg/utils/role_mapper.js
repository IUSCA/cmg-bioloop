/**
 * CMG Sync - Role Mapper
 * 
 * Maps CMG roles to Bioloop roles
 */

const { ROLE_MAPPING } = require('../constants');
const logger = require('../../../logger');

/**
 * Map CMG roles to Bioloop role names
 * 
 * @param {Array<string>} cmgRoles - Array of CMG role names (or corrupted string)
 * @returns {Array<string>} Array of Bioloop role names
 */
function mapCMGRolesToBioloop(cmgRoles) {
  // Handle case where CMG stored roles as a JSON string instead of array
  if (!Array.isArray(cmgRoles)) {
    const typeOfRoles = typeof cmgRoles;
    
    // Try to parse if it's a string that looks like a JSON array
    if (typeOfRoles === 'string') {
      const trimmed = cmgRoles.trim();
      
      // Try parsing as JSON (handles cases like '["user"]' or "[\"user\"]")
      if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
        try {
          // Try with double quotes first
          const parsed = JSON.parse(trimmed);
          if (Array.isArray(parsed)) {
            logger.info(`[Role Mapper] Recovered roles from JSON string: ${trimmed} → [${parsed.join(', ')}]`);
            return mapCMGRolesToBioloop(parsed);
          }
        } catch (e) {
          // Try converting single quotes to double quotes for malformed JSON
          try {
            const fixedJson = trimmed.replace(/'/g, '"');
            const parsed = JSON.parse(fixedJson);
            if (Array.isArray(parsed)) {
              logger.info(`[Role Mapper] Recovered roles from malformed JSON: ${trimmed} → [${parsed.join(', ')}]`);
              return mapCMGRolesToBioloop(parsed);
            }
          } catch (e2) {
            logger.warn(`[Role Mapper] Could not parse array-like string: "${trimmed}", defaulting to 'user'`);
            return ['user'];
          }
        }
      }
      
      // Not an array string, treat as single role name
      if (trimmed && ROLE_MAPPING[trimmed]) {
        logger.info(`[Role Mapper] Treating string as single role: "${trimmed}" → "${ROLE_MAPPING[trimmed]}"`);
        return [ROLE_MAPPING[trimmed]];
      }
      if (trimmed) {
        logger.warn(`[Role Mapper] Unknown role string: "${trimmed}", defaulting to 'user'`);
        return ['user'];
      }
    }
    
    // For undefined, null, or other invalid types
    const rolesValue = cmgRoles === undefined ? 'undefined' : 
                       cmgRoles === null ? 'null' : 
                       JSON.stringify(cmgRoles).substring(0, 200);
    logger.warn(`[Role Mapper] Invalid CMG roles (not an array or parseable string) - Type: ${typeOfRoles}, Value: ${rolesValue}`);
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

