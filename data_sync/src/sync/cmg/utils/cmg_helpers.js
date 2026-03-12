/**
 * CMG Sync - CMG Helpers
 * 
 * Utility functions for working with CMG MongoDB documents
 */

const { ObjectId } = require('mongodb');
const logger = require('../../../logger');

/**
 * Convert MongoDB ObjectId to string
 * 
 * @param {ObjectId|string} objectId - MongoDB ObjectId or string
 * @returns {string} ObjectId as string
 */
function objectIdToString(objectId) {
  if (!objectId) {
    return null;
  }
  
  if (typeof objectId === 'string') {
    return objectId;
  }
  
  if (objectId instanceof ObjectId || objectId._bsontype === 'ObjectID') {
    return objectId.toString();
  }
  
  logger.warn('[CMG Helpers] Invalid ObjectId:', objectId);
  return null;
}

/**
 * Convert string to MongoDB ObjectId
 * 
 * @param {string} idString - ObjectId as string
 * @returns {ObjectId} MongoDB ObjectId
 */
function stringToObjectId(idString) {
  if (!idString) {
    return null;
  }
  
  try {
    return new ObjectId(idString);
  } catch (error) {
    logger.warn(`[CMG Helpers] Invalid ObjectId string: ${idString}`);
    return null;
  }
}

/**
 * Get nested path value from CMG document
 * 
 * @param {Object} doc - CMG document
 * @param {string} path - Dot notation path (e.g., "paths.archive")
 * @param {*} defaultValue - Default value if path doesn't exist
 * @returns {*} Value at path or default
 */
function getNestedValue(doc, path, defaultValue = null) {
  const keys = path.split('.');
  let value = doc;
  
  for (const key of keys) {
    if (value && typeof value === 'object' && key in value) {
      value = value[key];
    } else {
      return defaultValue;
    }
  }
  
  return value !== undefined ? value : defaultValue;
}

/**
 * Build CMG MongoDB query with cursor
 * 
 * @param {Object} cursor - Cursor object with last_updated_at and last_cmg_objectid
 * @param {Date} roundEnd - End timestamp for bounded window
 * @returns {Object} MongoDB query object
 */
function buildCursorQuery(cursor, roundEnd) {
  const query = {};
  
  if (cursor.last_updated_at) {
    query.$or = [
      { updatedAt: { $gt: cursor.last_updated_at } },
      {
        updatedAt: cursor.last_updated_at,
        _id: { $gt: new ObjectId(cursor.last_cmg_objectid) }
      }
    ];
  }
  
  if (roundEnd) {
    query.updatedAt = { ...query.updatedAt, $lte: roundEnd };
  }
  
  return query;
}

/**
 * Extract genomic attributes from CMG dataset/dataproduct
 * 
 * @param {Object} cmgDoc - CMG document
 * @returns {Object} {genome_type, genome_value}
 */
function extractGenomicAttributes(cmgDoc) {
  // CMG has inconsistent field names
  const genome_type = cmgDoc.genome_type || 
                     cmgDoc.genomeType || 
                     null;
  
  const genome_value = cmgDoc.genome_value || 
                      cmgDoc.genomeValue || 
                      cmgDoc.genome || 
                      null;
  
  return {
    genome_type,
    genome_value
  };
}

/**
 * Extract paths from CMG dataset/dataproduct
 * 
 * @param {Object} cmgDoc - CMG document
 * @returns {Object} {origin_path, archive_path, staged_path}
 */
function extractPaths(cmgDoc) {
  const paths = cmgDoc.paths || {};
  
  return {
    origin_path: paths.origin || null,
    archive_path: paths.archive || null,
    staged_path: paths.staged || null
  };
}

/**
 * Convert BigInt-compatible fields from CMG
 * 
 * @param {number|string} value - Numeric value
 * @returns {BigInt|null} BigInt value or null
 */
function toBigInt(value) {
  if (value === null || value === undefined) {
    return null;
  }
  
  try {
    return BigInt(value);
  } catch (error) {
    logger.warn(`[CMG Helpers] Failed to convert to BigInt: ${value}`);
    return null;
  }
}

/**
 * Expand CMG groups to user IDs
 * 
 * @param {Object} cmgDb - CMG MongoDB database
 * @param {Array<ObjectId|string>} groupIds - Array of group IDs
 * @returns {Promise<Array<string>>} Array of user ID strings
 */
async function expandGroupsToUserIds(cmgDb, groupIds) {
  if (!Array.isArray(groupIds) || groupIds.length === 0) {
    return [];
  }
  
  const objectIds = groupIds.map(id => 
    id instanceof ObjectId ? id : new ObjectId(id)
  );
  
  const groups = await cmgDb.collection('groups').find({
    _id: { $in: objectIds }
  }).toArray();
  
  const userIds = new Set();
  
  for (const group of groups) {
    // CMG groups can have 'users' or 'members' field
    const members = group.users || group.members || [];
    
    for (const userId of members) {
      userIds.add(objectIdToString(userId));
    }
  }
  
  return Array.from(userIds);
}

/**
 * Generate slug from name (for projects)
 * 
 * @param {Object} prisma - Prisma client
 * @param {string} name - Project name
 * @param {string} cmgId - CMG _id for uniqueness checking
 * @returns {Promise<string>} Unique slug
 */
async function generateSlug(prisma, name, cmgId) {
  const NATO_ALPHABET = [
    'alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf', 'hotel',
    'india', 'juliet', 'kilo', 'lima', 'mike', 'november', 'oscar', 'papa',
    'quebec', 'romeo', 'sierra', 'tango', 'uniform', 'victor', 'whiskey',
    'xray', 'yankee', 'zulu',
  ];
  
  // Normalize name
  const normalized = name
    .toLowerCase()
    .trim()
    .replace(/[\W_]+/g, '-')      // Replace non-alphanumeric with hyphen
    .replace(/-+/g, '-')           // Multiple hyphens to single
    .replace(/^-|-$/g, '');        // Remove leading/trailing hyphens
  
  // Check if normalized slug is unique (excluding this project)
  const isUnique = async (slug) => {
    const existing = await prisma.project.findFirst({
      where: {
        slug: slug,
        cmg_id: { not: cmgId },
      },
    });
    return !existing;
  };
  
  if (await isUnique(normalized)) {
    return normalized;
  }
  
  // Generate suffixed slugs using NATO alphabet
  let i = 0;
  const N = NATO_ALPHABET.length;
  
  while (true) {
    const suffix = i < N ? NATO_ALPHABET[i] : `${NATO_ALPHABET[i % N]}-${Math.floor(i / N)}`;
    const slug = `${normalized}-${suffix}`;
    
    if (await isUnique(slug)) {
      return slug;
    }
    
    i++;
    
    // Safety check to prevent infinite loop
    if (i > 10000) {
      throw new Error(`Failed to generate unique slug for project: ${name}`);
    }
  }
}

/**
 * Expand CMG groups to user IDs (alias for backward compatibility)
 * @deprecated Use expandGroupsToUserIds instead
 */
async function expandGroups(cmgDb, groupIds) {
  return expandGroupsToUserIds(cmgDb, groupIds);
}

/**
 * Check if CMG document has been updated since last sync
 * 
 * @param {Object} cmgDoc - CMG document
 * @param {Date} lastSyncTime - Last sync timestamp
 * @returns {boolean} True if document updated since last sync
 */
function isUpdatedSinceLastSync(cmgDoc, lastSyncTime) {
  if (!lastSyncTime || !cmgDoc.updatedAt) {
    return true; // Assume updated if no timestamps
  }
  
  return new Date(cmgDoc.updatedAt) > new Date(lastSyncTime);
}

module.exports = {
  objectIdToString,
  stringToObjectId,
  getNestedValue,
  buildCursorQuery,
  extractGenomicAttributes,
  extractPaths,
  toBigInt,
  expandGroupsToUserIds,
  expandGroups,
  generateSlug,
  isUpdatedSinceLastSync
};

