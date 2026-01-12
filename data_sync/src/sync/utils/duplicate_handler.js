/**
 * CMG Sync - Duplicate Handler
 * 
 * Handles duplicate dataset names by adding DUPLICATE_ prefix
 * Bioloop has UNIQUE constraint on (name, type, is_deleted)
 */

const logger = require('../logger');

const DUPLICATE_PREFIX = 'DUPLICATE';
const UNKNOWN_PREFIX = 'UNKNOWN';

/**
 * Check if dataset name exists in Bioloop
 * 
 * @param {Object} prisma - Prisma client or transaction
 * @param {string} name - Dataset name
 * @param {string} type - Dataset type (RAW_DATA or DATA_PRODUCT)
 * @param {boolean} isDeleted - Is deleted flag
 * @returns {Promise<boolean>} True if name exists
 */
async function nameExists(prisma, name, type, isDeleted = false) {
  const existing = await prisma.dataset.findFirst({
    where: {
      name,
      type,
      is_deleted: isDeleted
    }
  });
  
  return existing !== null;
}

/**
 * Handle duplicate dataset name by generating unique name
 * 
 * @param {Object} prisma - Prisma client or transaction
 * @param {string} originalName - Original dataset name
 * @param {string} type - Dataset type (RAW_DATA or DATA_PRODUCT)
 * @param {boolean} isDeleted - Is deleted flag
 * @returns {Promise<string>} Unique dataset name
 */
async function handleDuplicateName(prisma, originalName, type, isDeleted = false) {
  if (!originalName) {
    throw new Error('Dataset name must be specified');
  }
  
  if (!type) {
    throw new Error('Dataset type must be specified');
  }
  
  logger.debug(`[Duplicate Handler] Checking for duplicate: ${originalName}`);
  
  let newName = `${DUPLICATE_PREFIX}_${originalName}`;
  let count = 1;
  
  while (await nameExists(prisma, newName, type, isDeleted)) {
    count++;
    newName = `${DUPLICATE_PREFIX}_${count}_${originalName}`;
    logger.debug(`[Duplicate Handler] Trying: ${newName}`);
  }
  
  logger.info(`[Duplicate Handler] Resolved duplicate: ${originalName} → ${newName}`);
  return newName;
}

/**
 * Handle unknown dataset name (missing name field)
 * 
 * @param {Object} prisma - Prisma client or transaction
 * @param {string} type - Dataset type
 * @param {boolean} isDeleted - Is deleted flag
 * @returns {Promise<string>} Generated name for unknown dataset
 */
async function handleUnknownName(prisma, type, isDeleted = false) {
  let newName = UNKNOWN_PREFIX;
  let count = 1;
  
  while (await nameExists(prisma, newName, type, isDeleted)) {
    count++;
    newName = `${UNKNOWN_PREFIX}-${count}`;
  }
  
  logger.warn(`[Duplicate Handler] Assigned name for dataset without name: ${newName}`);
  return newName;
}

/**
 * Get unique dataset name for CMG dataset
 * Handles both missing names and duplicates
 * 
 * @param {Object} prisma - Prisma client or transaction
 * @param {Object} cmgDataset - CMG dataset document
 * @param {string} type - Dataset type (RAW_DATA or DATA_PRODUCT)
 * @param {boolean} isDeleted - Is deleted flag
 * @returns {Promise<string>} Unique dataset name
 */
async function getUniqueDatasetName(prisma, cmgDataset, type, isDeleted = false) {
  // Handle missing name
  if (!cmgDataset.name) {
    return await handleUnknownName(prisma, type, isDeleted);
  }
  
  const originalName = cmgDataset.name;
  
  // Check if name is already unique
  if (!(await nameExists(prisma, originalName, type, isDeleted))) {
    return originalName;
  }
  
  // Handle duplicate
  return await handleDuplicateName(prisma, originalName, type, isDeleted);
}

/**
 * Check if a name has been modified (has DUPLICATE or UNKNOWN prefix)
 * 
 * @param {string} name - Dataset name
 * @returns {boolean} True if name has been modified
 */
function isModifiedName(name) {
  return name.startsWith(DUPLICATE_PREFIX) || name.startsWith(UNKNOWN_PREFIX);
}

/**
 * Get original name from modified name
 * 
 * @param {string} modifiedName - Modified dataset name
 * @returns {string} Original name (or modified name if not a duplicate)
 */
function getOriginalName(modifiedName) {
  if (!modifiedName) {
    return modifiedName;
  }
  
  // Remove DUPLICATE_ prefix
  if (modifiedName.startsWith(`${DUPLICATE_PREFIX}_`)) {
    const withoutPrefix = modifiedName.substring(DUPLICATE_PREFIX.length + 1);
    
    // Remove DUPLICATE_N_ pattern
    const match = withoutPrefix.match(/^\d+_(.+)$/);
    if (match) {
      return match[1];
    }
    
    return withoutPrefix;
  }
  
  // UNKNOWN names don't have an original
  if (modifiedName.startsWith(UNKNOWN_PREFIX)) {
    return modifiedName;
  }
  
  return modifiedName;
}

module.exports = {
  nameExists,
  handleDuplicateName,
  handleUnknownName,
  getUniqueDatasetName,
  isModifiedName,
  getOriginalName,
  DUPLICATE_PREFIX,
  UNKNOWN_PREFIX
};

