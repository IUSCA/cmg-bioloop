const config = require('config');
const path = require('path');
const logger = require('@/services/logger');

/**
 * Path Resolver Service
 *
 * Handles path resolution for file serving in different environments:
 * - Production: Files are mounted inside container at a different path
 * - Docker (local): Files are accessed directly via DATA_ROOT
 */

const MODE = config.get('mode');
const DATA_ROOT = config.get('data_root');

/**
 * Get the base directory for file access based on environment
 *
 * In production:
 *   - Database stores host paths: /N/scratch/cmguser/cmg-bioloop/stage/... or /N/project/CMG-SCA/cmg-bioloop/stage/...
 *   - Container accesses via mount: /opt/sca/scratch/ingestion_source_dir/cmguser/cmg-bioloop/stage/...
 *   - We construct: FILESYSTEM_MOUNT_DIR_SCRATCH + system_user.username
 *
 * In docker (local dev):
 *   - Use DATA_ROOT directly
 *
 * @returns {string} Base directory path for file access
 */
function getFileAccessRoot() {
  if (MODE === 'production') {
    // Production: Use mounted path inside container
    const mountDir = config.get('filesystem.mount_dir.slateScratch') || '';
    const systemUser = config.get('system_user.username') || '';

    if (!mountDir || !systemUser) {
      logger.error('[PathResolver] Missing configuration in production mode', {
        mountDir,
        systemUser,
      });
      throw new Error('Missing filesystem mount configuration for production');
    }

    const accessRoot = path.join(mountDir, systemUser);
    logger.info('[PathResolver] Production mode - using mounted path', { accessRoot });
    return accessRoot;
  }

  // Docker/local: Use DATA_ROOT directly
  logger.info('[PathResolver] Docker mode - using DATA_ROOT', { dataRoot: DATA_ROOT });
  return DATA_ROOT;
}

/**
 * Resolve a staged_path from the database to the actual file system path
 * that the API container can access
 *
 * @param {string} stagedPath - Absolute path from database (host path)
 * @returns {string} Resolved path accessible by the container
 */
function resolveHostPathToContainerPath(stagedPath) {
  if (!stagedPath) {
    return '';
  }

  if (MODE === 'production') {
    // In production, staged_path is a host path like:
    // /N/scratch/cmguser/cmg-bioloop/stage/data_products/...
    // /N/project/CMG-SCA/cmg-bioloop/stage/raw_data/...

    // We need to convert it to container path:
    // /opt/sca/scratch/ingestion_source_dir/cmguser/cmg-bioloop/stage/data_products/...

    const mountDir = config.get('filesystem.mount_dir.slateScratch') || '';
    const baseDir = config.get('filesystem.base_dir.slateScratch') || '';
    const systemUser = config.get('system_user.username') || '';

    // Remove the base_dir prefix (e.g., /N/scratch or /N/project)
    let relativePath = stagedPath;
    if (baseDir && relativePath.startsWith(baseDir)) {
      relativePath = relativePath.substring(baseDir.length);
    }

    // Remove leading slashes and system username if present
    relativePath = relativePath.replace(/^\/+/, '');
    if (relativePath.startsWith(`${systemUser}/`)) {
      relativePath = relativePath.substring(systemUser.length + 1);
    }

    // Construct container path: mountDir + systemUser + relativePath
    const containerPath = path.join(mountDir, systemUser, relativePath);

    logger.debug('[PathResolver] Converted host path to container path', {
      stagedPath,
      containerPath,
    });

    return containerPath;
  }

  // Docker/local: Return as-is
  return stagedPath;
}

/**
 * Convert dataset type to filesystem folder name
 * @param {string} datasetType - Dataset type (e.g., "raw_data", "data_product")
 * @returns {string} Folder name used in filesystem
 */
function getDatasetTypeFolder(datasetType) {
  if (!datasetType) return '';

  const normalized = datasetType.toLowerCase().trim();

  // Map dataset types from config to their filesystem folder names
  const datasetTypes = config.get('dataset_types') || [];
  const folderMap = {};

  datasetTypes.forEach((type) => {
    const lowerType = type.toLowerCase();
    // Handle special case: DATA_PRODUCT -> data_products (plural in filesystem)
    if (lowerType === 'data_product') {
      folderMap[lowerType] = 'data_products';
    } else {
      folderMap[lowerType] = lowerType;
    }
  });

  return folderMap[normalized] || normalized;
}

/**
 * Construct the relative path for file exposure API
 *
 * In production: Database paths already contain the full structure including dataset_type,
 *                so we just convert mount points and make it relative
 * In docker:     Database paths may need dataset_type folder inserted
 *
 * @param {Object} params
 * @param {Object} params.dataset - Dataset object with staged_path and type
 * @param {Object} params.datasetFile - Dataset file object with path
 * @returns {string} Relative path for URL construction
 */
function getRelativeFilePath({ dataset, datasetFile }) {
  const stagedPath = dataset.staged_path || '';
  const filePath = datasetFile.path || '';
  const datasetType = dataset.type || '';

  logger.debug('[PathResolver] getRelativeFilePath input', {
    stagedPath,
    filePath,
    datasetType,
  });

  // Get dataset type folder (e.g., "data_products", "raw_data")
  const datasetTypeFolder = getDatasetTypeFolder(datasetType);

  // Convert host path to container path if needed
  const containerPath = resolveHostPathToContainerPath(stagedPath);

  // Get the file access root for the current environment
  const accessRoot = getFileAccessRoot();

  // Make path relative to access root
  let relativePath = containerPath;
  if (relativePath.startsWith(accessRoot)) {
    relativePath = relativePath.substring(accessRoot.length);
  }

  // Remove leading slashes
  relativePath = relativePath.replace(/^\/+/, '');

  // In docker mode, ensure dataset_type folder is present at the beginning
  // In production mode, the path from database already has the correct structure
  if (MODE !== 'production') {
    // Only check/add dataset_type in non-production environments
    if (!relativePath.startsWith(`${datasetTypeFolder}/`) && !relativePath.startsWith(datasetTypeFolder)) {
      // Insert dataset_type folder at the beginning
      if (datasetTypeFolder) {
        relativePath = `${datasetTypeFolder}/${relativePath}`;
      }
    }
  }

  // Append file path if present
  const cleanedFilePath = filePath.replace(/^\/+/, '');
  if (cleanedFilePath) {
    relativePath = `${relativePath}/${cleanedFilePath}`;
  }

  // Clean up any double slashes
  relativePath = relativePath.replace(/\/+/g, '/');

  logger.debug('[PathResolver] getRelativeFilePath result', {
    relativePath,
  });

  return relativePath;
}

/**
 * Resolve a relative path to an absolute path that the container can access
 *
 * @param {string} relativePath - Relative path (with dataset_type folder)
 * @returns {string} Absolute path in the container
 */
function resolveToAbsolutePath(relativePath) {
  const accessRoot = getFileAccessRoot();
  const absolutePath = path.join(accessRoot, relativePath);

  logger.debug('[PathResolver] Resolved to absolute path', {
    relativePath,
    accessRoot,
    absolutePath,
  });

  return absolutePath;
}

module.exports = {
  getFileAccessRoot,
  resolveHostPathToContainerPath,
  getDatasetTypeFolder,
  getRelativeFilePath,
  resolveToAbsolutePath,
};
