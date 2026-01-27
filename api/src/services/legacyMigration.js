/**
 * Legacy Migration Service
 *
 * Handles legacy dataset and session migration status queries and utilities.
 * Specifically for CMG datasets migrated from MongoDB that need hydration.
 */

const prisma = require('@/db');

/**
 * Check if a dataset has reached a specific migration state
 * @param {number} datasetId - The dataset ID
 * @param {string} state - The state to check for
 * @returns {Promise<boolean>} - True if the state exists
 */
async function hasReachedState(datasetId, state) {
  const stateRecord = await prisma.dataset_state.findFirst({
    where: {
      dataset_id: datasetId,
      state,
    },
  });
  return !!stateRecord;
}

/**
 * Get migration status information for a dataset
 * @param {number} datasetId - The dataset ID
 * @returns {Promise<Object>} - Migration status object
 */
async function getDatasetMigrationStatus(datasetId) {
  // Get the dataset to check if it's a legacy dataset
  const dataset = await prisma.dataset.findUnique({
    where: { id: datasetId },
    select: {
      id: true,
      cmg_id: true,
    },
  });

  if (!dataset) {
    throw new Error(`Dataset with ID ${datasetId} not found`);
  }

  // If no cmg_id, it's not a legacy dataset
  if (!dataset.cmg_id) {
    return {
      is_legacy: false,
      is_hydrated: false,
      is_validated: false,
      is_migrated: false,
      is_migration_initiated: false,
      is_retrieved: false,
      is_inspected: false,
      is_metadata_populated: false,
    };
  }

  // Check all migration states
  const [
    isMigrationInitiated,
    isRetrieved,
    isInspected,
    isMetadataPopulated,
    isValidated,
    isMigrated,
  ] = await Promise.all([
    hasReachedState(datasetId, 'MIGRATION_INITIATED'),
    hasReachedState(datasetId, 'RETRIEVED'),
    hasReachedState(datasetId, 'INSPECTED'),
    hasReachedState(datasetId, 'METADATA_POPULATED'),
    hasReachedState(datasetId, 'STAGED'),
    hasReachedState(datasetId, 'MIGRATED'),
  ]);

  return {
    is_legacy: true,
    is_migration_initiated: isMigrationInitiated,
    is_retrieved: isRetrieved,
    is_inspected: isInspected,
    is_metadata_populated: isMetadataPopulated,
    is_hydrated: isMetadataPopulated, // Hydration complete when metadata populated
    is_validated: isValidated,
    is_migrated: isMigrated,
  };
}

/**
 * Check if a dataset is currently undergoing migration
 * @param {number} datasetId - The dataset ID
 * @returns {Promise<boolean>} - True if migration is in progress
 */
async function isMigrationInProgress(datasetId) {
  const status = await getDatasetMigrationStatus(datasetId);

  // Migration is in progress if it's initiated but not completed
  return status.is_migration_initiated && !status.is_migrated;
}

/**
 * Get migration status information for a session
 * @param {number} sessionId - The session ID
 * @returns {Promise<Object>} - Migration status object
 */
async function getSessionMigrationStatus(sessionId) {
  // Get the session to check if it's a legacy session and hydration status
  const session = await prisma.genome_browser_session.findUnique({
    where: { id: sessionId },
    select: {
      id: true,
      cmg_id: true,
      metadata: true,
    },
  });

  if (!session) {
    throw new Error(`Session with ID ${sessionId} not found`);
  }

  // Session is hydrated if metadata.is_hydrated is true
  const isHydrated = session.metadata?.is_hydrated === true;

  return {
    is_legacy: !!session.cmg_id,
    is_hydrated: isHydrated,
  };
}

module.exports = {
  getDatasetMigrationStatus,
  getSessionMigrationStatus,
  isMigrationInProgress,
  hasReachedState,
};
