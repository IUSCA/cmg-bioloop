/**
 * CMG Legacy Migration Service
 *
 * Handles migration status queries for datasets, sessions, and conversions
 * that were migrated from the legacy CMG MongoDB database into Bioloop.
 *
 * A CMG legacy object is identified by metadata.origin === 'legacy'.
 */

const prisma = require('@/db');

/**
 * Check if a dataset is a legacy CMG dataset (origin: 'legacy').
 * @param {Object} dataset
 * @returns {boolean}
 */
function isLegacyDataset(dataset) {
  return dataset?.metadata?.origin === 'legacy';
}

/**
 * Check if a genome browser session is a legacy CMG session.
 * @param {Object} session
 * @returns {boolean}
 */
function isLegacySession(session) {
  return session?.metadata?.origin === 'legacy';
}

/**
 * Check if a conversion is a legacy CMG conversion.
 * @param {Object} conversion
 * @returns {boolean}
 */
function isLegacyConversion(conversion) {
  return conversion?.metadata?.origin === 'legacy';
}

/**
 * Check if a dataset has reached a specific migration workflow state.
 * @param {number} datasetId
 * @param {string} state
 * @returns {Promise<boolean>}
 */
async function hasReachedState(datasetId, state) {
  const stateRecord = await prisma.dataset_state.findFirst({
    where: { dataset_id: datasetId, state },
  });
  return !!stateRecord;
}

/**
 * Get migration status information for a CMG-migrated dataset.
 * Returns a flat status object covering all workflow stages.
 *
 * @param {number} datasetId
 * @returns {Promise<Object>}
 */
async function getDatasetMigrationStatus(datasetId) {
  const dataset = await prisma.dataset.findUnique({
    where: { id: datasetId },
    select: { id: true, metadata: true },
  });

  if (!dataset) {
    throw new Error(`Dataset with ID ${datasetId} not found`);
  }

  if (!isLegacyDataset(dataset)) {
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
    is_hydrated: isMetadataPopulated,
    is_validated: isValidated,
    is_migrated: isMigrated,
  };
}

/**
 * Check if a CMG dataset is currently undergoing migration.
 * @param {number} datasetId
 * @returns {Promise<boolean>}
 */
async function isMigrationInProgress(datasetId) {
  const status = await getDatasetMigrationStatus(datasetId);
  return status.is_migration_initiated && !status.is_migrated;
}

/**
 * Get migration status information for a CMG-migrated genome browser session.
 * @param {number} sessionId
 * @returns {Promise<Object>}
 */
async function getSessionMigrationStatus(sessionId) {
  const session = await prisma.genome_browser_session.findUnique({
    where: { id: sessionId },
    select: { id: true, metadata: true },
  });

  if (!session) {
    throw new Error(`Session with ID ${sessionId} not found`);
  }

  return {
    is_legacy: isLegacySession(session),
    is_hydrated: session.metadata?.is_hydrated === true,
  };
}

module.exports = {
  isLegacyDataset,
  isLegacySession,
  isLegacyConversion,
  getDatasetMigrationStatus,
  getSessionMigrationStatus,
  isMigrationInProgress,
  hasReachedState,
};
