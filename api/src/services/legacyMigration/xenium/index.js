/**
 * Xenium Legacy Migration Service
 *
 * Handles migration status queries for datasets migrated from the Xenium
 * PostgreSQL database into cmg-bioloop.
 *
 * A Xenium legacy object is identified by metadata.origin === 'legacy_xenium'.
 *
 * Xenium does not have Sessions or Conversions features, so only dataset
 * migration status is supported here.
 */

const prisma = require('@/db');

/**
 * Check if a dataset is a legacy Xenium dataset (origin: 'legacy_xenium').
 * @param {Object} dataset
 * @returns {boolean}
 */
function isLegacyXeniumDataset(dataset) {
  return dataset?.metadata?.origin === 'legacy_xenium';
}

/**
 * Check if a dataset has reached a specific migration workflow state.
 * Shared logic with CMG migration (same dataset_state table).
 *
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
 * Get migration status for a Xenium-migrated dataset.
 * Xenium datasets use the same workflow stages as CMG datasets.
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

  if (!isLegacyXeniumDataset(dataset)) {
    return {
      is_legacy_xenium: false,
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
    is_legacy_xenium: true,
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
 * Check if a Xenium dataset is currently undergoing migration.
 * @param {number} datasetId
 * @returns {Promise<boolean>}
 */
async function isMigrationInProgress(datasetId) {
  const status = await getDatasetMigrationStatus(datasetId);
  return status.is_migration_initiated && !status.is_migrated;
}

module.exports = {
  isLegacyXeniumDataset,
  getDatasetMigrationStatus,
  isMigrationInProgress,
  hasReachedState,
};
