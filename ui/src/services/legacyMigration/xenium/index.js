/**
 * Xenium Legacy Migration Service (UI)
 *
 * Handles migration status queries for datasets migrated from the Xenium
 * PostgreSQL database into cmg-bioloop.
 *
 * Xenium does not have Sessions, Conversions, or Tracks features.
 * Xenium legacy objects have metadata.origin === 'legacy_xenium'.
 */

import api from "../../api";

/**
 * Get migration status for a Xenium-migrated dataset.
 * @param {number} datasetId
 * @returns {Promise<Object>} { is_legacy_xenium, is_migration_initiated, is_retrieved,
 *                              is_inspected, is_metadata_populated, is_hydrated,
 *                              is_validated, is_migrated }
 */
export async function getDatasetMigrationStatus(datasetId) {
  const response = await api.get(`/legacy/migrations/xenium/datasets/${datasetId}`);
  return response.data;
}

/**
 * Check if a dataset is a legacy Xenium dataset (origin: 'legacy_xenium').
 * @param {Object} dataset
 * @returns {boolean}
 */
export function isLegacyXeniumDataset(dataset) {
  return dataset?.metadata?.origin === "legacy_xenium";
}

/**
 * Check if a project is a legacy Xenium project.
 * @param {Object} project
 * @returns {boolean}
 */
export function isLegacyXeniumProject(project) {
  return project?.metadata?.origin === "legacy_xenium";
}

/**
 * Check if a user is a legacy Xenium user.
 * @param {Object} user
 * @returns {boolean}
 */
export function isLegacyXeniumUser(user) {
  return user?.metadata?.origin === "legacy_xenium";
}

/**
 * Check if a Xenium dataset needs hydration (is legacy_xenium but not yet hydrated).
 * @param {number} datasetId
 * @returns {Promise<boolean>}
 */
export async function needsHydration(datasetId) {
  try {
    const status = await getDatasetMigrationStatus(datasetId);
    return status.is_legacy_xenium && !status.is_hydrated;
  } catch (error) {
    console.error("Error checking Xenium hydration status:", error);
    return false;
  }
}

/**
 * Check if a Xenium dataset is currently undergoing migration.
 * @param {number} datasetId
 * @returns {Promise<boolean>}
 */
export async function isMigrationInProgress(datasetId) {
  try {
    const status = await getDatasetMigrationStatus(datasetId);
    return status.is_migration_initiated && !status.is_migrated;
  } catch (error) {
    console.error("Error checking Xenium migration progress:", error);
    return false;
  }
}

export default {
  getDatasetMigrationStatus,
  isLegacyXeniumDataset,
  isLegacyXeniumProject,
  isLegacyXeniumUser,
  needsHydration,
  isMigrationInProgress,
};
