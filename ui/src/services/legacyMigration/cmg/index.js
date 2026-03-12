/**
 * CMG Legacy Migration Service (UI)
 *
 * Handles migration status queries for datasets, sessions, users, and projects
 * that were migrated from the legacy CMG MongoDB database into Bioloop.
 *
 * CMG legacy objects have metadata.origin === 'legacy'.
 */

import api from "../../api";

/**
 * Get migration status for a CMG-migrated dataset.
 * @param {number} datasetId
 * @returns {Promise<Object>} { is_legacy, is_migration_initiated, is_retrieved,
 *                              is_inspected, is_metadata_populated, is_hydrated,
 *                              is_validated, is_migrated }
 */
export async function getDatasetMigrationStatus(datasetId) {
  const response = await api.get(`/legacy/migrations/cmg/datasets/${datasetId}`);
  return response.data;
}

/**
 * Get migration status for a CMG-migrated genome browser session.
 * @param {number} sessionId
 * @returns {Promise<Object>} { is_legacy, is_hydrated }
 */
export async function getSessionMigrationStatus(sessionId) {
  const response = await api.get(`/legacy/migrations/cmg/sessions/${sessionId}`);
  return response.data;
}

/**
 * Check if a dataset is a legacy CMG dataset (origin: 'legacy').
 * @param {Object} dataset
 * @returns {boolean}
 */
export function isLegacyDataset(dataset) {
  return dataset?.metadata?.origin === "legacy";
}

/**
 * Check if a genome browser session is a legacy CMG session.
 * @param {Object} session
 * @returns {boolean}
 */
export function isLegacySession(session) {
  return session?.metadata?.origin === "legacy";
}

/**
 * Check if a project is a legacy CMG project.
 * @param {Object} project
 * @returns {boolean}
 */
export function isLegacyProject(project) {
  return project?.metadata?.origin === "legacy";
}

/**
 * Check if a user is a legacy CMG user.
 * @param {Object} user
 * @returns {boolean}
 */
export function isLegacyUser(user) {
  return user?.metadata?.origin === "legacy";
}

/**
 * Check if a CMG dataset needs hydration (is legacy but not yet hydrated).
 * @param {number} datasetId
 * @returns {Promise<boolean>}
 */
export async function needsHydration(datasetId) {
  try {
    const status = await getDatasetMigrationStatus(datasetId);
    return status.is_legacy && !status.is_hydrated;
  } catch (error) {
    console.error("Error checking CMG hydration status:", error);
    return false;
  }
}

/**
 * Check if a CMG dataset is currently undergoing migration.
 * @param {number} datasetId
 * @returns {Promise<boolean>}
 */
export async function isMigrationInProgress(datasetId) {
  try {
    const status = await getDatasetMigrationStatus(datasetId);
    return status.is_migration_initiated && !status.is_migrated;
  } catch (error) {
    console.error("Error checking CMG migration progress:", error);
    return false;
  }
}

/**
 * Check if a CMG genome browser session has been hydrated.
 * @param {number} sessionId
 * @returns {Promise<boolean>}
 */
export async function isSessionHydrated(sessionId) {
  try {
    const status = await getSessionMigrationStatus(sessionId);
    return status.is_hydrated;
  } catch (error) {
    console.error("Error checking CMG session hydration status:", error);
    return false;
  }
}

export default {
  getDatasetMigrationStatus,
  getSessionMigrationStatus,
  isLegacyDataset,
  isLegacySession,
  isLegacyProject,
  isLegacyUser,
  needsHydration,
  isSessionHydrated,
  isMigrationInProgress,
};
