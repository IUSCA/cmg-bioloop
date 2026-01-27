/**
 * Legacy Migration Service (UI)
 * 
 * Handles legacy dataset and session migration status queries.
 * Specifically for CMG datasets migrated from MongoDB that need hydration.
 */

import api from './api';

/**
 * Get migration status information for a dataset
 * @param {number} datasetId - The dataset ID
 * @returns {Promise<Object>} - Migration status object
 */
export async function getDatasetMigrationStatus(datasetId) {
  try {
    const response = await api.get(`/legacy/migrations/datasets/${datasetId}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching dataset migration status:', error);
    throw error;
  }
}

/**
 * Check if a dataset is a legacy CMG dataset
 * @param {Object} dataset - The dataset object
 * @returns {boolean} - True if the dataset has a cmg_id
 */
export function isLegacyDataset(dataset) {
  return !!(dataset && dataset.cmg_id);
}

/**
 * Check if a dataset needs hydration
 * @param {number} datasetId - The dataset ID
 * @returns {Promise<boolean>} - True if the dataset needs hydration
 */
export async function needsHydration(datasetId) {
  try {
    const status = await getDatasetMigrationStatus(datasetId);
    return status.is_legacy && !status.is_hydrated;
  } catch (error) {
    console.error('Error checking hydration status:', error);
    return false;
  }
}

/**
 * Check if a dataset is currently undergoing migration
 * @param {number} datasetId - The dataset ID
 * @returns {Promise<boolean>} - True if migration is in progress
 */
export async function isMigrationInProgress(datasetId) {
  try {
    const status = await getDatasetMigrationStatus(datasetId);
    return status.is_migration_initiated && !status.is_migrated;
  } catch (error) {
    console.error('Error checking migration progress:', error);
    return false;
  }
}

/**
 * Get migration status information for a session
 * @param {number} sessionId - The session ID
 * @returns {Promise<Object>} - Migration status object
 */
export async function getSessionMigrationStatus(sessionId) {
  try {
    const response = await api.get(`/legacy/migrations/sessions/${sessionId}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching session migration status:', error);
    throw error;
  }
}

/**
 * Check if a session is a legacy CMG session
 * @param {Object} session - The session object
 * @returns {boolean} - True if the session has a cmg_id
 */
export function isLegacySession(session) {
  return !!(session && session.cmg_id);
}

/**
 * Check if a session is hydrated
 * @param {number} sessionId - The session ID
 * @returns {Promise<boolean>} - True if the session is hydrated
 */
export async function isSessionHydrated(sessionId) {
  try {
    const status = await getSessionMigrationStatus(sessionId);
    return status.is_hydrated;
  } catch (error) {
    console.error('Error checking session hydration status:', error);
    return false;
  }
}

export default {
  getDatasetMigrationStatus,
  getSessionMigrationStatus,
  isLegacyDataset,
  isLegacySession,
  needsHydration,
  isSessionHydrated,
  isMigrationInProgress,
};

