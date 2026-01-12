/**
 * CMG Sync - State Mapper
 * 
 * Maps CMG events and flags to Bioloop dataset states
 */

const { parseEventsForStates } = require('./event_parser');
const logger = require('../logger');

/**
 * Determine which states should be added to a dataset based on CMG data
 * 
 * @param {Object} cmgDataset - CMG dataset or dataproduct document
 * @returns {Array<Object>} Array of {state, timestamp} objects
 */
function determineStatesToAdd(cmgDataset) {
  const states = [];
  
  // Parse events if they exist
  if (cmgDataset.events && Array.isArray(cmgDataset.events)) {
    const eventStates = parseEventsForStates(cmgDataset.events);
    states.push(...eventStates);
  }
  
  // Additional state logic based on flags
  // Note: We rely mainly on events, but can add fallback logic here if needed
  
  return states;
}

/**
 * Add states to a dataset (application-level uniqueness check)
 * 
 * @param {Object} tx - Prisma transaction
 * @param {number} datasetId - Bioloop dataset ID
 * @param {Array<Object>} statesToAdd - Array of {state, timestamp} objects
 */
async function addStatesToDataset(tx, datasetId, statesToAdd) {
  for (const stateData of statesToAdd) {
    try {
      // Check if state already exists for this dataset
      const existing = await tx.dataset_state.findFirst({
        where: {
          dataset_id: datasetId,
          state: stateData.state
        }
      });
      
      if (!existing) {
        await tx.dataset_state.create({
          data: {
            dataset_id: datasetId,
            state: stateData.state,
            timestamp: stateData.timestamp || new Date()
          }
        });
        
        logger.debug(`[State Mapper] Added state '${stateData.state}' to dataset ${datasetId}`);
      } else {
        // State already exists - optionally update timestamp if needed
        // For now, skip (states don't change once set)
        logger.debug(`[State Mapper] State '${stateData.state}' already exists for dataset ${datasetId}`);
      }
    } catch (error) {
      logger.error(`[State Mapper] Failed to add state '${stateData.state}' to dataset ${datasetId}:`, error);
      throw error;
    }
  }
}

/**
 * Get current states for a dataset
 * 
 * @param {Object} prisma - Prisma client or transaction
 * @param {number} datasetId - Bioloop dataset ID
 * @returns {Promise<Array<string>>} Array of state names
 */
async function getCurrentStates(prisma, datasetId) {
  const states = await prisma.dataset_state.findMany({
    where: { dataset_id: datasetId },
    select: { state: true }
  });
  
  return states.map(s => s.state);
}

/**
 * Check if dataset has a specific state
 * 
 * @param {Object} prisma - Prisma client or transaction
 * @param {number} datasetId - Bioloop dataset ID
 * @param {string} stateName - State name to check
 * @returns {Promise<boolean>} True if dataset has this state
 */
async function hasState(prisma, datasetId, stateName) {
  const state = await prisma.dataset_state.findFirst({
    where: {
      dataset_id: datasetId,
      state: stateName
    }
  });
  
  return state !== null;
}

module.exports = {
  determineStatesToAdd,
  addStatesToDataset,
  getCurrentStates,
  hasState
};

