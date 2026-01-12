/**
 * CMG Sync - Event Parser
 * 
 * Parses CMG events arrays to extract timestamps and determine states
 */

const logger = require('../logger');

/**
 * Check if CMG document has a specific event
 * 
 * @param {Array<Object>} events - CMG events array
 * @param {string} eventDescription - Event description to search for
 * @returns {boolean} True if event exists
 */
function hasEvent(events, eventDescription) {
  if (!Array.isArray(events)) {
    return false;
  }
  
  return events.some(event => 
    event.description === eventDescription
  );
}

/**
 * Get timestamp for a specific event
 * 
 * @param {Array<Object>} events - CMG events array
 * @param {string} eventDescription - Event description to search for
 * @returns {Date|null} Event timestamp or null if not found
 */
function getEventTimestamp(events, eventDescription) {
  if (!Array.isArray(events)) {
    return null;
  }
  
  const event = events.find(e => e.description === eventDescription);
  
  if (event && event.stamp) {
    return new Date(event.stamp);
  }
  
  return null;
}

/**
 * Get all events of a specific type (e.g., all "finish" events)
 * 
 * @param {Array<Object>} events - CMG events array
 * @param {string} eventType - Partial event description (e.g., "finish")
 * @returns {Array<Object>} Matching events
 */
function getEventsByType(events, eventType) {
  if (!Array.isArray(events)) {
    return [];
  }
  
  return events.filter(event => 
    event.description && event.description.includes(eventType)
  );
}

/**
 * Parse CMG events to determine which Bioloop states should be set
 * 
 * @param {Array<Object>} events - CMG events array
 * @returns {Array<Object>} Array of {state, timestamp} objects
 */
function parseEventsForStates(events) {
  if (!Array.isArray(events)) {
    return [];
  }
  
  const states = [];
  
  // Check for archive completion
  if (hasEvent(events, 'archive - finish')) {
    states.push({
      state: 'ARCHIVED',
      timestamp: getEventTimestamp(events, 'archive - finish')
    });
  }
  
  // Check for stage completion
  if (hasEvent(events, 'stage - finish')) {
    states.push({
      state: 'FETCHED',
      timestamp: getEventTimestamp(events, 'stage - finish')
    });
  }
  
  // Check for validate completion
  if (hasEvent(events, 'validate - finish')) {
    states.push({
      state: 'STAGED',
      timestamp: getEventTimestamp(events, 'validate - finish')
    });
  }
  
  // Check for inspect completion
  if (hasEvent(events, 'inspect - finish')) {
    states.push({
      state: 'INSPECTED',
      timestamp: getEventTimestamp(events, 'inspect - finish')
    });
  }
  
  // Check for QC completion
  if (hasEvent(events, 'qc - finish')) {
    states.push({
      state: 'QC',
      timestamp: getEventTimestamp(events, 'qc - finish')
    });
  }
  
  // Filter out states with null timestamps
  return states.filter(s => s.timestamp !== null);
}

/**
 * Get most recent event timestamp
 * 
 * @param {Array<Object>} events - CMG events array
 * @returns {Date|null} Most recent event timestamp
 */
function getMostRecentEventTimestamp(events) {
  if (!Array.isArray(events) || events.length === 0) {
    return null;
  }
  
  const timestamps = events
    .map(e => e.stamp ? new Date(e.stamp) : null)
    .filter(t => t !== null)
    .sort((a, b) => b - a);  // Descending order
  
  return timestamps.length > 0 ? timestamps[0] : null;
}

/**
 * Check if dataset has completed a workflow step
 * 
 * @param {Array<Object>} events - CMG events array
 * @param {string} stepName - Step name (e.g., "stage", "archive")
 * @returns {boolean} True if step completed
 */
function hasCompletedStep(events, stepName) {
  return hasEvent(events, `${stepName} - finish`);
}

/**
 * Get all completed workflow steps
 * 
 * @param {Array<Object>} events - CMG events array
 * @returns {Array<string>} Array of completed step names
 */
function getCompletedSteps(events) {
  if (!Array.isArray(events)) {
    return [];
  }
  
  const completedSteps = [];
  
  const possibleSteps = [
    'inspect',
    'archive',
    'stage',
    'validate',
    'qc',
    'convert'
  ];
  
  for (const step of possibleSteps) {
    if (hasCompletedStep(events, step)) {
      completedSteps.push(step);
    }
  }
  
  return completedSteps;
}

module.exports = {
  hasEvent,
  getEventTimestamp,
  getEventsByType,
  parseEventsForStates,
  getMostRecentEventTimestamp,
  hasCompletedStep,
  getCompletedSteps
};

