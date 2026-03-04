import api from "./api";

class SessionService {
  /**
   * Get all sessions accessible to the current user
   * @param {Object} params - Query parameters
   * @param {string} params.title - Filter by title
   * @param {string} params.genome - Filter by genome
   * @param {string} params.genome_type - Filter by genome type
   * @param {number} params.limit - Number of results to return
   * @param {number} params.offset - Number of results to skip
   * @param {string} params.sort_by - Field to sort by
   * @param {string} params.sort_order - Sort order (asc/desc)
   * @returns {Promise<Object>} Sessions and metadata
   */
  getAll(params = {}) {
    return api.get("/sessions", { params });
  }

  /**
   * Get sessions for a specific user
   * @param {string} username - Username to get sessions for
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} Sessions and metadata
   */
  getByUsername(username, params = {}) {
    return api.get(`/sessions/${username}/all`, { params });
  }

  /**
   * Get a specific session by ID
   * @param {number} id - Session ID
   * @returns {Promise<Object>} Session data
   */
  getById(id) {
    return api.get(`/sessions/${id}`);
  }

  /**
   * Create a new session
   * @param {Object} sessionData - Session data
   * @param {string} sessionData.title - Session title
   * @param {string} sessionData.genome - Genome
   * @param {string} sessionData.genome_type - Genome type
   * @param {Array<number>} sessionData.track_ids - Array of track IDs
   * @returns {Promise<Object>} Created session
   */
  create(sessionData) {
    return api.post("/sessions", sessionData);
  }

  /**
   * Update a session
   * @param {number} id - Session ID
   * @param {Object} sessionData - Session data to update
   * @returns {Promise<Object>} Updated session
   */
  update(id, sessionData) {
    return api.patch(`/sessions/${id}`, sessionData);
  }

  /**
   * Delete a session
   * @param {number} id - Session ID
   * @returns {Promise<void>}
   */
  deleteSession(id) {
    return api.delete(`/sessions/${id}`);
  }

  /**
   * Check if a session name already exists for the current user
   * @param {string} name - Session name to check
   * @returns {Promise<Object>} { exists: boolean }
   */
  checkName(name) {
    return api.get(`/sessions/check-name/${encodeURIComponent(name.trim())}`);
  }

  /**
   * Get datahub configuration for a session
   * @param {number} id - Session ID
   * @param {string} browser - Browser type ('igv' or 'washu'), defaults to 'igv'
   * @returns {Promise<Object>} Datahub configuration
   */
  getDatahub(id, browser = "igv") {
    return api.get(`/sessions/${id}/datahub`, { params: { browser } });
  }

  /**
   * Set authentication cookie for file access in genome browsers (IGV, WashU)
   * @param {number} id - Session ID
   * @returns {Promise<Object>} Response
   */
  setFileCookie(id) {
    return api.post(`/sessions/${id}/set-file-cookie`);
  }

  /**
   * Stage a session
   * @param {number} id - Session ID
   * @param {Object} stagingData - Staging data
   * @returns {Promise<Object>} Staging result
   */
  stage(id, stagingData = {}) {
    return api.post(`/sessions/${id}/stage`, stagingData);
  }

  /**
   * Get projects associated with a session
   * @param {number} id - Session ID
   * @returns {Promise<Object>} Projects data
   */
  getProjects(id) {
    return api.get(`/sessions/${id}/projects`);
  }

  /**
   * Get datasets for a session
   * @param {number} id - Session ID
   * @param {Object} params - Query parameters
   * @param {boolean} params.staged - Filter by staged status (optional)
   * @returns {Promise<Object>} Datasets data
   */
  getDatasets(id, params = {}) {
    return api.get(`/sessions/${id}/datasets`, { params });
  }

  /**
   * Stage all unstaged datasets for a session
   * @param {number} id - Session ID
   * @returns {Promise<Object>} Staging results
   */
  stageDatasets(id) {
    return api.post(`/sessions/${id}/stage-datasets`);
  }

  /**
   * Trigger hydration workflow for a session
   * @param {number} id - Session ID
   * @returns {Promise<Object>} Workflow response
   */
  hydrateSession(id) {
    return api.post(`/sessions/${id}/workflows/hydrate_session`);
  }

  /**
   * Check if a session is legacy (migrated from CMG via bigbang)
   * @param {Object} session - The session object
   * @returns {boolean} - True if session was created via the bigbang migration
   */
  _isLegacySession(session) {
    return session?.metadata?.origin === "legacy";
  }

  /**
   * Check if hydration workflow has completed successfully
   * Uses DONE_STATES = [SUCCESS] logic from backend
   * NOTE: session_workflows must be enriched with status and name from Rhythm API
   * @param {Array} sessionWorkflows - Array of session_workflow objects enriched with status and name
   * @returns {boolean} - True if hydration workflow finished successfully
   */
  _isHydrationComplete(sessionWorkflows) {
    if (!sessionWorkflows || sessionWorkflows.length === 0) {
      return false;
    }

    // Find the hydrate_session workflow by NAME (workflow_id is a UUID, not the workflow name!)
    // The backend enriches session_workflows with workflow details from Rhythm
    const hydrationWorkflow = sessionWorkflows.find(
      (sw) => sw.name === "hydrate_session",
    );

    if (!hydrationWorkflow) {
      return false;
    }

    // Check if workflow status is SUCCESS
    return hydrationWorkflow.status === "SUCCESS";
  }

  /**
   * Check if a legacy session needs hydration
   * @param {Object} session - The session object with session_workflows
   * @returns {boolean} - True if session is legacy and not hydrated
   */
  _needsHydration(session) {
    if (!this._isLegacySession(session)) {
      return false;
    }
    return !this._isHydrationComplete(session.session_workflows);
  }
}

export default new SessionService();
