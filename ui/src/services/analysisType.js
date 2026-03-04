import api from "./api";

export default {
  /**
   * Get all analysis types
   * @returns {Promise} Axios response with list of analysis types
   */
  getAll() {
    return api.get("/analysis-types");
  },

  /**
   * Create a new analysis type
   * @param {Object} data - Analysis type data
   * @param {string} data.name - Analysis type name
   * @param {string} data.extension - File extension
   * @returns {Promise} Axios response with created analysis type
   */
  create(data) {
    return api.post("/analysis-types", data);
  },
};
