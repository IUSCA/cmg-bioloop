/**
 * Formats analysis type text to uppercase with underscores
 * Example: "Raw Reads" -> "RAW_READS"
 * @param {string} text - The text to format
 * @returns {string} - The formatted text
 */
function formatAnalysisType(text) {
  if (!text || typeof text !== 'string') {
    return null;
  }

  return text
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '_')
    .replace(/[^A-Z0-9_]/g, '');
}

module.exports = {
  formatAnalysisType,
};
