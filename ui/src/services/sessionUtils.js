/**
 * Formats analysis type text to uppercase with underscores
 * Example: "Raw Reads" -> "RAW_READS"
 * @param {string} text - The text to format
 * @returns {string} - The formatted text
 */
export function formatAnalysisType(text) {
  if (!text || typeof text !== 'string') {
    return '';
  }
  
  return text
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '_')
    .replace(/[^A-Z0-9_]/g, '');
}

/**
 * Converts formatted analysis type back to human-readable format
 * Example: "RAW_READS" -> "Raw Reads"
 * @param {string} formattedType - The formatted analysis type
 * @returns {string} - The human-readable format
 */
export function humanizeAnalysisType(formattedType) {
  if (!formattedType || typeof formattedType !== 'string') {
    return '';
  }
  
  return formattedType
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
}
