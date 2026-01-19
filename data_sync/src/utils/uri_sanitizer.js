/**
 * URI Sanitizer - Remove credentials from database connection strings
 * 
 * Used to safely log database URIs without exposing passwords.
 */

/**
 * Sanitize database URIs by removing credentials
 * @param {string|any} str - String to sanitize (usually a URI or error message)
 * @returns {string} Sanitized string with credentials replaced by <credentials>
 */
function sanitizeUri(str) {
  if (!str) return str;
  if (typeof str !== 'string') {
    str = JSON.stringify(str);
  }
  // Sanitize MongoDB URIs (mongodb://username:password@host -> mongodb://<credentials>@host)
  str = str.replace(/mongodb:\/\/[^:]+:[^@]+@/g, 'mongodb://<credentials>@');
  // Sanitize PostgreSQL URIs (postgresql://username:password@host -> postgresql://<credentials>@host)
  str = str.replace(/postgresql:\/\/[^:]+:[^@]+@/g, 'postgresql://<credentials>@');
  return str;
}

module.exports = {
  sanitizeUri,
};

