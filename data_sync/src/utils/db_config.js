/**
 * Database Configuration Utility
 * 
 * Dynamically determines which PostgreSQL database to connect to based on:
 * - Command line flag (--target-db)
 * - Environment variables
 * - Configuration files
 */

const fs = require('fs');
const path = require('path');

/**
 * Parse .env file and return key-value pairs
 * @param {string} envFilePath - Path to .env file
 * @returns {Object} Parsed environment variables
 */
function parseEnvFile(envFilePath) {
  if (!fs.existsSync(envFilePath)) {
    return {};
  }

  const envContent = fs.readFileSync(envFilePath, 'utf8');
  const envVars = {};

  envContent.split('\n').forEach((line) => {
    // Skip empty lines and comments
    if (!line.trim() || line.trim().startsWith('#')) {
      return;
    }

    // Parse KEY=VALUE format
    const match = line.match(/^([^=]+)=(.*)$/);
    if (match) {
      const key = match[1].trim();
      let value = match[2].trim();

      // Remove quotes if present
      if ((value.startsWith('"') && value.endsWith('"'))
        || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }

      envVars[key] = value;
    }
  });

  return envVars;
}

/**
 * Get DATABASE_URL based on target database
 * @param {string} targetDb - Target database: 'sandbox', 'app', or 'custom'
 * @returns {string} DATABASE_URL connection string
 */
function getDatabaseUrl(targetDb = 'sandbox') {
  // If DATABASE_URL is already set in environment, use it (highest priority)
  if (process.env.DATABASE_URL && targetDb === 'custom') {
    return process.env.DATABASE_URL;
  }

  switch (targetDb) {
    case 'sandbox':
      // Use sandbox database (data_sync's own PostgreSQL)
      return process.env.DATABASE_URL
        || 'postgresql://appuser:example@localhost:5432/bioloop_sync?schema=public';

    case 'app': {
      // Read from api/.env file (mounted at /opt/sca/api/.env in container)
      const apiEnvPath = path.resolve(__dirname, '../../../api/.env');
      const apiEnvVars = parseEnvFile(apiEnvPath);

      // Check if we got any variables
      if (Object.keys(apiEnvVars).length === 0) {
        throw new Error(`Could not read ${apiEnvPath}. File may not exist or be empty.`);
      }

      // Prefer constructing from discrete fields so credentials are properly
      // URL-encoded (raw DATABASE_URL may break when password has URI chars).
      const host = apiEnvVars.POSTGRES_HOST || apiEnvVars.DATABASE_HOST || 'postgres';
      const port = apiEnvVars.POSTGRES_PORT || apiEnvVars.DATABASE_PORT || '5432';
      const user = apiEnvVars.POSTGRES_USER || apiEnvVars.DATABASE_USER || 'appuser';
      const password = apiEnvVars.POSTGRES_PASSWORD || apiEnvVars.DATABASE_PASSWORD || 'example';
      const database = apiEnvVars.POSTGRES_DB || apiEnvVars.DATABASE_NAME || 'app';
      const schema = apiEnvVars.POSTGRES_SCHEMA || 'public';
      if (host && port && user && database) {
        const dbUrl = `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${database}?schema=${schema}`;
        return dbUrl;
      }

      // Final fallback: resolve DATABASE_URL with variable expansion.
      if (apiEnvVars.DATABASE_URL) {
        let dbUrl = apiEnvVars.DATABASE_URL;
        dbUrl = dbUrl.replace(/\$\{([^}]+)\}/g, (match, varName) => (
          apiEnvVars[varName] || process.env[varName] || match
        ));
        dbUrl = dbUrl.replace(/\$([A-Z_]+)/g, (match, varName) => (
          apiEnvVars[varName] || process.env[varName] || match
        ));
        return dbUrl;
      }

      throw new Error(
        `Could not resolve app database connection from ${apiEnvPath}. `
        + 'Set POSTGRES_HOST/PORT/USER/PASSWORD/DB (preferred) or DATABASE_URL.',
      );
    }

    case 'custom':
      // Use DATABASE_URL from current environment or data_sync/.env
      return process.env.DATABASE_URL
        || 'postgresql://appuser:example@localhost:5432/bioloop_sync?schema=public';

    default:
      throw new Error(`Invalid target database: ${targetDb}. Must be 'sandbox', 'app', or 'custom'`);
  }
}

/**
 * Set DATABASE_URL environment variable based on target
 * @param {string} targetDb - Target database: 'sandbox', 'app', or 'custom'
 */
function setDatabaseUrl(targetDb) {
  const databaseUrl = getDatabaseUrl(targetDb);
  process.env.DATABASE_URL = databaseUrl;
  return databaseUrl;
}

module.exports = {
  parseEnvFile,
  getDatabaseUrl,
  setDatabaseUrl,
};

