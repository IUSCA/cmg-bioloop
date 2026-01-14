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
      // Read from api/.env file
      const apiEnvPath = path.resolve(__dirname, '../../../api/.env');
      const apiEnvVars = parseEnvFile(apiEnvPath);

      if (apiEnvVars.DATABASE_URL) {
        return apiEnvVars.DATABASE_URL;
      }

      // Fallback: construct from individual variables
      const dbUrl = `postgresql://${apiEnvVars.DATABASE_USER || 'appuser'}:${apiEnvVars.DATABASE_PASSWORD || 'example'}@${apiEnvVars.DATABASE_HOST || 'postgres'}:${apiEnvVars.DATABASE_PORT || '5432'}/${apiEnvVars.DATABASE_NAME || 'app'}?schema=public`;
      return dbUrl;
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

