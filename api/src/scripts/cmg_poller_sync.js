#!/usr/bin/env node

/**
 * CMG to Bioloop Incremental Poller Sync Script
 * 
 * Continuous synchronization of CMG changes into Bioloop.
 * Runs after big-bang migration to keep data in sync.
 * 
 * Usage:
 *   node src/scripts/cmg_poller_sync.js
 * 
 * Environment Variables (via config system):
 *   CMG_MONGO_HOST, CMG_MONGO_PORT, CMG_MONGO_DB, etc.
 *   RHYTHM_MONGO_HOST, RHYTHM_MONGO_PORT, RHYTHM_MONGO_DB, etc.
 * 
 * Pollers:
 * - user_roles: Syncs user role changes
 * - project_acl: Syncs project access control (users, datasets)
 * - dataset_activity: Syncs dataset paths and lifecycle flags
 * - dataset_metadata: Syncs dataset metadata (size, description, etc.)
 * - workflow_status: Syncs dataset states from Rhythm workflows
 */

const config = require('config');
const { MongoClient } = require('mongodb');
const prisma = require('@/db');
const logger = require('@/services/logger');

// Poller classes
const UserRolesPoller = require('./cmg_sync/pollers/user_roles_poller');
const ProjectACLPoller = require('./cmg_sync/pollers/project_acl_poller');
const DatasetActivityPoller = require('./cmg_sync/pollers/dataset_activity_poller');
const DatasetMetadataPoller = require('./cmg_sync/pollers/dataset_metadata_poller');
const workflowStatusPoller = require('./cmg_sync/pollers/workflow_status_poller');

/**
 * Build MongoDB connection URI from config
 */
function buildMongoUri(dbType) {
  const configKey = dbType === 'cmg' ? 'cmg_mongodb' : 'rhythm_mongodb';
  const dbConfig = config.get(configKey);
  
  const {
    host, port, database, username, password, authSource,
  } = dbConfig;
  
  if (!host || !database) {
    throw new Error(`${dbType.toUpperCase()} MongoDB configuration missing`);
  }
  
  let uri = 'mongodb://';
  
  if (username && password) {
    uri += `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`;
  }
  
  uri += `${host}:${port}/${database}`;
  
  if (authSource) {
    uri += `?authSource=${authSource}`;
  }
  
  return uri;
}

/**
 * Main poller function
 */
async function main() {
  logger.info('='.repeat(80));
  logger.info('CMG to Bioloop Incremental Poller Sync');
  logger.info('='.repeat(80));
  
  let cmgClient;
  const pollers = [];
  
  try {
    // Build connection URIs
    const cmgUri = buildMongoUri('cmg');
    
    logger.info('Connecting to databases...');
    logger.info(`CMG MongoDB: ${cmgUri.replace(/\/\/.*@/, '//<credentials>@')}`);
    
    // Connect to CMG MongoDB
    cmgClient = new MongoClient(cmgUri, {
      maxPoolSize: 10,
      minPoolSize: 2,
      serverSelectionTimeoutMS: 5000,
    });
    await cmgClient.connect();
    const cmgDb = cmgClient.db();
    logger.info('[OK] Connected to CMG MongoDB');
    
    logger.info('[OK] Prisma client ready');
    logger.info('');
    
    // Initialize pollers
    logger.info('Initializing pollers...');
    
    // 1. User Roles Poller
    const userRolesPoller = new UserRolesPoller(prisma, cmgDb);
    pollers.push(userRolesPoller);
    logger.info('  - user_roles (10s interval)');
    
    // 2. Project ACL Poller
    const projectACLPoller = new ProjectACLPoller(prisma, cmgDb);
    pollers.push(projectACLPoller);
    logger.info('  - project_acl (10s interval)');
    
    // 3. Dataset Activity Poller
    const datasetActivityPoller = new DatasetActivityPoller(prisma, cmgDb);
    pollers.push(datasetActivityPoller);
    logger.info('  - dataset_activity (10s interval)');
    
    // 4. Dataset Metadata Poller
    const datasetMetadataPoller = new DatasetMetadataPoller(prisma, cmgDb);
    pollers.push(datasetMetadataPoller);
    logger.info('  - dataset_metadata (15s interval)');
    
    logger.info('');
    logger.info('Starting pollers...');
    
    // Start all pollers
    for (const poller of pollers) {
      poller.start();
    }
    
    // Start workflow status poller (separate, connects to Rhythm MongoDB)
    logger.info('  - workflow_status (10s interval, Rhythm MongoDB)');
    workflowStatusPoller.startPolling().catch(error => {
      logger.error('[workflow_status] Poller crashed:', error);
    });
    
    logger.info('');
    logger.info('='.repeat(80));
    logger.info('[SUCCESS] All pollers started successfully');
    logger.info('='.repeat(80));
    logger.info('');
    logger.info('Press Ctrl+C to stop');
    logger.info('');
    
    // Setup graceful shutdown
    setupGracefulShutdown(pollers, cmgClient);
    
    // Log metrics periodically
    startMetricsReporter(pollers);
    
    // Keep process alive
    await new Promise(() => {}); // Never resolves
    
  } catch (error) {
    logger.error('');
    logger.error('='.repeat(80));
    logger.error('[FAILED] Poller initialization failed');
    logger.error('='.repeat(80));
    logger.error('Error:', error);
    logger.error('Stack:', error.stack);
    logger.error('');
    
    // Cleanup
    for (const poller of pollers) {
      poller.stop();
    }
    
    if (cmgClient) {
      await cmgClient.close();
    }
    
    await prisma.$disconnect();
    
    process.exit(1);
  }
}

/**
 * Setup graceful shutdown handlers
 */
function setupGracefulShutdown(pollers, cmgClient) {
  const shutdown = async (signal) => {
    logger.info('');
    logger.info('='.repeat(80));
    logger.info(`Received ${signal}, shutting down gracefully...`);
    logger.info('='.repeat(80));
    
    // Stop all pollers
    logger.info('Stopping pollers...');
    for (const poller of pollers) {
      poller.stop();
      logger.info(`  - ${poller.pollerName} stopped`);
    }
    
    // Close connections
    if (cmgClient) {
      await cmgClient.close();
      logger.info('Closed CMG MongoDB connection');
    }
    
    await prisma.$disconnect();
    logger.info('Closed Prisma connection');
    
    logger.info('');
    logger.info('[OK] Shutdown complete');
    process.exit(0);
  };
  
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  
  // Handle uncaught errors
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception:', error);
    shutdown('UNCAUGHT_EXCEPTION');
  });
  
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled rejection at:', promise, 'reason:', reason);
    shutdown('UNHANDLED_REJECTION');
  });
}

/**
 * Start metrics reporter
 * Logs metrics every 60 seconds
 */
function startMetricsReporter(pollers) {
  setInterval(() => {
    logger.info('');
    logger.info('--- Poller Metrics ---');
    
    for (const poller of pollers) {
      const metrics = poller.getMetrics();
      logger.info(`[${metrics.pollerName}]`);
      logger.info(`  Running: ${metrics.isRunning}`);
      logger.info(`  Total runs: ${metrics.totalRuns}`);
      logger.info(`  Successful: ${metrics.successfulRuns}`);
      logger.info(`  Failed: ${metrics.failedRuns}`);
      logger.info(`  Total processed: ${metrics.totalProcessed}`);
      logger.info(`  Last run: ${metrics.lastRunTime || 'Never'}`);
      logger.info(`  Last success: ${metrics.lastSuccessTime || 'Never'}`);
      if (metrics.lastError) {
        logger.info(`  Last error: ${metrics.lastError}`);
      }
    }
    
    logger.info('');
  }, 60000); // Every 60 seconds
}

// Run the main function
if (require.main === module) {
  main().catch((error) => {
    logger.error('Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = { main };

