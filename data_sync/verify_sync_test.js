#!/usr/bin/env node

/**
 * CMG to Bioloop Sync Test Verification Script
 * 
 * Monitors /tmp/sync_test_queue.jsonl for new test entries from CMG agent.
 * Verifies each change appears in Bioloop database and reports delays.
 * 
 * Results written to: /tmp/sync_test_results.jsonl
 */

require('module-alias/register');
const fs = require('fs');
const path = require('path');
const { PrismaClient } = require('@prisma/client');
const logger = require('./src/logger');

// Target database configuration
const targetDb = process.argv.find(arg => arg.startsWith('--target-db='))?.split('=')[1] || 'app';
let databaseUrl;

if (targetDb === 'app') {
  // Use environment variables from docker-compose or defaults
  const host = process.env.POSTGRES_HOST || 'postgres';
  const port = process.env.POSTGRES_PORT || '5432';
  const user = process.env.POSTGRES_USER || 'appuser';
  const password = process.env.POSTGRES_PASSWORD || 'example';
  const database = process.env.POSTGRES_DB || 'app';
  databaseUrl = `postgresql://${user}:${password}@${host}:${port}/${database}?schema=public`;
} else {
  // Sandbox or custom - use DATABASE_URL from environment
  databaseUrl = process.env.DATABASE_URL || 'postgresql://appuser:example@localhost:5432/bioloop_sync?schema=public';
}

const prisma = new PrismaClient({
  datasources: {
    db: {
      url: databaseUrl
    }
  }
});

const TEST_QUEUE_FILE = '/tmp/cmg-bioloop/sync_test_queue.jsonl';
const RESULTS_FILE = '/tmp/cmg-bioloop/sync_test_results.jsonl';
const POLL_INTERVAL = 2000; // Check every 2 seconds
const TIMEOUT = 60000; // 60 second timeout

// Mapping of CMG collections to Bioloop tables and fields
const FIELD_MAPPINGS = {
  datasets: {
    table: 'dataset',
    fields: {
      description: 'description',
      size: 'size',
      du_size: 'du_size',
      num_files: 'num_files',
      num_directories: 'num_directories',
      file_type: 'file_type',
      'path.staged': 'path_staged',
      'path.archive': 'path_archive',
      'path.origin': 'path_origin',
      staged: 'is_staged',  // CMG calls it 'staged', Bioloop calls it 'is_staged'
      is_staged: 'is_staged',
    }
  },
  dataproducts: {
    table: 'dataset',
    fields: {
      description: 'description',
      size: 'size',
      du_size: 'du_size',
      num_files: 'num_files',
      num_directories: 'num_directories',
      file_type: 'file_type',
      'path.staged': 'path_staged',
      'path.archive': 'path_archive',
      'path.origin': 'path_origin',
      is_staged: 'is_staged',
    }
  },
  projects: {
    table: 'project',
    fields: {
      description: 'description',
      browser_enabled: 'browser_enabled',
    }
  },
  users: {
    table: 'user',
    fields: {
      active: 'active',
    }
  }
};

let processedTests = new Set();
let lastFilePosition = 0;

/**
 * Initialize test files
 */
function initializeTestFiles() {
  if (!fs.existsSync(TEST_QUEUE_FILE)) {
    fs.writeFileSync(TEST_QUEUE_FILE, '', 'utf8');
    logger.info(`[MONITOR] Created test queue file: ${TEST_QUEUE_FILE}`);
  }
  
  if (!fs.existsSync(RESULTS_FILE)) {
    fs.writeFileSync(RESULTS_FILE, '', 'utf8');
    logger.info(`[MONITOR] Created results file: ${RESULTS_FILE}`);
  }
  
  // Set initial file position to end (ignore existing entries)
  const stats = fs.statSync(TEST_QUEUE_FILE);
  lastFilePosition = stats.size;
}

/**
 * Read new entries from test queue file
 */
function readNewEntries() {
  const stats = fs.statSync(TEST_QUEUE_FILE);
  
  if (stats.size <= lastFilePosition) {
    return []; // No new data
  }
  
  const fd = fs.openSync(TEST_QUEUE_FILE, 'r');
  const bufferSize = stats.size - lastFilePosition;
  const buffer = Buffer.alloc(bufferSize);
  
  fs.readSync(fd, buffer, 0, bufferSize, lastFilePosition);
  fs.closeSync(fd);
  
  lastFilePosition = stats.size;
  
  const newData = buffer.toString('utf8');
  const lines = newData.trim().split('\n').filter(line => line.trim());
  
  return lines.map(line => {
    try {
      return JSON.parse(line);
    } catch (error) {
      logger.error(`[MONITOR] Failed to parse line: ${line}`, error);
      return null;
    }
  }).filter(entry => entry !== null);
}

/**
 * Verify change in Bioloop database
 */
async function verifyChange(test) {
  const startTime = Date.now();
  const cmgTimestamp = new Date(test.timestamp);
  
  const mapping = FIELD_MAPPINGS[test.collection];
  if (!mapping) {
    throw new Error(`Unknown collection: ${test.collection}`);
  }
  
  const biolooField = mapping.fields[test.field];
  if (!biolooField) {
    throw new Error(`Unknown field mapping: ${test.collection}.${test.field}`);
  }
  
  // Poll until change appears or timeout
  while (Date.now() - startTime < TIMEOUT) {
    try {
      const record = await prisma[mapping.table].findFirst({
        where: { cmg_id: test.cmg_id }
      });
      
      if (!record) {
        logger.warn(`[MONITOR] Record not found: ${mapping.table} with cmg_id=${test.cmg_id}`);
        await sleep(POLL_INTERVAL);
        continue;
      }
      
      // Check if field matches expected value
      const actualValue = record[biolooField];
      const expectedValue = test.new_value;
      
      // Handle different value types
      let matches = false;
      if (typeof expectedValue === 'boolean') {
        matches = actualValue === expectedValue;
      } else if (typeof expectedValue === 'number') {
        matches = actualValue === expectedValue;
      } else if (typeof expectedValue === 'string') {
        matches = actualValue === expectedValue;
      } else {
        matches = JSON.stringify(actualValue) === JSON.stringify(expectedValue);
      }
      
      if (matches) {
        const detectionTime = new Date();
        const delay = detectionTime - cmgTimestamp;
        
        logger.info(`[MONITOR] ✓ Test ${test.test_id} verified in ${delay}ms`);
        logger.info(`[MONITOR]   Field: ${test.collection}.${test.field} = ${JSON.stringify(expectedValue)}`);
        
        return {
          test_id: test.test_id,
          status: 'success',
          cmg_timestamp: test.timestamp,
          bioloop_timestamp: detectionTime.toISOString(),
          delay_ms: delay,
          poller: test.poller,
          verified_value: actualValue,
        };
      }
      
      // Value doesn't match yet, keep polling
      await sleep(POLL_INTERVAL);
      
    } catch (error) {
      logger.error(`[MONITOR] Error checking database:`, error);
      await sleep(POLL_INTERVAL);
    }
  }
  
  // Timeout
  const detectionTime = new Date();
  const delay = detectionTime - cmgTimestamp;
  
  logger.error(`[MONITOR] ✗ Test ${test.test_id} TIMEOUT after ${delay}ms`);
  
  return {
    test_id: test.test_id,
    status: 'timeout',
    cmg_timestamp: test.timestamp,
    delay_ms: delay,
    poller: test.poller,
    error: 'Change not detected within 60s',
  };
}

/**
 * Write result to results file
 */
function writeResult(result) {
  fs.appendFileSync(RESULTS_FILE, JSON.stringify(result) + '\n', 'utf8');
}

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Main monitoring loop
 */
async function monitorLoop() {
  while (true) {
    try {
      const newEntries = readNewEntries();
      
      for (const test of newEntries) {
        if (processedTests.has(test.test_id)) {
          continue; // Already processed
        }
        
        logger.info(`[MONITOR] New test detected: ${test.test_id}`);
        logger.info(`[MONITOR]   Collection: ${test.collection}`);
        logger.info(`[MONITOR]   CMG ID: ${test.cmg_id}`);
        logger.info(`[MONITOR]   Field: ${test.field} → ${JSON.stringify(test.new_value)}`);
        logger.info(`[MONITOR]   Expected poller: ${test.poller}`);
        
        processedTests.add(test.test_id);
        
        // Verify change (this will poll until found or timeout)
        const result = await verifyChange(test);
        
        // Write result
        writeResult(result);
        
        if (result.status === 'success') {
          logger.info(`[MONITOR] ✓ Result: SUCCESS (${result.delay_ms}ms delay)`);
        } else {
          logger.error(`[MONITOR] ✗ Result: ${result.status.toUpperCase()}`);
        }
      }
      
      await sleep(1000); // Check queue file every second
      
    } catch (error) {
      logger.error('[MONITOR] Error in monitoring loop:', error);
      await sleep(5000); // Wait longer on error
    }
  }
}

/**
 * Graceful shutdown
 */
async function shutdown() {
  logger.info('[MONITOR] Shutting down...');
  await prisma.$disconnect();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

/**
 * Main entry point
 */
async function main() {
  logger.info('================================================================================');
  logger.info('CMG to Bioloop Sync Test Monitor');
  logger.info('================================================================================');
  logger.info(`Target database: ${targetDb}`);
  logger.info(`Database URL: ${databaseUrl.replace(/:[^:@]+@/, ':<credentials>@')}`);
  logger.info(`Test queue file: ${TEST_QUEUE_FILE}`);
  logger.info(`Results file: ${RESULTS_FILE}`);
  logger.info('');
  logger.info('Waiting for test entries from CMG agent...');
  logger.info('Press Ctrl+C to stop');
  logger.info('');
  
  try {
    await prisma.$connect();
    logger.info('[OK] Connected to Bioloop database');
    
    initializeTestFiles();
    
    await monitorLoop();
    
  } catch (error) {
    logger.error('[MONITOR] Fatal error:', error);
    await prisma.$disconnect();
    process.exit(1);
  }
}

main();
