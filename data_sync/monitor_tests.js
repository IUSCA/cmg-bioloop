const { PrismaClient } = require('@prisma/client');
const fs = require('fs');

const prisma = new PrismaClient({
  datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
});

const TEST_QUEUE = '/tmp/cmg-bioloop/test_queue.jsonl';
const RESULTS_FILE = '/tmp/cmg-bioloop/test_results.jsonl';
const COORD_LOG = '/tmp/cmg-bioloop/coordination.log';

let processedTests = new Set();
let lastFileSize = 0;

function log(message) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] [BIOLOOP] [MONITOR] ${message}\n`;
  fs.appendFileSync(COORD_LOG, logEntry);
  console.log(`[${timestamp}] ${message}`);
}

async function verifyTest(test) {
  const now = new Date();
  const startTime = Date.now();
  
  log(`🔍 Verifying ${test.test_id}: ${test.collection}.${test.field}`);
  
  try {
    let record;
    
    if (test.collection === 'projects') {
      record = await prisma.project.findFirst({
        where: { cmg_id: test.cmg_id },
        select: { [test.bioloop_field]: true, updated_at: true }
      });
    } else if (test.collection === 'datasets' || test.collection === 'dataproducts') {
      record = await prisma.dataset.findFirst({
        where: { cmg_id: test.cmg_id },
        select: { [test.bioloop_field]: true, updated_at: true }
      });
    } else if (test.collection === 'users') {
      // For users, need to get roles from user_role table
      const user = await prisma.user.findFirst({
        where: { cmg_id: test.cmg_id },
        include: { user_role: { include: { roles: true } } }
      });
      
      if (!user) {
        throw new Error(`User not found with cmg_id: ${test.cmg_id}`);
      }
      
      // Get current roles as array of role names
      const currentRoles = user.user_role.map(ur => ur.roles.name);
      
      record = {
        user_roles: currentRoles,
        updated_at: user.updated_at
      };
    } else {
      throw new Error(`Unknown collection: ${test.collection}`);
    }
    
    if (!record) {
      log(`❌ ${test.test_id}: Record not found (cmg_id: ${test.cmg_id})`);
      const result = {
        test_id: test.test_id,
        status: 'error',
        error: 'Record not found in Bioloop',
        verified_at: now.toISOString()
      };
      fs.appendFileSync(RESULTS_FILE, JSON.stringify(result) + '\n');
      return;
    }
    
    const actualValue = record[test.bioloop_field];
    const expectedValue = test.new_value;
    const bioloopTimestamp = record.updated_at;
    
    // Convert BigInt to string for comparison
    const actualValueStr = typeof actualValue === 'bigint' ? actualValue.toString() : actualValue;
    const expectedValueStr = typeof expectedValue === 'bigint' ? expectedValue.toString() : expectedValue;
    
    // Check if synced
    if (JSON.stringify(actualValueStr) === JSON.stringify(expectedValueStr)) {
      const cmgTime = new Date(test.cmg_updated_at);
      const bioloopTime = new Date(bioloopTimestamp);
      const delayMs = bioloopTime - cmgTime;
      
      log(`✅ ${test.test_id}: SUCCESS - Synced in ${delayMs}ms (${(delayMs/1000).toFixed(1)}s)`);
      
      const result = {
        test_id: test.test_id,
        status: 'success',
        cmg_timestamp: test.cmg_updated_at,
        bioloop_timestamp: bioloopTimestamp.toISOString(),
        delay_ms: delayMs,
        verified_value: actualValueStr,
        poller: test.poller,
        verified_at: now.toISOString()
      };
      fs.appendFileSync(RESULTS_FILE, JSON.stringify(result) + '\n');
      return true; // Success!
    }
    
    return false; // Not synced yet
    
  } catch (error) {
    log(`❌ ${test.test_id}: ERROR - ${error.message}`);
    const result = {
      test_id: test.test_id,
      status: 'error',
      error: error.message,
      verified_at: now.toISOString()
    };
    fs.appendFileSync(RESULTS_FILE, JSON.stringify(result) + '\n');
    return null; // Error
  }
}

async function pollForSync(test, maxWaitMs = 60000) {
  const startTime = Date.now();
  const pollInterval = 2000; // Check every 2 seconds
  
  while (Date.now() - startTime < maxWaitMs) {
    const result = await verifyTest(test);
    
    if (result === true) {
      return; // Success!
    } else if (result === null) {
      return; // Error occurred
    }
    
    // Not synced yet, wait and try again
    await new Promise(resolve => setTimeout(resolve, pollInterval));
  }
  
  // Timeout
  log(`⏱️  ${test.test_id}: TIMEOUT - No sync after ${maxWaitMs/1000}s`);
  const result = {
    test_id: test.test_id,
    status: 'timeout',
    cmg_timestamp: test.cmg_updated_at,
    delay_ms: maxWaitMs,
    poller: test.poller,
    error: `Change not detected within ${maxWaitMs/1000}s`,
    verified_at: new Date().toISOString()
  };
  fs.appendFileSync(RESULTS_FILE, JSON.stringify(result) + '\n');
}

async function checkForNewTests() {
  try {
    const stats = fs.statSync(TEST_QUEUE);
    
    if (stats.size === lastFileSize) {
      return; // No changes
    }
    
    lastFileSize = stats.size;
    
    const content = fs.readFileSync(TEST_QUEUE, 'utf8');
    const lines = content.trim().split('\n').filter(l => l);
    
    for (const line of lines) {
      const test = JSON.parse(line);
      
      if (processedTests.has(test.test_id)) {
        continue; // Already processing/processed
      }
      
      processedTests.add(test.test_id);
      log(`📝 New test detected: ${test.test_id} - ${test.collection}.${test.field} → ${test.new_value}`);
      
      // Start polling for this test (non-blocking)
      pollForSync(test, 60000).catch(err => {
        log(`❌ Error polling ${test.test_id}: ${err.message}`);
      });
    }
    
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.error('Error checking queue:', error.message);
    }
  }
}

async function main() {
  log('🚀 Monitor started - watching for CMG agent test submissions');
  log(`   Queue: ${TEST_QUEUE}`);
  log(`   Results: ${RESULTS_FILE}`);
  log(`   Polling interval: 2 seconds per test`);
  log(`   Timeout: 60 seconds per test`);
  
  // Check every 3 seconds for new tests
  setInterval(checkForNewTests, 3000);
  
  // Initial check
  checkForNewTests();
}

main().catch(console.error);
