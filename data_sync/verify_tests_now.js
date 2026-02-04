const { PrismaClient } = require('@prisma/client');
const fs = require('fs');

// Use direct connection string for app database
const DATABASE_URL = process.env.DATABASE_URL_APP || 'postgresql://appuser:example@postgres:5432/app';

const prisma = new PrismaClient({
  datasources: {
    db: { url: DATABASE_URL }
  }
});

async function verifyTests() {
  const tests = [
    JSON.parse('{"test_id":"test_001","batch":"batch_1","timestamp":"2026-02-04T02:49:29.378Z","collection":"projects","cmg_id":"606b521f02d8137b0ff40049","field":"description","bioloop_field":"description","old_value":"TEST_PROJECT_DESC_1770168819","new_value":"TEST_PROJECT_DESC_1770173369378","poller":"project_metadata","cmg_updated_at":"2026-02-04T02:49:29.378Z"}'),
    JSON.parse('{"test_id":"test_002","batch":"batch_1","timestamp":"2026-02-04T02:51:29.649Z","collection":"projects","cmg_id":"606b521f02d8137b0ff40049","field":"notes","bioloop_field":"notes","old_value":null,"new_value":"TEST_PROJECT_NOTES_1770173489649","poller":"project_metadata","cmg_updated_at":"2026-02-04T02:51:29.649Z"}'),
    JSON.parse('{"test_id":"test_003","batch":"batch_2","timestamp":"2026-02-04T02:53:29.951Z","collection":"users","cmg_id":"5f577fb638972540c2718122","field":"name","bioloop_field":"name","old_value":null,"new_value":"TEST_USER_1770173609951","poller":"user_roles","cmg_updated_at":"2026-02-04T02:53:29.951Z"}'),
    JSON.parse('{"test_id":"test_004","batch":"batch_2","timestamp":"2026-02-04T02:55:30.500Z","collection":"users","cmg_id":"5f577fb638972540c2718122","field":"email","bioloop_field":"email","old_value":"hongao@iu.edu","new_value":"test_1770173730500@bioloop-test.local","poller":"user_roles","cmg_updated_at":"2026-02-04T02:55:30.500Z"}')
  ];

  console.log('Starting verification of 4 tests...\n');

  for (const test of tests) {
    const now = new Date();
    console.log(`[${test.test_id}] Checking ${test.collection} ${test.cmg_id}...`);
    
    try {
      let record;
      if (test.collection === 'projects') {
        record = await prisma.project.findFirst({
          where: { cmg_id: test.cmg_id },
          select: { [test.bioloop_field]: true, updated_at: true }
        });
      } else if (test.collection === 'users') {
        record = await prisma.user.findFirst({
          where: { cmg_id: test.cmg_id },
          select: { [test.bioloop_field]: true, updated_at: true }
        });
      }

      if (!record) {
        console.log(`[${test.test_id}] ❌ RECORD NOT FOUND\n`);
        continue;
      }

      const actualValue = record[test.bioloop_field];
      const expectedValue = test.new_value;
      const bioloopTimestamp = record.updated_at;
      
      if (actualValue === expectedValue) {
        const cmgTime = new Date(test.cmg_updated_at);
        const bioloopTime = new Date(bioloopTimestamp);
        const delayMs = bioloopTime - cmgTime;
        
        console.log(`[${test.test_id}] ✅ SUCCESS`);
        console.log(`  Expected: "${expectedValue}"`);
        console.log(`  Actual:   "${actualValue}"`);
        console.log(`  CMG Time:     ${test.cmg_updated_at}`);
        console.log(`  Bioloop Time: ${bioloopTimestamp.toISOString()}`);
        console.log(`  Sync Delay:   ${delayMs}ms (${(delayMs/1000).toFixed(1)}s)`);
        
        // Write result
        const result = {
          test_id: test.test_id,
          status: 'success',
          cmg_timestamp: test.cmg_updated_at,
          bioloop_timestamp: bioloopTimestamp.toISOString(),
          delay_ms: delayMs,
          verified_value: actualValue,
          poller: test.poller,
          verified_at: now.toISOString()
        };
        fs.appendFileSync('/tmp/cmg-bioloop/test_results.jsonl', JSON.stringify(result) + '\n');
        console.log(`[${test.test_id}] ✓ Result written\n`);
      } else {
        console.log(`[${test.test_id}] ❌ VALUE MISMATCH`);
        console.log(`  Expected: "${expectedValue}"`);
        console.log(`  Actual:   "${actualValue}"\n`);
        
        const result = {
          test_id: test.test_id,
          status: 'mismatch',
          cmg_timestamp: test.cmg_updated_at,
          expected_value: expectedValue,
          actual_value: actualValue,
          poller: test.poller,
          verified_at: now.toISOString()
        };
        fs.appendFileSync('/tmp/cmg-bioloop/test_results.jsonl', JSON.stringify(result) + '\n');
      }
    } catch (error) {
      console.log(`[${test.test_id}] ❌ ERROR: ${error.message}\n`);
      const result = {
        test_id: test.test_id,
        status: 'error',
        error: error.message,
        verified_at: now.toISOString()
      };
      fs.appendFileSync('/tmp/cmg-bioloop/test_results.jsonl', JSON.stringify(result) + '\n');
    }
  }

  await prisma.$disconnect();
  console.log('Verification complete!');
}

verifyTests().catch(console.error);
