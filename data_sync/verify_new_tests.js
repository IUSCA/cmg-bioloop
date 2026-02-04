const { PrismaClient } = require('@prisma/client');
const fs = require('fs');

const prisma = new PrismaClient({
  datasources: { db: { url: 'postgresql://appuser:example@postgres:5432/app' } }
});

(async () => {
  const tests = [
    {id: 'verify_001_corrected', cmg_id: '606b521f02d8137b0ff4004a', collection: 'projects', field: 'description', expected: 'VERIFY_PROJECT_DESC_1770198817489', cmg_updated_at: '2026-02-04T09:53:37.489Z'},
    {id: 'verify_002', cmg_id: '606b521f02d8137b0ff4004a', collection: 'projects', field: 'browser_enabled', expected: true, cmg_updated_at: '2026-02-04T09:54:01.530Z'},
    {id: 'verify_003_corrected', cmg_id: '5fbd7266c64cc935805cc9d3', collection: 'datasets', field: 'description', expected: 'VERIFY_DATASET_DESC_1770198852219', cmg_updated_at: '2026-02-04T09:54:12.219Z'}
  ];
  
  console.log('=== Verifying 3 New Tests ===\n');
  
  const results = [];
  
  for (const test of tests) {
    console.log(`🔍 ${test.id} - ${test.collection}.${test.field}`);
    
    let record;
    if (test.collection === 'projects') {
      record = await prisma.project.findFirst({
        where: { cmg_id: test.cmg_id },
        select: { [test.field]: true, updated_at: true }
      });
    } else {
      record = await prisma.dataset.findFirst({
        where: { cmg_id: test.cmg_id },
        select: { [test.field]: true, updated_at: true }
      });
    }
    
    if (!record) {
      console.log('  ❌ Record not found\n');
      results.push({...test, status: 'error', error: 'Record not found'});
      continue;
    }
    
    const current = record[test.field];
    const matches = JSON.stringify(current) === JSON.stringify(test.expected);
    
    console.log(`  Expected: ${JSON.stringify(test.expected)}`);
    console.log(`  Current:  ${JSON.stringify(current)}`);
    console.log(`  Updated:  ${record.updated_at.toISOString()}`);
    
    if (matches) {
      const cmgTime = new Date(test.cmg_updated_at);
      const bioloopTime = new Date(record.updated_at);
      const delayMs = bioloopTime - cmgTime;
      console.log(`  ✅ SYNCED! Delay: ${delayMs}ms (${(delayMs/1000).toFixed(1)}s)\n`);
      results.push({...test, status: 'success', delay_ms: delayMs, bioloop_updated_at: record.updated_at.toISOString()});
    } else {
      console.log(`  ⏳ NOT SYNCED YET\n`);
      results.push({...test, status: 'pending', current_value: current});
    }
  }
  
  // Write results
  const resultsFile = '/tmp/cmg-bioloop/verify_results.json';
  fs.writeFileSync(resultsFile, JSON.stringify(results, null, 2));
  
  const synced = results.filter(r => r.status === 'success').length;
  console.log(`\n=== Summary: ${synced}/3 tests synced ===`);
  
  await prisma.$disconnect();
})();
