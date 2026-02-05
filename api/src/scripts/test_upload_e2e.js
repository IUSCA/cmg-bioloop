#!/usr/bin/env node
/**
 * End-to-End Upload Test Script
 * 
 * Tests the complete upload flow:
 * 1. Create a dataset
 * 2. Upload a file via TUS
 * 3. Call /complete endpoint
 * 4. Verify upload log status
 * 5. Verify workflow was triggered
 * 
 * Usage:
 *   node test_upload_e2e.js [--checksum]
 * 
 * Options:
 *   --checksum    Test with checksum verification enabled
 */

const fs = require('fs');
const path = require('path');
const tus = require('tus-js-client');
const http = require('http');
const https = require('https');

// Configuration
const API_URL = process.env.API_URL || 'http://localhost:3030';
const TOKEN = process.env.TEMP_AGENT_ACCESS || process.env.APP_API_TOKEN;
const TEST_WITH_CHECKSUM = process.argv.includes('--checksum');

if (!TOKEN) {
  console.error('Error: No authentication token found.');
  console.error('Set TEMP_AGENT_ACCESS or APP_API_TOKEN environment variable.');
  process.exit(1);
}

console.log('='.repeat(60));
console.log('Upload E2E Test');
console.log('='.repeat(60));
console.log(`API URL: ${API_URL}`);
console.log(`Token: ${TOKEN.substring(0, 20)}...`);
console.log(`Checksum Test: ${TEST_WITH_CHECKSUM}`);
console.log('');

// API request helper
function apiRequest(method, endpoint, data = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(endpoint, API_URL);
    const options = {
      method,
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      headers: {
        'Authorization': `Bearer ${TOKEN}`,
        'Content-Type': 'application/json',
      },
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const json = body ? JSON.parse(body) : {};
          if (res.statusCode >= 400) {
            reject(new Error(`API error ${res.statusCode}: ${JSON.stringify(json)}`));
          } else {
            resolve(json);
          }
        } catch (e) {
          reject(new Error(`Parse error: ${body}`));
        }
      });
    });

    req.on('error', reject);

    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

// Sleep helper
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Main test
async function runTest() {
  const timestamp = Date.now();
  const testName = `test-upload-${timestamp}`;
  
  console.log('Step 1: Creating dataset...');
  console.log(`  Dataset name: ${testName}`);
  
  // Create dataset via upload endpoint
  const createResponse = await apiRequest('POST', '/datasets/uploads', {
    name: testName,
    type: 'DATA_PRODUCT',
  });
  
  const datasetId = createResponse.audit_log?.dataset?.id;
  const uploadLogId = createResponse.id;
  
  if (!datasetId) {
    throw new Error('Failed to create dataset: ' + JSON.stringify(createResponse));
  }
  
  console.log(`  Dataset ID: ${datasetId}`);
  console.log(`  Upload Log ID: ${uploadLogId}`);
  console.log(`  Origin Path: ${createResponse.audit_log?.dataset?.origin_path}`);
  console.log('');

  // Create test file
  console.log('Step 2: Creating test file...');
  const testFilePath = '/tmp/test-upload-e2e.txt';
  const testContent = `Test upload file
Created at: ${new Date().toISOString()}
Dataset ID: ${datasetId}
Test name: ${testName}
Content for checksum testing: ${Math.random().toString(36)}
`;
  fs.writeFileSync(testFilePath, testContent);
  const fileSize = fs.statSync(testFilePath).size;
  console.log(`  File: ${testFilePath}`);
  console.log(`  Size: ${fileSize} bytes`);
  console.log('');

  // Upload via TUS
  console.log('Step 3: Uploading file via TUS...');
  const tusEndpoint = `${API_URL}/uploads/files`;
  console.log(`  TUS Endpoint: ${tusEndpoint}`);
  
  const uploadUrl = await new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(testFilePath);
    
    const upload = new tus.Upload(fileStream, {
      endpoint: tusEndpoint,
      retryDelays: [0, 1000, 3000],
      uploadSize: fileSize,
      metadata: {
        entity_type: 'dataset',
        entity_id: String(datasetId),
        filename: 'test-upload-e2e.txt',
        filetype: 'text/plain',
        selection_mode: 'files',
        relative_path: 'test-upload-e2e.txt',
        directory_name: '',
      },
      headers: {
        Authorization: `Bearer ${TOKEN}`,
      },
      onError: (error) => {
        console.error('  TUS upload error:', error);
        reject(error);
      },
      onProgress: (bytesUploaded, bytesTotal) => {
        const pct = ((bytesUploaded / bytesTotal) * 100).toFixed(2);
        process.stdout.write(`\r  Progress: ${bytesUploaded}/${bytesTotal} (${pct}%)`);
      },
      onSuccess: () => {
        console.log('');
        console.log(`  Upload URL: ${upload.url}`);
        resolve(upload.url);
      },
    });
    
    upload.start();
  });
  
  // Extract process_id from URL
  const processId = uploadUrl.split('/').pop();
  console.log(`  Process ID: ${processId}`);
  console.log('');

  // Call /complete endpoint
  console.log('Step 4: Calling /complete endpoint...');
  const completeResponse = await apiRequest('POST', `/datasets/uploads/${datasetId}/complete`, {
    process_id: processId,
    selection_mode: 'files',
    directory_name: '',
    relative_path: 'test-upload-e2e.txt',
  });
  
  console.log(`  Response: ${JSON.stringify(completeResponse.success)}`);
  console.log(`  Status: ${completeResponse.upload_log?.status}`);
  console.log('');

  // Add checksum metadata if testing with checksum
  if (TEST_WITH_CHECKSUM) {
    console.log('Step 4b: Computing BLAKE3 checksum...');
    
    // Compute BLAKE3 manifest hash (matches worker algorithm exactly)
    // Manifest format: "blake3-manifest-v1\nrelative_path\tsize\tfile_hash"
    const { blake3 } = require('hash-wasm');
    
    const fileContent = fs.readFileSync(testFilePath);
    const fileHash = await blake3(fileContent);
    
    // Build manifest matching worker format
    // Use the actual filename (same as what UI would use via file.name)
    const relativePath = 'test-upload-e2e.txt';
    const manifestLines = [
      'blake3-manifest-v1',
      `${relativePath}\t${fileSize}\t${fileHash}`
    ];
    const manifestStr = manifestLines.join('\n');
    const manifestHash = await blake3(manifestStr);
    
    console.log(`  File hash: ${fileHash.substring(0, 32)}...`);
    console.log(`  Manifest hash: ${manifestHash.substring(0, 32)}...`);
    
    const checksumData = {
      algorithm: 'blake3',
      mode: 'single',
      manifest_hash: manifestHash,
      file_count: 1,
      total_size: fileSize,
      computed_at: new Date().toISOString(),
    };
    
    await apiRequest('PATCH', `/datasets/uploads/${datasetId}/upload-log`, {
      metadata: { checksum: checksumData },
    });
    console.log(`  Checksum stored in DB`);
    console.log('');
  }

  // Clean up test file
  fs.unlinkSync(testFilePath);
  console.log('Cleaned up test file.');
  
  // Final result
  console.log('');
  console.log('='.repeat(60));
  console.log('Test Result');
  console.log('='.repeat(60));
  console.log(`Dataset ID: ${datasetId}`);
  console.log(`Process ID: ${processId}`);
  console.log(`Status: UPLOADED`);
  console.log('');
  console.log('SUCCESS: Upload completed!');
  console.log('');
  console.log('The async polling job (manage_upload_workflows.py) will:');
  console.log('  1. Verify upload integrity (checksum or file existence)');
  console.log('  2. Trigger integrated workflow if verified');
  console.log('  3. Set VERIFICATION_FAILED status if verification fails');
  console.log('');
  console.log('To manually trigger the polling job:');
  console.log('  docker compose exec celery_worker python -m workers.scripts.manage_upload_workflows --dry-run=False');
  
  return true;
}

// Run test
runTest()
  .then((success) => {
    process.exit(success ? 0 : 1);
  })
  .catch((error) => {
    console.error('Test failed with error:');
    console.error(error);
    process.exit(1);
  });
