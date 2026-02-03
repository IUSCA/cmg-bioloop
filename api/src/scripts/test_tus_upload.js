#!/usr/bin/env node
/**
 * Test TUS Upload Script
 * 
 * Tests the TUS upload endpoint by uploading a small test file.
 * Run from within API container or workers container.
 */

const fs = require('fs');
const path = require('path');
const tus = require('tus-js-client');

// Configuration
const API_URL = process.env.API_URL || 'http://localhost:3030';
const TOKEN = process.env.TEMP_AGENT_ACCESS || process.env.APP_API_TOKEN;
const DATASET_ID = process.argv[2] || '7589'; // Pass dataset ID as argument

if (!TOKEN) {
  console.error('Error: No authentication token found.');
  console.error('Set TEMP_AGENT_ACCESS or APP_API_TOKEN environment variable.');
  process.exit(1);
}

console.log('TUS Upload Test');
console.log('================');
console.log(`API URL: ${API_URL}`);
console.log(`Token: ${TOKEN.substring(0, 20)}...`);
console.log(`Dataset ID: ${DATASET_ID}`);
console.log('');

// Create a small test file
const testFilePath = '/tmp/test-upload.txt';
const testContent = `Test upload file created at ${new Date().toISOString()}\n`;
fs.writeFileSync(testFilePath, testContent);

console.log(`Created test file: ${testFilePath} (${testContent.length} bytes)`);
console.log('');

// Create a readable stream for the file
const fileStream = fs.createReadStream(testFilePath);
const fileSize = fs.statSync(testFilePath).size;

// Upload with TUS  
const upload = new tus.Upload(fileStream, {
  endpoint: `${API_URL}/uploads/files`,  // No /api prefix when calling directly
  retryDelays: [0, 1000, 3000],
  uploadSize: fileSize,
  metadata: {
    entity_type: 'dataset',
    entity_id: String(DATASET_ID),
    filename: 'test-upload.txt',
    filetype: 'text/plain',
    selection_mode: 'files',
    relative_path: 'test-upload.txt',
    directory_name: '',
  },
  headers: {
    Authorization: `Bearer ${TOKEN}`,
  },
  onError: (error) => {
    console.error('Upload failed:');
    console.error(error);
    process.exit(1);
  },
  onProgress: (bytesUploaded, bytesTotal) => {
    const percentage = ((bytesUploaded / bytesTotal) * 100).toFixed(2);
    console.log(`Progress: ${bytesUploaded}/${bytesTotal} bytes (${percentage}%)`);
  },
  onSuccess: () => {
    console.log('');
    console.log('✅ Upload successful!');
    console.log(`Upload ID: ${upload.url}`);
    
    // Clean up
    fs.unlinkSync(testFilePath);
    console.log('Cleaned up test file.');
    process.exit(0);
  },
});

console.log('Starting upload...');
console.log('');

upload.start();
