#!/usr/bin/env node

/**
 * Populate Bundles Script
 * 
 * Standalone script to populate the bundle table with MD5 checksums from HPSS.
 * This script is meant to be run directly on the remote host where HSI is available.
 * 
 * NOT part of bigbang/poller process - run separately as needed.
 * 
 * Usage:
 *   node populate_bundles.js [options]
 * 
 * Options:
 *   --target-db=<target>    Target database: sandbox (default), app, or custom
 *   --dry-run               Show what would be done without making changes
 *   --limit=<n>             Process only N datasets (for testing)
 *   --help, -h              Show this help message
 * 
 * Examples:
 *   # Dry run (see what would happen):
 *   node populate_bundles.js --dry-run
 * 
 *   # Populate bundles in production database:
 *   node populate_bundles.js --target-db=app
 * 
 *   # Test with first 10 datasets:
 *   node populate_bundles.js --target-db=app --limit=10
 * 
 * Requirements:
 *   - Must run on a host with HSI binary available
 *   - HSI must be authenticated (Kerberos ticket, etc.)
 *   - Database connection configured (via .env or environment variables)
 */

const { spawn } = require('child_process');
const { PrismaClient } = require('@prisma/client');
const config = require('config');
const { setDatabaseUrl } = require('./src/utils/db_config');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

/**
 * Execute HSI command locally (on the same host where this script runs)
 */
async function executeHsi(command, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const process = spawn('hsi', ['-P', command], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    process.stdout.on('data', (data) => {
      stdout += data.toString('utf8');
    });

    process.stderr.on('data', (data) => {
      stderr += data.toString('utf8');
    });

    const timeout = setTimeout(() => {
      process.kill('SIGKILL');
      reject(new Error(`HSI command timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    process.on('error', (err) => {
      clearTimeout(timeout);
      reject(new Error(`Failed to spawn HSI: ${err.message}`));
    });

    process.on('close', (code) => {
      clearTimeout(timeout);
      
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(new Error(
          `HSI command failed with exit code ${code}\n` +
          `Command: ${command}\n` +
          `Stderr: ${stderr}\n` +
          `Stdout: ${stdout}`
        ));
      }
    });
  });
}

/**
 * Get MD5 checksum from HPSS for a file
 */
async function getHpssChecksum(archivePath) {
  try {
    const stdout = await executeHsi(`hashlist ${archivePath}`);
    
    // HSI hashlist output format: <checksum> <size> <path>
    // Example: "d41d8cd98f00b204e9800998ecf8427e  1024  /path/to/file.tar"
    const firstLine = stdout.trim().split('\n')[0];
    const checksum = firstLine.split(/\s+/)[0];
    
    if (checksum === '(none)' || !checksum || checksum.length !== 32) {
      return null;
    }
    
    return checksum;
  } catch (error) {
    console.error(`[ERROR] Failed to get checksum for ${archivePath}: ${error.message}`);
    return null;
  }
}

/**
 * Populate bundle for a single dataset
 */
async function populateBundle(prisma, dataset, dryRun = false) {
  // Check if bundle already exists
  const existingBundle = await prisma.bundle.findFirst({
    where: { dataset_id: dataset.id }
  });
  
  if (existingBundle) {
    console.log(`[SKIP] Dataset ${dataset.id} (${dataset.name}): bundle already exists`);
    return { status: 'skipped', reason: 'already_exists' };
  }
  
  // Get MD5 checksum from HPSS
  console.log(`[FETCH] Dataset ${dataset.id} (${dataset.name}): ${dataset.archive_path}`);
  const md5 = await getHpssChecksum(dataset.archive_path);
  
  if (!md5) {
    console.log(`[FAIL] Dataset ${dataset.id} (${dataset.name}): no checksum retrieved`);
    return { status: 'failed', reason: 'no_checksum' };
  }
  
  if (dryRun) {
    console.log(`[DRY-RUN] Would create bundle for dataset ${dataset.id} (${dataset.name}): ${md5}`);
    return { status: 'dry_run', md5 };
  }
  
  // Create bundle record
  await prisma.bundle.create({
    data: {
      name: `${dataset.name}.tar`,
      size: dataset.du_size,
      md5: md5,
      dataset_id: dataset.id,
    }
  });
  
  console.log(`[SUCCESS] Created bundle for dataset ${dataset.id} (${dataset.name}): ${md5}`);
  return { status: 'created', md5 };
}

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    targetDb: 'sandbox',
    dryRun: false,
    limit: null,
  };

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      console.log(`
Populate Bundles Script

Standalone script to populate the bundle table with MD5 checksums from HPSS.
This script is meant to be run directly on the remote host where HSI is available.

Usage:
  node populate_bundles.js [options]

Options:
  --target-db=<target>    Target database: sandbox (default), app, or custom
  --dry-run               Show what would be done without making changes
  --limit=<n>             Process only N datasets (for testing)
  --help, -h              Show this help message

Examples:
  # Dry run (see what would happen):
  node populate_bundles.js --dry-run

  # Populate bundles in production database:
  node populate_bundles.js --target-db=app

  # Test with first 10 datasets:
  node populate_bundles.js --target-db=app --limit=10

Requirements:
  - Must run on a host with HSI binary available
  - HSI must be authenticated (Kerberos ticket, etc.)
  - Database connection configured (via .env or environment variables)
`);
      process.exit(0);
    } else if (arg.startsWith('--target-db=')) {
      [, options.targetDb] = arg.split('=');
    } else if (arg === '--dry-run') {
      options.dryRun = true;
    } else if (arg.startsWith('--limit=')) {
      [, options.limit] = arg.split('=');
      options.limit = parseInt(options.limit, 10);
    } else {
      console.error(`Unknown option: ${arg}`);
      console.error('Run with --help for usage information');
      process.exit(1);
    }
  }

  return options;
}

/**
 * Main function
 */
async function main() {
  const options = parseArgs();
  
  console.log('');
  console.log('='.repeat(80));
  console.log('Bundle Population Script');
  console.log('='.repeat(80));
  console.log('');
  console.log(`Target Database: ${options.targetDb}`);
  console.log(`Dry Run: ${options.dryRun ? 'YES' : 'NO'}`);
  if (options.limit) {
    console.log(`Limit: ${options.limit} datasets`);
  }
  console.log('');
  
  // Set database URL
  const databaseUrl = setDatabaseUrl(options.targetDb);
  console.log(`Database URL: ${databaseUrl.replace(/\/\/.*@/, '//<credentials>@')}`);
  console.log('');
  
  // Create Prisma client
  const prisma = new PrismaClient();
  
  try {
    // Test database connection
    await prisma.$connect();
    console.log('[OK] Connected to database');
    console.log('');
    
    // Check if HSI is available
    console.log('[CHECK] Testing HSI availability...');
    try {
      await executeHsi('pwd');
      console.log('[OK] HSI is available and authenticated');
    } catch (error) {
      console.error('[ERROR] HSI is not available or not authenticated');
      console.error('        Make sure you are on a host with HSI installed and authenticated');
      console.error('        Run: kinit <username> (if using Kerberos)');
      process.exit(1);
    }
    console.log('');
    
    // Find archived datasets without bundles
    const query = {
      where: {
        archive_path: { not: null },
        bundle: null,
      },
      select: {
        id: true,
        name: true,
        archive_path: true,
        du_size: true,
      },
      orderBy: {
        id: 'asc',
      },
    };
    
    if (options.limit) {
      query.take = options.limit;
    }
    
    console.log('[QUERY] Finding archived datasets without bundles...');
    const datasets = await prisma.dataset.findMany(query);
    
    if (datasets.length === 0) {
      console.log('[INFO] No datasets need bundle population');
      console.log('       Either all bundles exist or no datasets are archived');
      process.exit(0);
    }
    
    console.log(`[INFO] Found ${datasets.length} archived datasets without bundles`);
    console.log('');
    
    // Process each dataset
    let created = 0;
    let skipped = 0;
    let failed = 0;
    
    for (let i = 0; i < datasets.length; i++) {
      const dataset = datasets[i];
      console.log(`[${i + 1}/${datasets.length}] Processing dataset ${dataset.id}...`);
      
      const result = await populateBundle(prisma, dataset, options.dryRun);
      
      if (result.status === 'created' || result.status === 'dry_run') {
        created++;
      } else if (result.status === 'skipped') {
        skipped++;
      } else if (result.status === 'failed') {
        failed++;
      }
      
      console.log('');
    }
    
    // Summary
    console.log('='.repeat(80));
    console.log('Summary');
    console.log('='.repeat(80));
    console.log(`Total datasets processed: ${datasets.length}`);
    console.log(`Created: ${created}`);
    console.log(`Skipped: ${skipped}`);
    console.log(`Failed: ${failed}`);
    console.log('');
    
    if (options.dryRun) {
      console.log('[DRY-RUN] No changes were made to the database');
      console.log('          Run without --dry-run to actually create bundles');
    } else {
      console.log('[DONE] Bundle population complete');
    }
    console.log('');
    
  } catch (error) {
    console.error('');
    console.error('='.repeat(80));
    console.error('[FAILED] Bundle population failed');
    console.error('='.repeat(80));
    console.error(`Error: ${error.message}`);
    if (error.stack) {
      console.error('');
      console.error('Stack trace:');
      console.error(error.stack);
    }
    console.error('');
    process.exit(1);
  } finally {
    await prisma.$disconnect();
  }
}

// Run if executed directly
if (require.main === module) {
  main().catch((error) => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = {
  executeHsi,
  getHpssChecksum,
  populateBundle,
};

