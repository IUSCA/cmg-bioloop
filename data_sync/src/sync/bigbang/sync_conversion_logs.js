/**
 * CMG Conversion Logs Migration
 * 
 * Migrates historical conversion logs from CMG filesystem to Bioloop database.
 * 
 * CMG Log Storage:
 * - Logs stored as files: /N/project/CMG-SCA/production/runlogs/convert_{dataset_name}.log
 * - Multiple conversions for same dataset append to same file
 * - Each conversion section marked by "logfile header" text
 * 
 * Bioloop Log Storage:
 * - worker_process table: Process metadata
 * - log table: Individual log entries linked to worker_process
 * 
 * Relationship:
 * conversion.workflow_id -> worker_process.workflow_id -> log.worker_process_id
 */

const fs = require('fs').promises;
const path = require('path');
const logger = require('../../logger');
const { ObjectId } = require('mongodb');

/**
 * Parse a CMG log file and split into sections by conversion
 * Each section starts with "logfile header"
 */
function parseLogFileIntoSections(logContent) {
  const sections = [];
  const lines = logContent.split('\n');
  
  let currentSection = [];
  let sectionIndex = 0;
  
  for (const line of lines) {
    if (line.includes('logfile header')) {
      // Start of new conversion section
      if (currentSection.length > 0) {
        sections.push({
          index: sectionIndex,
          lines: currentSection,
        });
        sectionIndex++;
      }
      currentSection = [line];
    } else {
      currentSection.push(line);
    }
  }
  
  // Add final section
  if (currentSection.length > 0) {
    sections.push({
      index: sectionIndex,
      lines: currentSection,
    });
  }
  
  return sections;
}

/**
 * Infer log level from message content
 * CMG logs don't have structured levels, so we guess based on keywords
 */
function inferLogLevel(message) {
  const lowerMsg = message.toLowerCase();
  
  if (lowerMsg.includes('error') || lowerMsg.includes('failed') || lowerMsg.includes('exception')) {
    return 'ERROR';
  }
  if (lowerMsg.includes('warn') || lowerMsg.includes('warning')) {
    return 'WARN';
  }
  if (lowerMsg.includes('debug')) {
    return 'DEBUG';
  }
  
  return 'INFO';
}

/**
 * Convert log section lines into structured log entries
 */
function parseLogLines(lines, baseTimestamp) {
  const logs = [];
  let currentTime = new Date(baseTimestamp);
  
  for (const line of lines) {
    if (!line || line.trim() === '') {
      continue;
    }
    
    // Create log entry
    logs.push({
      timestamp: new Date(currentTime),
      message: line,
      level: inferLogLevel(line),
    });
    
    // Increment time by 1 second for each line (approximation since CMG logs don't have per-line timestamps)
    currentTime = new Date(currentTime.getTime() + 1000);
  }
  
  return logs;
}

/**
 * Sync conversion logs for a single conversion
 */
async function syncConversionLogs(prisma, cmgDb, conversion, runlogsDir) {
  try {
    const cmgId = conversion.cmg_id;
    if (!cmgId) {
      logger.debug(`[CONVERSION LOGS] Skipping conversion ${conversion.id} - no cmg_id`);
      return { success: false, reason: 'no_cmg_id' };
    }
    
    // Get CMG conversion to find dataset name
    const cmgConversion = await cmgDb.collection('conversions').findOne({
      _id: new ObjectId(cmgId),
    });
    
    if (!cmgConversion) {
      logger.warn(`[CONVERSION LOGS] CMG conversion not found: ${cmgId}`);
      return { success: false, reason: 'cmg_conversion_not_found' };
    }
    
    // Get CMG dataset to find name
    if (!cmgConversion.dataset) {
      logger.debug(`[CONVERSION LOGS] No dataset for CMG conversion: ${cmgId}`);
      return { success: false, reason: 'no_dataset' };
    }
    
    const cmgDataset = await cmgDb.collection('datasets').findOne({
      _id: new ObjectId(cmgConversion.dataset),
    });
    
    if (!cmgDataset || !cmgDataset.name) {
      logger.warn(`[CONVERSION LOGS] CMG dataset not found or no name: ${cmgConversion.dataset}`);
      return { success: false, reason: 'dataset_not_found' };
    }
    
    const datasetName = cmgDataset.name;
    const logFileName = `convert_${datasetName}.log`;
    const logFilePath = path.join(runlogsDir, logFileName);
    
    // Check if log file exists
    try {
      await fs.access(logFilePath);
    } catch (err) {
      logger.debug(`[CONVERSION LOGS] Log file not found: ${logFilePath}`);
      return { success: false, reason: 'log_file_not_found' };
    }
    
    // Read log file
    const logContent = await fs.readFile(logFilePath, 'utf-8');
    
    // Parse into sections
    const sections = parseLogFileIntoSections(logContent);
    
    if (sections.length === 0) {
      logger.debug(`[CONVERSION LOGS] No log sections found in: ${logFilePath}`);
      return { success: false, reason: 'no_log_sections' };
    }
    
    // Find all CMG conversions for this dataset (to determine which section belongs to our conversion)
    const allCmgConversionsForDataset = await cmgDb.collection('conversions')
      .find({ dataset: new ObjectId(cmgConversion.dataset) })
      .sort({ createdAt: 1 })
      .toArray();
    
    // Find the index of our conversion
    const conversionIndex = allCmgConversionsForDataset.findIndex(
      c => c._id.toString() === cmgId
    );
    
    if (conversionIndex === -1 || conversionIndex >= sections.length) {
      logger.debug(`[CONVERSION LOGS] Section index out of range for conversion ${cmgId} (index: ${conversionIndex}, sections: ${sections.length})`);
      return { success: false, reason: 'section_index_out_of_range' };
    }
    
    const logSection = sections[conversionIndex];
    
    // Check if worker_process already exists for this conversion's workflow
    if (!conversion.workflow_id) {
      logger.debug(`[CONVERSION LOGS] No workflow_id for conversion ${conversion.id}`);
      return { success: false, reason: 'no_workflow_id' };
    }
    
    const existingProcess = await prisma.worker_process.findFirst({
      where: {
        workflow_id: conversion.workflow_id,
        step: 'convert',
      },
    });
    
    if (existingProcess) {
      logger.debug(`[CONVERSION LOGS] Worker process already exists for conversion ${conversion.id}`);
      return { success: false, reason: 'already_exists' };
    }
    
    // Create worker_process record
    const workerProcess = await prisma.worker_process.create({
      data: {
        pid: 0, // CMG didn't track PIDs in logs
        task_id: conversion.workflow_id, // Use workflow_id as task_id
        step: 'convert',
        workflow_id: conversion.workflow_id,
        tags: {
          cmg_dataset_name: datasetName,
          cmg_conversion_id: cmgId,
          cmg_log_file: logFileName,
          cmg_log_section_index: conversionIndex,
          migration_note: 'Historical CMG conversion logs migrated from filesystem',
        },
        start_time: cmgConversion.createdAt || new Date(),
        hostname: 'cmg-legacy', // CMG didn't track hostname in old logs
      },
    });
    
    logger.debug(`[CONVERSION LOGS] Created worker_process ${workerProcess.id} for conversion ${conversion.id}`);
    
    // Parse log lines
    const logEntries = parseLogLines(logSection.lines, cmgConversion.createdAt || new Date());
    
    if (logEntries.length === 0) {
      logger.debug(`[CONVERSION LOGS] No log entries parsed for conversion ${conversion.id}`);
      return { success: true, log_count: 0 };
    }
    
    // Insert logs in batches to avoid memory issues
    const BATCH_SIZE = 1000;
    let totalInserted = 0;
    
    for (let i = 0; i < logEntries.length; i += BATCH_SIZE) {
      const batch = logEntries.slice(i, i + BATCH_SIZE);
      const logData = batch.map(entry => ({
        timestamp: entry.timestamp,
        message: entry.message,
        level: entry.level,
        worker_process_id: workerProcess.id,
      }));
      
      await prisma.log.createMany({
        data: logData,
        skipDuplicates: true,
      });
      
      totalInserted += batch.length;
    }
    
    logger.info(`[CONVERSION LOGS] Migrated ${totalInserted} log entries for conversion ${conversion.id} (CMG: ${cmgId})`);
    
    return { success: true, log_count: totalInserted };
    
  } catch (error) {
    logger.error(`[CONVERSION LOGS] Error syncing logs for conversion ${conversion.id}:`, error);
    return { success: false, reason: 'error', error: error.message };
  }
}

/**
 * Main function to sync all conversion logs
 */
async function syncAllConversionLogs(prisma, cmgDb, runlogsDir) {
  logger.info('[CONVERSION LOGS] Starting conversion logs migration...');
  logger.info(`[CONVERSION LOGS] Reading logs from: ${runlogsDir}`);
  
  // Check if runlogs directory exists
  try {
    await fs.access(runlogsDir);
  } catch (err) {
    logger.error(`[CONVERSION LOGS] Runlogs directory not accessible: ${runlogsDir}`);
    logger.error('[CONVERSION LOGS] This is expected in non-production environments.');
    logger.error('[CONVERSION LOGS] Skipping conversion logs migration.');
    return;
  }
  
  // Get all conversions with cmg_id
  const conversions = await prisma.conversion.findMany({
    where: {
      cmg_id: { not: null },
    },
    orderBy: {
      initiated_at: 'asc',
    },
  });
  
  logger.info(`[CONVERSION LOGS] Found ${conversions.length} conversions to process`);
  
  const stats = {
    total: conversions.length,
    success: 0,
    skipped: 0,
    failed: 0,
    total_logs: 0,
    skip_reasons: {},
  };
  
  for (const conversion of conversions) {
    const result = await syncConversionLogs(prisma, cmgDb, conversion, runlogsDir);
    
    if (result.success) {
      stats.success++;
      stats.total_logs += result.log_count || 0;
    } else {
      stats.skipped++;
      const reason = result.reason || 'unknown';
      stats.skip_reasons[reason] = (stats.skip_reasons[reason] || 0) + 1;
    }
    
    // Progress logging every 100 conversions
    if ((stats.success + stats.skipped) % 100 === 0) {
      logger.info(`[CONVERSION LOGS] Progress: ${stats.success + stats.skipped}/${stats.total} conversions processed`);
    }
  }
  
  logger.info('[CONVERSION LOGS] ============================================');
  logger.info(`[CONVERSION LOGS] Migration complete:`);
  logger.info(`[CONVERSION LOGS]   - Total conversions: ${stats.total}`);
  logger.info(`[CONVERSION LOGS]   - Successfully migrated: ${stats.success}`);
  logger.info(`[CONVERSION LOGS]   - Skipped: ${stats.skipped}`);
  logger.info(`[CONVERSION LOGS]   - Total log entries: ${stats.total_logs}`);
  logger.info('[CONVERSION LOGS] ');
  logger.info('[CONVERSION LOGS] Skip reasons:');
  Object.entries(stats.skip_reasons)
    .sort((a, b) => b[1] - a[1])
    .forEach(([reason, count]) => {
      logger.info(`[CONVERSION LOGS]   - ${reason}: ${count}`);
    });
  logger.info('[CONVERSION LOGS] ============================================');
}

module.exports = {
  syncAllConversionLogs,
  syncConversionLogs,
};

