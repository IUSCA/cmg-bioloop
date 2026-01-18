const fs = require('fs').promises;
const path = require('path');
const logger = require('../../logger');

const BATCH_SIZE = 1000; // Batch size for inserting log entries

/**
 * Infer log level from message content
 */
function inferLogLevel(message) {
  const lowerMsg = message.toLowerCase();
  
  if (lowerMsg.includes('error') || lowerMsg.includes('failed') || lowerMsg.includes('exception')) {
    return 'ERROR';
  }
  if (lowerMsg.includes('warn') || lowerMsg.includes('warning')) {
    return 'WARNING';
  }
  if (lowerMsg.includes('info') || lowerMsg.includes('completed') || lowerMsg.includes('success')) {
    return 'INFO';
  }
  
  return 'DEBUG';
}

/**
 * Parse timestamp from log line
 * Expected format: YYYY-MM-DD HH:MM:SS
 */
function parseTimestamp(line) {
  const timestampPattern = /(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})/;
  const match = line.match(timestampPattern);
  
  if (match) {
    try {
      return new Date(match[1]);
    } catch (error) {
      return null;
    }
  }
  
  return null;
}

/**
 * Process conversion logs for a single dataset
 */
async function processDatasetConversionLogs(prisma, conversions, logFilePath, stats) {
  // Read log file once for all conversions on this dataset
  let logContent;
  try {
    logContent = await fs.readFile(logFilePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') {
      logger.warn(`[CONVERSION LOGS] Log file not found: ${logFilePath}`);
      stats.missingLogFiles.push(logFilePath);
    } else {
      logger.error(`[CONVERSION LOGS] Error reading log file ${logFilePath}: ${error.message}`);
      stats.errors.push({ file: logFilePath, error: error.message });
    }
    return;
  }
  
  const logLines = logContent.split('\n').filter(line => line.trim() !== '');
  
  if (logLines.length === 0) {
    logger.warn(`[CONVERSION LOGS] Empty log file: ${logFilePath}`);
    return;
  }
  
  stats.datasetsProcessed += 1;
  
  // Process each conversion in this dataset
  for (const conversion of conversions) {
    try {
      // Check if already processed
      if (conversion.workflow_id) {
        logger.debug(`[CONVERSION LOGS] Conversion ${conversion.id} already has workflow_id, skipping`);
        continue;
      }
      
      // Generate synthetic workflow_id
      const workflowId = `cmg-historic-conversion-${conversion.id}`;
      
      // Create worker_process record
      const workerProcess = await prisma.worker_process.create({
        data: {
          pid: 0,
          task_id: `cmg-conversion-${conversion.cmg_id}`,
          step: 'conversion',
          workflow_id: workflowId,
          hostname: 'cmg-historic',
          start_time: conversion.initiated_at,
          tags: {
            source: 'cmg-migration',
            conversion_id: conversion.id,
            cmg_conversion_id: conversion.cmg_id,
            dataset_id: conversion.dataset_id,
            dataset_name: conversion.dataset?.name || 'unknown',
          },
        },
      });
      
      logger.info(`[CONVERSION LOGS] Created worker_process ${workerProcess.id} for conversion ${conversion.id}`);
      
      // Prepare log entries (duplicate the entire log for this conversion)
      const logEntries = [];
      for (const line of logLines) {
        const timestamp = parseTimestamp(line) || conversion.initiated_at;
        const level = inferLogLevel(line);
        
        logEntries.push({
          timestamp,
          message: line,
          level,
          worker_process_id: workerProcess.id,
        });
        
        // Batch insert when we reach BATCH_SIZE
        if (logEntries.length >= BATCH_SIZE) {
          await prisma.log.createMany({
            data: logEntries,
            skipDuplicates: true,
          });
          stats.logEntriesCreated += logEntries.length;
          logEntries.length = 0; // Clear array
        }
      }
      
      // Insert remaining log entries
      if (logEntries.length > 0) {
        await prisma.log.createMany({
          data: logEntries,
          skipDuplicates: true,
        });
        stats.logEntriesCreated += logEntries.length;
      }
      
      // Update conversion with workflow_id
      await prisma.conversion.update({
        where: { id: conversion.id },
        data: { workflow_id: workflowId },
      });
      
      stats.conversionsUpdated += 1;
      stats.workerProcessesCreated += 1;
      
      logger.info(`[CONVERSION LOGS] Processed conversion ${conversion.id}: ${logLines.length} log entries duplicated`);
      
    } catch (error) {
      logger.error(`[CONVERSION LOGS] Error processing conversion ${conversion.id}: ${error.message}`);
      stats.errors.push({ conversion_id: conversion.id, error: error.message });
    }
  }
}

/**
 * Main function to sync all conversion logs
 */
async function syncConversionLogs(prisma, cmgDb) {
  logger.info('[CONVERSION LOGS] Starting historic CMG conversion logs population...');
  
  // Get the configured logs directory
  const config = require('config');
  const logsDir = config.get('cmg.legacyConversionsLogsDir');
  
  if (!logsDir) {
    logger.error('[CONVERSION LOGS] CMG_LEGACY_CONVERSIONS_LOGS_DIR not configured');
    logger.error('[CONVERSION LOGS] Set environment variable: CMG_LEGACY_CONVERSIONS_LOGS_DIR=/path/to/logs');
    throw new Error('CMG_LEGACY_CONVERSIONS_LOGS_DIR not configured');
  }
  
  logger.info(`[CONVERSION LOGS] Using logs directory: ${logsDir}`);
  
  // Check if directory exists
  try {
    await fs.access(logsDir);
  } catch (error) {
    logger.error(`[CONVERSION LOGS] Logs directory not accessible: ${logsDir}`);
    logger.info('[CONVERSION LOGS] This is expected in non-production environments.');
    logger.info('[CONVERSION LOGS] Skipping conversion logs population.');
    return {
      datasetsProcessed: 0,
      conversionsUpdated: 0,
      workerProcessesCreated: 0,
      logEntriesCreated: 0,
      missingLogFiles: [],
      errors: [],
    };
  }
  
  // Statistics
  const stats = {
    datasetsProcessed: 0,
    conversionsUpdated: 0,
    workerProcessesCreated: 0,
    logEntriesCreated: 0,
    missingLogFiles: [],
    errors: [],
  };
  
  // Get all conversions with CMG IDs, grouped by dataset
  const conversions = await prisma.conversion.findMany({
    where: {
      cmg_id: { not: null },
      dataset_id: { not: null },
    },
    include: {
      dataset: {
        select: {
          id: true,
          name: true,
        },
      },
    },
    orderBy: {
      dataset_id: 'asc',
    },
  });
  
  if (conversions.length === 0) {
    logger.warn('[CONVERSION LOGS] No conversions with CMG IDs found');
    return stats;
  }
  
  logger.info(`[CONVERSION LOGS] Found ${conversions.length} conversions to process`);
  
  // Group conversions by dataset
  const conversionsByDataset = new Map();
  for (const conversion of conversions) {
    const datasetId = conversion.dataset_id;
    if (!conversionsByDataset.has(datasetId)) {
      conversionsByDataset.set(datasetId, []);
    }
    conversionsByDataset.get(datasetId).push(conversion);
  }
  
  logger.info(`[CONVERSION LOGS] Processing logs for ${conversionsByDataset.size} datasets`);
  
  // Process each dataset's conversions
  let processedCount = 0;
  for (const [datasetId, datasetConversions] of conversionsByDataset) {
    processedCount += 1;
    const datasetName = datasetConversions[0].dataset?.name || `dataset-${datasetId}`;
    const logFileName = `convert_${datasetName}.log`;
    const logFilePath = path.join(logsDir, logFileName);
    
    logger.info(`[CONVERSION LOGS] [${processedCount}/${conversionsByDataset.size}] Processing ${datasetConversions.length} conversion(s) for dataset: ${datasetName}`);
    
    await processDatasetConversionLogs(prisma, datasetConversions, logFilePath, stats);
  }
  
  // Summary
  logger.info('');
  logger.info('='.repeat(80));
  logger.info('[CONVERSION LOGS] Historic conversion logs population complete');
  logger.info('='.repeat(80));
  logger.info(`Datasets processed: ${stats.datasetsProcessed}`);
  logger.info(`Conversions updated: ${stats.conversionsUpdated}`);
  logger.info(`Worker processes created: ${stats.workerProcessesCreated}`);
  logger.info(`Log entries created: ${stats.logEntriesCreated}`);
  
  if (stats.missingLogFiles.length > 0) {
    logger.warn(`Missing log files: ${stats.missingLogFiles.length}`);
    if (stats.missingLogFiles.length <= 10) {
      stats.missingLogFiles.forEach(file => logger.warn(`  - ${file}`));
    } else {
      logger.warn(`  (First 10 of ${stats.missingLogFiles.length}):`);
      stats.missingLogFiles.slice(0, 10).forEach(file => logger.warn(`  - ${file}`));
    }
  }
  
  if (stats.errors.length > 0) {
    logger.error(`Errors encountered: ${stats.errors.length}`);
    if (stats.errors.length <= 10) {
      stats.errors.forEach(err => logger.error(`  - ${JSON.stringify(err)}`));
    } else {
      logger.error(`  (First 10 of ${stats.errors.length}):`);
      stats.errors.slice(0, 10).forEach(err => logger.error(`  - ${JSON.stringify(err)}`));
    }
  }
  
  logger.info('='.repeat(80));
  logger.info('');
  
  return stats;
}

module.exports = {
  syncConversionLogs,
  syncAllConversionLogs: syncConversionLogs, // Alias for consistency
};
