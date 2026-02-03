const { ObjectId } = require('mongodb');
const { handleDuplicateName } = require('../utils/duplicate_handler');
const { extractGenomicAttributes } = require('../utils/cmg_helpers');
const logger = require('../../logger');

const DUPLICATE_PREFIX = 'DUPLICATE';
const UNKNOWN_PREFIX = 'UNKNOWN';

/**
 * Convert all datasets from CMG to Bioloop
 */
async function syncAllDatasets(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting datasets...');
  
  // Convert RAW_DATA (from CMG datasets collection)
  await convertCMGDatasets(prisma, cmgDb, 'RAW_DATA');
  
  // Convert DATA_PRODUCT (from CMG dataproducts collection)
  await convertCMGDatasets(prisma, cmgDb, 'DATA_PRODUCT');
  
  logger.info('[BIGBANG] Dataset conversion complete');
}

/**
 * Process a batch of items: group by name, handle duplicates, and insert
 * This keeps memory usage low by processing small batches at a time
 */
async function processBatch(prisma, batchItems, datasetType) {
  // Group items in this batch by name
  const nameGroups = {};
  
  for (const item of batchItems) {
    const name = item.name;
    if (!nameGroups[name]) {
      nameGroups[name] = [];
    }
    nameGroups[name].push(item);
  }
  
  // Insert each group (always check DB for existing names, even if only 1 in batch)
  for (const [originalName, items] of Object.entries(nameGroups)) {
    for (const item of items) {
      // Always check if name exists in DB (handles cross-batch duplicates)
      const existingDataset = await prisma.dataset.findFirst({
        where: {
          name: originalName,
          type: datasetType,
          is_deleted: false,
        },
      });
      
      let finalName;
      if (existingDataset || items.length > 1) {
        // Name exists in DB OR multiple items in this batch - get unique name
        finalName = await handleDuplicateName(prisma, originalName, datasetType, false);
      } else {
        // Name doesn't exist, safe to use original
        finalName = originalName;
      }
      
      await insertDataset(prisma, item, datasetType, finalName, false);
    }
  }
}

/**
 * Convert datasets from a specific CMG collection
 */
async function convertCMGDatasets(prisma, cmgDb, datasetType) {
  const collectionName = datasetType === 'DATA_PRODUCT' ? 'dataproducts' : 'datasets';
  const collection = cmgDb.collection(collectionName);
  
  logger.info(`[BIGBANG] Processing CMG collection: ${collectionName} (type: ${datasetType})`);
  
  // Get total count for progress tracking
  const totalCount = await collection.countDocuments({});
  logger.info(`[BIGBANG] Found ${totalCount} items to process`);
  
  // Process in small batches and insert immediately to avoid memory issues
  let unknownCount = 0;
  let processedCount = 0;
  const BATCH_SIZE = 50;  // Small batch size to avoid memory buildup
  let currentBatch = [];
  
  // Use cursor to stream data
  const cursor = collection.find({}).batchSize(BATCH_SIZE);
  
  for await (const cmgItem of cursor) {
    // Handle missing names
    if (!cmgItem.name) {
      unknownCount++;
      const assignedName = unknownCount > 1 ? `${UNKNOWN_PREFIX}-${unknownCount}` : UNKNOWN_PREFIX;
      logger.warn(`[BIGBANG] No name field for CMG ${datasetType} ${cmgItem._id}, assigned: ${assignedName}`);
      cmgItem.name = assignedName;
    }
    
    currentBatch.push(cmgItem);
    
    // Process and insert batch when it reaches size limit
    if (currentBatch.length >= BATCH_SIZE) {
      await processBatch(prisma, currentBatch, datasetType);
      processedCount += currentBatch.length;
      
      // Clear batch from memory immediately
      currentBatch = [];
      
      // Progress logging
      if (processedCount % 500 === 0) {
        logger.info(`[BIGBANG] Processed ${processedCount}/${totalCount} items (${Math.round(processedCount / totalCount * 100)}%)`);
      }
      
      // Force garbage collection hint
      if (global.gc && processedCount % 1000 === 0) {
        global.gc();
      }
    }
  }
  
  // Process remaining items
  if (currentBatch.length > 0) {
    await processBatch(prisma, currentBatch, datasetType);
    processedCount += currentBatch.length;
    currentBatch = [];
  }
  
  logger.info(`[BIGBANG] Completed processing ${processedCount} ${datasetType} items`);
}

/**
 * Insert a single dataset into Bioloop
 */
async function insertDataset(prisma, cmgItem, datasetType, name, isDeleted) {
  if (!name) {
    throw new Error('Dataset name must be specified');
  }
  if (!datasetType) {
    throw new Error('Dataset type must be specified');
  }
  if (isDeleted === null || isDeleted === undefined) {
    throw new Error('Deletion status must be specified');
  }
  
  const currentTimestamp = new Date();
  
  // Handle missing timestamps for UNKNOWN datasets
  let createdAt, updatedAt;
  if (name.startsWith(UNKNOWN_PREFIX)) {
    createdAt = cmgItem.createdAt || currentTimestamp;
    updatedAt = cmgItem.updatedAt || currentTimestamp;
  } else {
    createdAt = cmgItem.createdAt || null;
    updatedAt = cmgItem.updatedAt || null;
  }
  
  // Look up analysis_type for DATA_PRODUCT datasets
  // Read from CMG's file_type field and connect to analysis_type table
  let analysisTypeConnect = null;
  if (datasetType === 'DATA_PRODUCT' && cmgItem.file_type) {
    // Normalize file_type from CMG (uppercase with underscores)
    const normalizedName = cmgItem.file_type
      .trim()
      .toUpperCase()
      .replace(/\s+/g, '_')
      .replace(/[^A-Z0-9_-]/g, '');
    
    // Look up analysis_type in database (case-insensitive)
    const analysisType = await prisma.analysis_type.findFirst({
      where: {
        name: { equals: normalizedName, mode: 'insensitive' },
      },
    });
    
    if (analysisType) {
      analysisTypeConnect = { connect: { id: analysisType.id } };
    } else {
      // Log warning if file_type from CMG doesn't match any seeded analysis type
      logger.warn(`[BIGBANG] No analysis_type found for CMG file_type: "${cmgItem.file_type}" (normalized: "${normalizedName}") on dataset ${cmgItem._id}`);
    }
  }
  
  // Insert dataset
  const dataset = await prisma.dataset.create({
    data: {
      name: name,
      type: datasetType,
      is_deleted: isDeleted,
      cmg_id: cmgItem._id.toString(),
      description: cmgItem.description || null,
      num_directories: cmgItem.directories || 0,
      num_files: datasetType === 'RAW_DATA' ? (cmgItem.files || 0) : 0,
      du_size: datasetType === 'RAW_DATA' ? BigInt(cmgItem.du_size || 0) : BigInt(0),
      // For DATA_PRODUCT: use 'size' field from CMG dataproducts collection
      // For RAW_DATA: use 'size' field from CMG datasets collection
      size: cmgItem.size ? BigInt(cmgItem.size) : null,
      created_at: createdAt,
      updated_at: updatedAt,
      origin_path: cmgItem.paths?.origin || null,
      archive_path: cmgItem.paths?.archive || null,
      staged_path: null, // Always null - staging state not migrated from CMG
      is_staged: false, // Always false - staging state not migrated from CMG
      metadata: null, // No metadata needed - analysis_type now via relation
      // Connect to analysis_type via foreign key
      ...(analysisTypeConnect && { analysis_type: analysisTypeConnect }),
    },
  });
  
  // Insert genomic details if present
  // CMG has inconsistent field names (genomeType vs genome_type, genomeValue vs genome_value vs genome)
  // Use utility function to handle all variations
  const { genome_type, genome_value } = extractGenomicAttributes(cmgItem);
  
  if (genome_type || genome_value) {
    await prisma.dataset_genomic_attributes.create({
      data: {
        dataset_id: dataset.id,
        genome_type,
        genome_value,
      },
    });
  }
  
  return dataset;
}

module.exports = {
  syncAllDatasets,
  convertCMGDatasets,
  insertDataset,
};

