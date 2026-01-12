const { ObjectId } = require('mongodb');
const { handleDuplicateName } = require('../utils/duplicate_handler');
const logger = require('../logger');

const DUPLICATE_PREFIX = 'DUPLICATE';
const UNKNOWN_PREFIX = 'UNKNOWN';

/**
 * Convert all datasets from CMG to Bioloop
 * Equivalent to: db_conversion/src/convert/entity/dataset.py::convert_all_datasets()
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
 * Convert datasets from a specific CMG collection
 * Equivalent to: db_conversion/src/convert/entity/dataset.py::convert_cmg_datasets()
 */
async function convertCMGDatasets(prisma, cmgDb, datasetType) {
  const collectionName = datasetType === 'DATA_PRODUCT' ? 'dataproducts' : 'datasets';
  const collection = cmgDb.collection(collectionName);
  
  logger.info(`[BIGBANG] Processing CMG collection: ${collectionName} (type: ${datasetType})`);
  
  // Group CMG datasets by name
  const nameGroups = {};
  let unknownCount = 0;
  
  const cmgItems = await collection.find({}).toArray();
  
  for (const cmgItem of cmgItems) {
    // Handle missing names
    if (!cmgItem.name) {
      unknownCount++;
      const assignedName = unknownCount > 1 ? `${UNKNOWN_PREFIX}-${unknownCount}` : UNKNOWN_PREFIX;
      logger.warn(`[BIGBANG] No name field for CMG ${datasetType} ${cmgItem._id}, assigned: ${assignedName}`);
      cmgItem.name = assignedName;
    }
    
    const name = cmgItem.name;
    if (!nameGroups[name]) {
      nameGroups[name] = [];
    }
    nameGroups[name].push(cmgItem);
  }
  
  logger.info(`[BIGBANG] Found ${Object.keys(nameGroups).length} unique ${datasetType} names`);
  
  // Process each name group
  for (const [originalName, items] of Object.entries(nameGroups)) {
    if (items.length === 1) {
      // No duplicates, use original name
      await insertDataset(prisma, items[0], datasetType, originalName, false);
    } else {
      // Handle duplicates - add DUPLICATE prefix
      for (const item of items) {
        const newName = await handleDuplicateName(prisma, originalName, datasetType, false);
        await insertDataset(prisma, item, datasetType, newName, false);
      }
    }
  }
}

/**
 * Insert a single dataset into Bioloop
 * Equivalent to: db_conversion/src/convert/entity/dataset.py::insert_dataset()
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
      size: BigInt(cmgItem.size || 0),
      created_at: createdAt,
      updated_at: updatedAt,
      origin_path: cmgItem.paths?.origin || null,
      archive_path: cmgItem.paths?.archive || null,
      staged_path: cmgItem.paths?.staged || null,
      is_staged: cmgItem.staged || false,
      metadata: null,
    },
  });
  
  // Insert genomic details if present
  const genomeType = cmgItem.genome_type || null;
  const genomeValue = cmgItem.genome || null;
  
  if (genomeType || genomeValue) {
    await prisma.dataset_genomic_attributes.create({
      data: {
        dataset_id: dataset.id,
        genome_type: genomeType,
        genome_value: genomeValue,
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

