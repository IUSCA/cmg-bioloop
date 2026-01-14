const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Convert conversions from CMG to Bioloop
 */
async function syncConversions(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting conversions...');
  
  const cmgConversions = await cmgDb.collection('conversions').find({}).toArray();
  logger.info(`[BIGBANG] Found ${cmgConversions.length} CMG conversions to convert`);
  
  let convertedCount = 0;
  let skippedCount = 0;
  
  for (const cmgConversion of cmgConversions) {
    try {
      await convertConversion(prisma, cmgDb, cmgConversion);
      convertedCount++;
    } catch (error) {
      // Log the error but continue - some conversions might reference missing data
      logger.warn(`[BIGBANG] Failed to convert conversion ${cmgConversion._id}: ${error.message}`);
      skippedCount++;
    }
  }
  
  logger.info(`[BIGBANG] Conversion complete: ${convertedCount} succeeded, ${skippedCount} skipped`);
}

/**
 * Convert a single conversion
 * Equivalent to Python's conversion conversion logic
 */
async function convertConversion(prisma, cmgDb, cmgConversion) {
  // Get conversion definition ID
  const pipelineName = cmgConversion.pipeline;
  if (!pipelineName) {
    throw new Error('Pipeline name is required');
  }
  
  const conversionDefinition = await prisma.conversion_definition.findUnique({
    where: { name: pipelineName },
  });
  
  if (!conversionDefinition) {
    throw new Error(`Conversion definition not found for pipeline: ${pipelineName}`);
  }
  
  // Find initiator user
  let initiatorId = null;
  if (cmgConversion.user) {
    const initiator = await prisma.user.findFirst({
      where: { cmg_id: cmgConversion.user.toString() },
    });
    
    if (initiator) {
      initiatorId = initiator.id;
    }
  }
  
  // Find source dataset
  let sourceDatasetId = null;
  if (cmgConversion.dataset) {
    const sourceDataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgConversion.dataset.toString() },
    });
    
    if (sourceDataset) {
      sourceDatasetId = sourceDataset.id;
    }
  }
  
  // Create conversion
  const conversion = await prisma.conversion.create({
    data: {
      name: cmgConversion.name || `Conversion ${cmgConversion._id}`,
      status: mapCMGStatusToBioloop(cmgConversion.status),
      output_directory: cmgConversion.output_directory || null,
      log_file_path: cmgConversion.log_file_path || null,
      definition_id: conversionDefinition.id,
      initiator_id: initiatorId,
      source_dataset_id: sourceDatasetId,
      cmg_id: cmgConversion._id.toString(),
      created_at: cmgConversion.createdAt || new Date(),
      updated_at: cmgConversion.updatedAt || new Date(),
    },
  });
  
  // Link derived datasets (dataproducts)
  await linkDerivedDatasets(prisma, cmgDb, conversion.id, cmgConversion._id.toString());
  
  return conversion;
}

/**
 * Link derived datasets to a conversion
 */
async function linkDerivedDatasets(prisma, cmgDb, bioloopConversionId, cmgConversionId) {
  // Find all dataproducts that have this conversion
  const dataproducts = await cmgDb.collection('dataproducts').find({
    conversion: new ObjectId(cmgConversionId),
  }).toArray();
  
  for (const dataproduct of dataproducts) {
    const bioloopDataset = await prisma.dataset.findFirst({
      where: { cmg_id: dataproduct._id.toString() },
    });
    
    if (bioloopDataset) {
      await prisma.conversion_derived_dataset.create({
        data: {
          conversion_id: bioloopConversionId,
          dataset_id: bioloopDataset.id,
        },
      });
    }
  }
}

/**
 * Map CMG conversion status to Bioloop status
 */
function mapCMGStatusToBioloop(cmgStatus) {
  const statusMap = {
    'pending': 'PENDING',
    'running': 'RUNNING',
    'completed': 'COMPLETED',
    'failed': 'FAILED',
    'cancelled': 'CANCELLED',
  };
  
  return statusMap[cmgStatus] || 'PENDING';
}

module.exports = {
  syncConversions,
};

