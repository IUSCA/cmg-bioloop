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
  // Check if conversion already exists (idempotency)
  const existingConversion = await prisma.conversion.findFirst({
    where: { cmg_id: cmgConversion._id.toString() },
  });
  
  if (existingConversion) {
    // Already migrated, skip
    return existingConversion;
  }
  
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
  // Parse CMG's options field (array of strings) into Bioloop's additional_args format
  // CMG format: ['--no-lane-splitting', '--barcode-mismatches', '1']
  // Bioloop format: [{argument_name: '--no-lane-splitting', value: 'true'}, {argument_name: '--barcode-mismatches', value: '1'}]
  //
  // Note: CMG metadata (name, status, output_directory, log_file_path, samplesheet) is intentionally NOT stored
  // in additional_args as Bioloop uses this field strictly for command-line arguments.
  // This metadata can be queried from CMG MongoDB if needed using cmg_id.
  
  let additionalArgs = null;
  
  if (cmgConversion.options && Array.isArray(cmgConversion.options) && cmgConversion.options.length > 0) {
    const parsedArgs = [];
    
    for (let i = 0; i < cmgConversion.options.length; i++) {
      const option = cmgConversion.options[i];
      
      // Skip empty strings
      if (!option || option.trim() === '') {
        continue;
      }
      
      // Check if this is a flag (starts with -- or -)
      if (option.startsWith('--') || option.startsWith('-')) {
        // Check if the flag has an inline value (--key=value)
        if (option.includes('=')) {
          const [name, value] = option.split('=', 2);
          parsedArgs.push({
            argument_name: name,
            value: value || 'true',
          });
        } else {
          // Check if next item is a value (doesn't start with --)
          const nextItem = cmgConversion.options[i + 1];
          if (nextItem && !nextItem.startsWith('--') && !nextItem.startsWith('-')) {
            // This flag has a value in the next position
            parsedArgs.push({
              argument_name: option,
              value: nextItem,
            });
            i++; // Skip the next item since we consumed it
          } else {
            // This is a boolean flag
            parsedArgs.push({
              argument_name: option,
              value: 'true',
            });
          }
        }
      } else {
        // This is a standalone value (shouldn't happen in well-formed args, but handle it)
        logger.warn(`[BIGBANG] Unexpected standalone value in conversion ${cmgConversion._id} options: ${option}`);
      }
    }
    
    // Store as additional_args if we parsed any arguments
    if (parsedArgs.length > 0) {
      additionalArgs = parsedArgs;
      logger.debug(`[BIGBANG] Parsed ${parsedArgs.length} argument(s) from CMG conversion ${cmgConversion._id}`);
    }
  }
  
  const conversion = await prisma.conversion.create({
    data: {
      definition_id: conversionDefinition.id,
      initiator_id: initiatorId,
      dataset_id: sourceDatasetId, // Source dataset for the conversion
      workflow_id: null, // CMG doesn't use workflow IDs like Bioloop
      cmg_id: cmgConversion._id.toString(),
      initiated_at: cmgConversion.createdAt || new Date(),
      additional_args: additionalArgs,
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
      // Check if link already exists (idempotency)
      const existingLink = await prisma.conversion_derived_dataset.findUnique({
        where: {
          conversion_id_dataset_id: {
            conversion_id: bioloopConversionId,
            dataset_id: bioloopDataset.id,
          },
        },
      });
      
      if (!existingLink) {
        await prisma.conversion_derived_dataset.create({
          data: {
            conversion_id: bioloopConversionId,
            dataset_id: bioloopDataset.id,
          },
        });
      }
    }
  }
}

/**
 * Map CMG conversion status to Bioloop status
 * Note: This function is kept for reference but not currently used in direct field mapping.
 * CMG status is stored in additional_args.cmg_status for historical tracking.
 * Bioloop conversion status is tracked via workflow system separately.
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

