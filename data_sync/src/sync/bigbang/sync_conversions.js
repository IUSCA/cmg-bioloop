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
 * Parse CMG options array into argument name-value pairs
 * 
 * CMG formats:
 * 1. Boolean: "no-lane-splitting"
 * 2. Key-value in same string: "barcode-mismatches 1" (space-separated)
 * 3. Key-value in separate items: ["key", "value"]
 * 4. Key=value: "barcode-mismatches=1"
 * 
 * @param {string[]} options - Array of CMG option strings
 * @returns {Object} { parsedArgs: [{argument_name, value}], skipped: number }
 */
function parseOptionsArray(options) {
  const parsedArgs = [];
  let skipped = 0;
  
  for (let i = 0; i < options.length; i++) {
    const option = options[i];
    
    // Skip empty strings
    if (!option || option.trim() === '') {
      skipped++;
      continue;
    }
    
    let argumentName = option;
    let argumentValue = 'true';
    
    // Check if already has prefix (some might)
    if (option.startsWith('--') || option.startsWith('-')) {
      // Already has prefix, parse normally
      if (option.includes('=')) {
        const [name, value] = option.split('=', 2);
        argumentName = name;
        argumentValue = value || 'true';
      } else if (option.includes(' ')) {
        // Space-separated key-value: "--key value"
        const [name, ...valueParts] = option.split(' ');
        argumentName = name;
        argumentValue = valueParts.join(' ');
      } else {
        // Check if next item is a value
        const nextItem = options[i + 1];
        if (nextItem && !nextItem.startsWith('--') && !nextItem.startsWith('-') && nextItem.trim().toLowerCase() !== 'wildcards') {
          argumentName = option;
          argumentValue = nextItem;
          i++; // Skip the next item since we consumed it
        } else {
          argumentName = option;
          argumentValue = 'true';
        }
      }
    } else {
      // No prefix - this is the CMG format
      // Add -- prefix for Bioloop compatibility
      
      if (option.includes('=')) {
        // Handle key=value format: "barcode-mismatches=1"
        const [name, value] = option.split('=', 2);
        argumentName = `--${name}`;
        argumentValue = value || 'true';
      } else if (option.includes(' ')) {
        // CRITICAL: CMG stores key-value in SAME string: "barcode-mismatches 1"
        const spaceIndex = option.indexOf(' ');
        const name = option.substring(0, spaceIndex);
        const value = option.substring(spaceIndex + 1).trim();
        argumentName = `--${name}`;
        argumentValue = value || 'true';
      } else {
        // Check if next item exists and looks like a value
        const nextItem = options[i + 1];
        
        // If next item exists and doesn't contain hyphens and isn't "wildcards" (likely a value, not another option)
        if (nextItem && nextItem.trim() && !nextItem.includes('-') && nextItem.trim().toLowerCase() !== 'wildcards') {
          argumentName = `--${option}`;
          argumentValue = nextItem;
          i++; // Consume the value
        } else {
          // This is a boolean flag
          argumentName = `--${option}`;
          argumentValue = 'true';
        }
      }
    }
    
    parsedArgs.push({
      argument_name: argumentName,
      value: argumentValue,
    });
  }
  
  return { parsedArgs, skipped };
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
  
  // Parse CMG conversion arguments
  // CMG stores arguments in multiple places:
  // 1. options[] array - command line arguments (predefined + additional)
  // 2. samplesheet string - CSV content for sample sheet
  //
  // CMG options array format:
  // ["no-lane-splitting", "barcode-mismatches 1", "wildcards", "--custom-flag", "value"]
  //                                                   ↑
  //                                        Separator between predefined and additional args
  //
  // Bioloop storage:
  // - Predefined args (before "wildcards") → argument_values table (linked to argument definitions)
  // - Additional args (after "wildcards") → additional_args JSON field
  // - Sample sheet → argument_values table (linked to --sample-sheet argument)
  //
  // This matches current Bioloop behavior where genomic_conversion workflow creates argument_values.
  
  // Get all program arguments for this conversion definition
  const programArguments = await prisma.argument.findMany({
    where: {
      program_id: conversionDefinition.program_id,
    },
  });
  
  // Build lookup map: argument name → argument definition
  const argumentLookup = {};
  programArguments.forEach(arg => {
    argumentLookup[arg.name] = arg;
  });
  
  const argumentValuesToCreate = []; // Will become argument_values records
  let additionalArgs = null; // Will become additional_args JSON
  
  // Parse CMG options array - split at "wildcards" separator
  if (cmgConversion.options && Array.isArray(cmgConversion.options) && cmgConversion.options.length > 0) {
    // Find index of "wildcards" separator
    const wildcardsIndex = cmgConversion.options.findIndex(opt => 
      opt && opt.trim().toLowerCase() === 'wildcards'
    );
    
    // Split options into predefined (before wildcards) and additional (after wildcards)
    const predefinedOptions = wildcardsIndex >= 0 
      ? cmgConversion.options.slice(0, wildcardsIndex)
      : cmgConversion.options; // If no wildcards, all are predefined
      
    const additionalOptions = wildcardsIndex >= 0 
      ? cmgConversion.options.slice(wildcardsIndex + 1)
      : [];
    
    // Parse predefined arguments → argument_values
    const { parsedArgs: predefinedParsed } = parseOptionsArray(predefinedOptions);
    
    for (const { argument_name, value } of predefinedParsed) {
      // Look up the argument definition
      const argDef = argumentLookup[argument_name];
      
      if (argDef) {
        // Validate against allowed_values if defined
        if (argDef.allowed_values && Array.isArray(argDef.allowed_values) && argDef.allowed_values.length > 0) {
          if (!argDef.allowed_values.includes(value)) {
            logger.warn(
              `[BIGBANG] ⚠️  INVALID ARGUMENT VALUE from CMG:\n` +
              `  Conversion CMG ID: ${cmgConversion._id}\n` +
              `  Dataset CMG ID: ${cmgConversion.dataset ? cmgConversion.dataset.toString() : 'N/A'}\n` +
              `  Pipeline: ${cmgConversion.pipeline}\n` +
              `  Argument: ${argument_name}\n` +
              `  Value: "${value}" (NOT in allowed range)\n` +
              `  Allowed values: [${argDef.allowed_values.join(', ')}]\n` +
              `  → This value will be migrated as-is but may fail validation in Bioloop UI/API`
            );
          }
        }
        
        // This is a known argument - create argument_value record
        argumentValuesToCreate.push({
          argument_id: argDef.id,
          value: value,
        });
        logger.debug(`[BIGBANG] Matched predefined arg: ${argument_name} = ${value}`);
      } else {
        // Unknown argument - treat as additional arg
        logger.warn(`[BIGBANG] Unknown argument '${argument_name}' for program ${conversionDefinition.program_id}, treating as additional arg`);
        if (!additionalArgs) additionalArgs = [];
        additionalArgs.push({ argument_name, value });
      }
    }
    
    // Parse additional arguments → additional_args JSON
    if (additionalOptions.length > 0) {
      const { parsedArgs: additionalParsed } = parseOptionsArray(additionalOptions);
      
      if (additionalParsed.length > 0) {
        additionalArgs = additionalParsed;
        logger.debug(`[BIGBANG] Parsed ${additionalParsed.length} additional argument(s)`);
      }
    }
  }
  
  // Handle sample sheet from CMG → argument_values for --sample-sheet
  if (cmgConversion.samplesheet && typeof cmgConversion.samplesheet === 'string') {
    const sampleSheetArg = argumentLookup['--sample-sheet'];
    
    if (sampleSheetArg) {
      argumentValuesToCreate.push({
        argument_id: sampleSheetArg.id,
        value: cmgConversion.samplesheet, // Store CSV content
      });
      logger.debug(`[BIGBANG] Added sample sheet (${cmgConversion.samplesheet.length} chars)`);
    } else {
      logger.warn(`[BIGBANG] No --sample-sheet argument definition found for program ${conversionDefinition.program_id}`);
    }
  }
  
  // Create conversion with argument_values and additional_args
  const conversion = await prisma.conversion.create({
    data: {
      definition_id: conversionDefinition.id,
      initiator_id: initiatorId,
      dataset_id: sourceDatasetId, // Source dataset for the conversion
      workflow_id: null, // CMG doesn't use workflow IDs like Bioloop
      cmg_id: cmgConversion._id.toString(),
      initiated_at: cmgConversion.createdAt || new Date(),
      additional_args: additionalArgs,
      argument_values: {
        create: argumentValuesToCreate, // Create linked argument_value records
      },
    },
  });
  
  logger.debug(`[BIGBANG] Created conversion ${conversion.id}: ${argumentValuesToCreate.length} argument_values, ${additionalArgs ? additionalArgs.length : 0} additional_args`);
  
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

