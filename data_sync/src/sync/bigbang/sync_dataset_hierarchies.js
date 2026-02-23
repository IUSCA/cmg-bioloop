const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Convert dataset hierarchies from CMG to Bioloop
 * Creates links between RAW_DATA (source) and DATA_PRODUCT (derived)
 * 
 * Links are created by looking at dataproduct.dataset field which references
 * the source raw dataset that was used to create the derived data product.
 * 
 * Derivation method is determined by checking CMG dataproduct fields:
 * - If dataproduct.conversion exists → derivation_method: 'conversion'
 * - Otherwise → derivation_method: 'manual_assignment'
 */
async function syncDatasetHierarchies(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting dataset hierarchies...');
  
  const cmgDataProducts = await cmgDb.collection('dataproducts').find({}).toArray();
  logger.info(`[BIGBANG] Found ${cmgDataProducts.length} CMG dataproducts to process`);
  
  let createdCount = 0;
  let skippedCount = 0;
  const skipReasons = {};  // Track reasons for skipping
  const derivationMethodCounts = { conversion: 0, manual_assignment: 0 };  // Track derivation methods
  
  for (const cmgDataProduct of cmgDataProducts) {
    // Find the corresponding Bioloop DATA_PRODUCT
    const bioloopDataProduct = await prisma.dataset.findFirst({
      where: { cmg_id: cmgDataProduct._id.toString() },
    });
    
    if (!bioloopDataProduct) {
      if (skippedCount < 5) {
        logger.warn(`[BIGBANG] Bioloop DATA_PRODUCT not found for CMG ID: ${cmgDataProduct._id}`);
        logger.warn(`  This dataproduct may not have been migrated in step 5`);
      }
      skipReasons['dataproduct_not_in_bioloop'] = (skipReasons['dataproduct_not_in_bioloop'] || 0) + 1;
      skippedCount++;
      continue;
    }
    
    // Get the source dataset ID from CMG dataproduct
    const cmgSourceDatasetId = cmgDataProduct.dataset;
    
    if (!cmgSourceDatasetId) {
      // Dataproduct has no source dataset reference
      logger.debug(`[BIGBANG] Dataproduct ${cmgDataProduct._id} has no source dataset reference`);
      skipReasons['no_source_dataset_reference'] = (skipReasons['no_source_dataset_reference'] || 0) + 1;
      skippedCount++;
      continue;
    }
    
    // Find the corresponding source dataset in CMG
    const cmgSourceDataset = await cmgDb.collection('datasets').findOne({
      _id: new ObjectId(cmgSourceDatasetId),
    });
    
    if (!cmgSourceDataset) {
      if (skippedCount < 5) {
        logger.warn(`[BIGBANG] CMG source dataset not found: ${cmgSourceDatasetId}`);
        logger.warn(`  Referenced by dataproduct: ${cmgDataProduct._id} (${cmgDataProduct.name})`);
      }
      skipReasons['source_dataset_not_in_cmg'] = (skipReasons['source_dataset_not_in_cmg'] || 0) + 1;
      skippedCount++;
      continue;
    }
    
    // Find the corresponding Bioloop RAW_DATA
    const bioloopRawData = await prisma.dataset.findFirst({
      where: { cmg_id: cmgSourceDataset._id.toString() },
    });
    
    if (!bioloopRawData) {
      if (skippedCount < 5) {
        logger.warn(`[BIGBANG] Bioloop RAW_DATA not found for CMG ID: ${cmgSourceDataset._id}`);
        logger.warn(`  Source for dataproduct: ${cmgDataProduct.name}`);
      }
      skipReasons['source_dataset_not_in_bioloop'] = (skipReasons['source_dataset_not_in_bioloop'] || 0) + 1;
      skippedCount++;
      continue;
    }
    
    // Check if hierarchy relationship already exists (for idempotency)
    const existingHierarchy = await prisma.dataset_hierarchy.findFirst({
      where: {
        source_id: bioloopRawData.id,
        derived_id: bioloopDataProduct.id,
      },
    });
    
    if (existingHierarchy) {
      logger.debug(`[BIGBANG] Hierarchy already exists: source=${bioloopRawData.id}, derived=${bioloopDataProduct.id}`);
      skipReasons['already_exists_idempotency'] = (skipReasons['already_exists_idempotency'] || 0) + 1;
      skippedCount++;
      continue;
    }
    
    // Determine derivation method based on CMG dataproduct fields
    // - If dataproduct.conversion exists → created by conversion pipeline
    // - Otherwise → manually assigned/uploaded by user
    const derivationMethod = cmgDataProduct.conversion ? 'conversion' : 'manual_assignment';
    derivationMethodCounts[derivationMethod]++;
    
    // Insert the hierarchy relationship with derivation method metadata
    await prisma.dataset_hierarchy.create({
      data: {
        source_id: bioloopRawData.id,
        derived_id: bioloopDataProduct.id,
        metadata: {
          derivation_method: derivationMethod,
        },
      },
    });
    
    createdCount++;
    logger.debug(`[BIGBANG] Created hierarchy: ${bioloopRawData.name} (${bioloopRawData.type}) → ${bioloopDataProduct.name} (${bioloopDataProduct.type}), method=${derivationMethod}`);
  }
  
  // Log detailed summary
  logger.info(`[BIGBANG] Dataset hierarchy conversion complete: ${createdCount} created, ${skippedCount} skipped`);
  logger.info(`[BIGBANG] Derivation methods: ${derivationMethodCounts.conversion} via conversion, ${derivationMethodCounts.manual_assignment} via manual assignment/upload`);
  
  // Show breakdown of skip reasons
  if (Object.keys(skipReasons).length > 0) {
    logger.info('[BIGBANG] Skip reasons breakdown:');
    Object.entries(skipReasons).forEach(([reason, count]) => {
      logger.info(`  - ${reason}: ${count} records`);
    });
  }
  
  // Provide context for common issues
  if (skipReasons['source_dataset_not_in_bioloop']) {
    logger.warn('[BIGBANG] Some source datasets were not found in Bioloop.');
    logger.warn('  This could mean they failed to migrate in step 5, or were filtered out.');
  }
  
  if (skipReasons['already_exists_idempotency']) {
    logger.info('[BIGBANG] Many hierarchies already existed (idempotency working correctly).');
  }
}

module.exports = {
  syncDatasetHierarchies,
};

