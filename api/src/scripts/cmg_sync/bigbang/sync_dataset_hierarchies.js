const { ObjectId } = require('mongodb');
const logger = require('@/services/logger');

/**
 * Convert dataset hierarchies from CMG to Bioloop
 * Creates links between RAW_DATA (source) and DATA_PRODUCT (derived)
 * Equivalent to: db_conversion/src/convert/entity/dataset_hierarchy.py::convert_dataset_hierarchies()
 */
async function syncDatasetHierarchies(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting dataset hierarchies...');
  
  const cmgDataProducts = await cmgDb.collection('dataproducts').find({}).toArray();
  let createdCount = 0;
  let skippedCount = 0;
  
  for (const cmgDataProduct of cmgDataProducts) {
    // Find the corresponding Bioloop DATA_PRODUCT
    const bioloopDataProduct = await prisma.dataset.findUnique({
      where: { cmg_id: cmgDataProduct._id.toString() },
    });
    
    if (!bioloopDataProduct) {
      logger.warn(`[BIGBANG] Bioloop DATA_PRODUCT not found for CMG ID: ${cmgDataProduct._id}`);
      skippedCount++;
      continue;
    }
    
    // Get the source dataset ID from CMG dataproduct
    const cmgSourceDatasetId = cmgDataProduct.dataset;
    
    if (!cmgSourceDatasetId) {
      skippedCount++;
      continue;
    }
    
    // Find the corresponding source dataset in CMG
    const cmgSourceDataset = await cmgDb.collection('datasets').findOne({
      _id: new ObjectId(cmgSourceDatasetId),
    });
    
    if (!cmgSourceDataset) {
      logger.warn(`[BIGBANG] CMG source dataset not found: ${cmgSourceDatasetId}`);
      skippedCount++;
      continue;
    }
    
    // Find the corresponding Bioloop RAW_DATA
    const bioloopRawData = await prisma.dataset.findUnique({
      where: { cmg_id: cmgSourceDataset._id.toString() },
    });
    
    if (!bioloopRawData) {
      logger.warn(`[BIGBANG] Bioloop RAW_DATA not found for CMG ID: ${cmgSourceDataset._id}`);
      skippedCount++;
      continue;
    }
    
    // Insert the hierarchy relationship
    await prisma.dataset_hierarchy.create({
      data: {
        source_id: bioloopRawData.id,
        derived_id: bioloopDataProduct.id,
      },
    });
    
    createdCount++;
  }
  
  logger.info(`[BIGBANG] Dataset hierarchy conversion complete: ${createdCount} created, ${skippedCount} skipped`);
}

module.exports = {
  syncDatasetHierarchies,
};

