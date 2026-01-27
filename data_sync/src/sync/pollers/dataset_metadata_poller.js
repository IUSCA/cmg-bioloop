const BasePoller = require('./base_poller');
const logger = require('../../logger');

/**
 * Dataset Metadata Poller
 * 
 * Polls CMG datasets and dataproducts collections for metadata changes.
 * Updates: description only (user-editable field)
 * Does NOT update: file_type (immutable after bigbang), metadata (no sync tracking)
 * Does NOT update: name, type (immutable), size, du_size, num_files, num_directories (set by Bioloop inspect_dataset worker)
 */
class DatasetMetadataPoller extends BasePoller {
  constructor(prisma, cmgDb, options = {}) {
    super('dataset_metadata', prisma, cmgDb, {
      pollIntervalMs: options.pollIntervalMs || 15000, // 15 seconds (less critical)
      batchSize: options.batchSize || 200,
      ...options,
    });
    
    // Track which collection we're currently polling
    this.currentCollection = 'datasets';
  }
  
  getCollectionName() {
    // Alternate between datasets and dataproducts
    return this.currentCollection;
  }
  
  /**
   * Override poll to alternate between datasets and dataproducts
   */
  async poll() {
    // Poll datasets
    this.currentCollection = 'datasets';
    await super.poll();
    
    // Poll dataproducts
    this.currentCollection = 'dataproducts';
    await super.poll();
  }
  
  /**
   * Process a single dataset/dataproduct document
   * Updates: description only (user-editable field)
   * Does NOT update: file_type (immutable after bigbang), metadata (no sync tracking)
   * Note: size, du_size, num_files, num_directories are computed by inspect_dataset worker, NOT synced from CMG
   */
  async processDocument(cmgDataset, tx) {
    // Find dataset by cmg_id
    const bioloopDataset = await tx.dataset.findFirst({
      where: { cmg_id: cmgDataset._id.toString() },
    });
    
    if (!bioloopDataset) {
      logger.debug(`[${this.pollerName}] Dataset not found for CMG ID: ${cmgDataset._id}, skipping`);
      return;
    }
    
    // Extract new description value
    const newDescription = cmgDataset.description || null;
    
    // Check if description actually changed
    if (bioloopDataset.description === newDescription) {
      logger.debug(`[${this.pollerName}] No changes detected for dataset ${bioloopDataset.id}, skipping update`);
      return;
    }
    
    // Update only description (user-editable field)
    // DO NOT update: file_type (immutable), metadata (no sync tracking)
    // DO NOT sync: size, du_size, num_files, num_directories (computed by inspect_dataset worker)
    await tx.dataset.update({
      where: { id: bioloopDataset.id },
      data: {
        description: newDescription,
      },
    });
    
    logger.debug(`[${this.pollerName}] Updated dataset ${bioloopDataset.id}: description: "${bioloopDataset.description}" -> "${newDescription}"`);
  }
}

module.exports = DatasetMetadataPoller;

