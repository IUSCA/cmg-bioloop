const BasePoller = require('./base_poller');
const logger = require('../../logger');

/**
 * Dataset Metadata Poller
 * 
 * Polls CMG datasets and dataproducts collections for metadata changes.
 * Updates: description, file_type
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
   * Updates: description, file_type (user-editable metadata only)
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
    
    // Update only user-editable metadata fields
    // DO NOT sync: size, du_size, num_files, num_directories (computed by inspect_dataset worker)
    const existingMetadata = bioloopDataset.metadata || {};
    
    await tx.dataset.update({
      where: { id: bioloopDataset.id },
      data: {
        description: cmgDataset.description || null,
        file_type: cmgDataset.file_type || null, // dataproducts only
        metadata: {
          ...existingMetadata,
          cmg_sync_state: {
            cmg_updated_at: cmgDataset.updatedAt,
            last_sync_time: new Date(),
          },
        },
      },
    });
    
    logger.debug(`[${this.pollerName}] Updated metadata for dataset ${bioloopDataset.id}`);
  }
}

module.exports = DatasetMetadataPoller;

