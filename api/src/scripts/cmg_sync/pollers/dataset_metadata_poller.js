const BasePoller = require('./base_poller');
const logger = require('@/services/logger');

/**
 * Dataset Metadata Poller
 * 
 * Polls CMG datasets and dataproducts collections for metadata changes.
 * Updates: description, size, du_size, num_files, num_directories, file_type
 * Does NOT update: name, type (these are immutable)
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
   * Updates: description, size, du_size, counts, file_type
   */
  async processDocument(cmgDataset, tx) {
    // Find dataset by cmg_id
    const bioloopDataset = await tx.dataset.findUnique({
      where: { cmg_id: cmgDataset._id.toString() },
    });
    
    if (!bioloopDataset) {
      logger.debug(`[${this.pollerName}] Dataset not found for CMG ID: ${cmgDataset._id}, skipping`);
      return;
    }
    
    // Determine if this is RAW_DATA or DATA_PRODUCT
    const isRawData = this.currentCollection === 'datasets';
    
    // Update metadata fields
    const existingMetadata = bioloopDataset.metadata || {};
    
    await tx.dataset.update({
      where: { id: bioloopDataset.id },
      data: {
        description: cmgDataset.description || null,
        size: cmgDataset.size ? BigInt(cmgDataset.size) : null,
        du_size: isRawData ? (cmgDataset.du_size ? BigInt(cmgDataset.du_size) : null) : null,
        num_files: isRawData ? (cmgDataset.files || 0) : 0,
        num_directories: cmgDataset.directories || 0,
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

