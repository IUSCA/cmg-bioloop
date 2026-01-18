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
   * Updates: description, file_type (user-editable metadata only, if changed)
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
    
    // Extract new values
    const newDescription = cmgDataset.description || null;
    const newFileType = cmgDataset.file_type || null;
    
    // Check if any fields actually changed
    const descriptionChanged = bioloopDataset.description !== newDescription;
    const fileTypeChanged = bioloopDataset.file_type !== newFileType;
    
    if (!descriptionChanged && !fileTypeChanged) {
      logger.debug(`[${this.pollerName}] No changes detected for dataset ${bioloopDataset.id}, skipping update`);
      return;
    }
    
    // Build update data with only changed fields
    const updateData = {};
    const changes = [];
    
    if (descriptionChanged) {
      updateData.description = newDescription;
      changes.push(`description: "${bioloopDataset.description}" -> "${newDescription}"`);
    }
    
    if (fileTypeChanged) {
      updateData.file_type = newFileType;
      changes.push(`file_type: "${bioloopDataset.file_type}" -> "${newFileType}"`);
    }
    
    // Update only user-editable metadata fields
    // DO NOT sync: size, du_size, num_files, num_directories (computed by inspect_dataset worker)
    const existingMetadata = bioloopDataset.metadata || {};
    
    await tx.dataset.update({
      where: { id: bioloopDataset.id },
      data: {
        ...updateData,
        metadata: {
          ...existingMetadata,
          cmg_sync_state: {
            cmg_updated_at: cmgDataset.updatedAt,
            last_sync_time: new Date(),
          },
        },
      },
    });
    
    logger.debug(`[${this.pollerName}] Updated dataset ${bioloopDataset.id}: ${changes.join(', ')}`);
  }
}

module.exports = DatasetMetadataPoller;

