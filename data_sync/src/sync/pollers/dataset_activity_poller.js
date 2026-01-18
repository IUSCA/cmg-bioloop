const logger = require('../../logger');
const BasePoller = require('./base_poller');

/**
 * Dataset Activity Poller
 *
 * Polls CMG datasets and dataproducts collections for path changes.
 * Updates: origin_path (immutable after bigbang)
 * Does NOT update: archive_path, staged_path, is_staged (set by Bioloop workers)
 * Does NOT: parse events (delegated to Workflow Status Poller), populate dataset_file
 */
class DatasetActivityPoller extends BasePoller {
  constructor(prisma, cmgDb, options = {}) {
    super('dataset_activity', prisma, cmgDb, {
      pollIntervalMs: options.pollIntervalMs || 10000, // 10 seconds
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
   * Updates: origin_path only (if changed)
   * Note: archive_path, staged_path, is_staged are managed by Bioloop workers, NOT synced from CMG
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

    // Extract paths
    const paths = cmgDataset.paths || {};
    const newOriginPath = paths.origin || null;

    // Only update if value actually changed (avoid unnecessary writes)
    if (bioloopDataset.origin_path === newOriginPath) {
      logger.debug(`[${this.pollerName}] No changes detected for dataset ${bioloopDataset.id}, skipping update`);
      return;
    }

    // Update only origin_path (immutable after bigbang)
    // DO NOT sync: archive_path, staged_path, is_staged (managed by Bioloop workers)
    const existingMetadata = bioloopDataset.metadata || {};

    await tx.dataset.update({
      where: { id: bioloopDataset.id },
      data: {
        origin_path: newOriginPath,
        metadata: {
          ...existingMetadata,
          cmg_sync_state: {
            cmg_updated_at: cmgDataset.updatedAt,
            last_sync_time: new Date(),
          },
        },
      },
    });

    logger.debug(`[${this.pollerName}] Updated origin_path for dataset ${bioloopDataset.id}: ${bioloopDataset.origin_path} -> ${newOriginPath}`);
  }
}

module.exports = DatasetActivityPoller;
