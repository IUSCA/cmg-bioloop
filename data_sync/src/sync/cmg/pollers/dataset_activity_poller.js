const logger = require('../../../logger');
const BasePoller = require('./base_poller');

/**
 * Dataset Activity Poller
 *
 * DEPRECATED: This poller no longer updates any fields.
 * - origin_path: Immutable after bigbang, should NOT be updated
 * - archive_path, staged_path, is_staged: Managed by Bioloop workers, NOT synced from CMG
 * - metadata: Should NOT store sync tracking data
 *
 * This poller is kept for backward compatibility but does nothing.
 * Consider removing it entirely.
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
   * DOES NOTHING: All dataset fields are either immutable or Bioloop-managed
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

    // No fields to update - all dataset fields are either:
    // - Immutable after bigbang (origin_path, name, type)
    // - Bioloop-managed (archive_path, staged_path, is_staged, size, num_files, etc.)
    // - Should not store sync data (metadata)
    logger.debug(`[${this.pollerName}] No updates needed for dataset ${bioloopDataset.id} (all fields immutable or Bioloop-managed)`);
  }
}

module.exports = DatasetActivityPoller;
