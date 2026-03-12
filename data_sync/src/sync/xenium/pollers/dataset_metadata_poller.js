/**
 * Xenium Dataset Metadata Poller
 *
 * Watches the Xenium `dataset` table for user-editable metadata changes
 * (currently: `description`) and syncs them into cmg-bioloop.
 *
 * Cursor: tracks `dataset.updated_at` + `dataset.id`.
 *
 * Does NOT sync: origin_path, archive_path, is_staged, file_type — these are
 * immutable after bigbang migration. Only user-editable fields are synced.
 *
 * Implemented in Chat 3.
 */

const logger = require('../../../logger');
const XeniumBasePoller = require('./base_poller');
const { XENIUM_POLLER_NAMES } = require('../constants');

class XeniumDatasetMetadataPoller extends XeniumBasePoller {
  constructor(prisma, xeniumPrisma, options = {}) {
    super(XENIUM_POLLER_NAMES.DATASET_METADATA, prisma, xeniumPrisma, options);
  }

  getSourceModel() {
    return 'dataset';
  }

  /**
   * Sync metadata changes for a single xenium dataset row.
   *
   * Strategy:
   *   - Find the Bioloop dataset where xenium_id = row.id
   *   - If not found, skip with warning
   *   - Update only the `description` field (user-editable in xenium)
   *
   * @param {Object} row - Xenium dataset row
   * @param {Object} tx  - Target Prisma transaction
   */
  async processRow(row, tx) {
    const bioloopDataset = await tx.dataset.findFirst({
      where: { xenium_id: row.id },
      select: {
        id: true,
        description: true,
      },
    });

    if (!bioloopDataset) {
      logger.debug(`[${this.pollerName}] Dataset not found for xenium_id=${row.id}, skipping`);
      return;
    }

    const newDescription = row.description || null;
    if (bioloopDataset.description === newDescription) {
      return;
    }

    await tx.dataset.update({
      where: { id: bioloopDataset.id },
      data: { description: newDescription },
    });
  }
}

module.exports = XeniumDatasetMetadataPoller;
