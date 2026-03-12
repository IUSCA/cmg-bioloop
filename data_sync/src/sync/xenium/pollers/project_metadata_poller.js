/**
 * Xenium Project Metadata Poller
 *
 * Watches the Xenium `project` table for metadata changes (name, description,
 * browser_enabled, funding) and syncs them into cmg-bioloop.
 *
 * Cursor: tracks `project.updated_at` + `project.id`.
 *
 * Implemented in Chat 3.
 */

const logger = require('../../../logger');
const XeniumBasePoller = require('./base_poller');
const { XENIUM_POLLER_NAMES } = require('../constants');

class XeniumProjectMetadataPoller extends XeniumBasePoller {
  constructor(prisma, xeniumPrisma, options = {}) {
    super(XENIUM_POLLER_NAMES.PROJECT_METADATA, prisma, xeniumPrisma, options);
  }

  getSourceModel() {
    return 'project';
  }

  /**
   * Sync metadata changes for a single xenium project row.
   *
   * Strategy:
   *   - Find the Bioloop project where xenium_id = row.id
   *   - If not found, skip with warning
   *   - Update: name, description, browser_enabled, funding
   *
   * @param {Object} row - Xenium project row
   * @param {Object} tx  - Target Prisma transaction
   */
  async processRow(row, tx) {
    const bioloopProject = await tx.project.findFirst({
      where: { xenium_id: row.id },
      select: {
        id: true,
        name: true,
        description: true,
        browser_enabled: true,
        funding: true,
      },
    });

    if (!bioloopProject) {
      logger.debug(`[${this.pollerName}] Project not found for xenium_id=${row.id}, skipping`);
      return;
    }

    const newName = row.name || bioloopProject.name;
    const newDescription = row.description || null;
    const newBrowserEnabled = row.browser_enabled ?? false;
    const newFunding = row.funding || null;

    const updateData = {};
    if (bioloopProject.name !== newName) updateData.name = newName;
    if (bioloopProject.description !== newDescription) updateData.description = newDescription;
    if (bioloopProject.browser_enabled !== newBrowserEnabled) updateData.browser_enabled = newBrowserEnabled;
    if (bioloopProject.funding !== newFunding) updateData.funding = newFunding;

    if (Object.keys(updateData).length === 0) return;

    await tx.project.update({
      where: { id: bioloopProject.id },
      data: updateData,
    });
  }
}

module.exports = XeniumProjectMetadataPoller;
