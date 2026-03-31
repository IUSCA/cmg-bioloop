/**
 * Xenium Project ACL Poller
 *
 * Watches the Xenium `project` table for ACL changes (project_user and
 * project_dataset associations) and syncs them into cmg-bioloop.
 *
 * Cursor: tracks `project.updated_at` + `project.id`.
 *
 * Implemented in Chat 3.
 */

const logger = require('../../../logger');
const XeniumBasePoller = require('./base_poller');
const { XENIUM_POLLER_NAMES } = require('../constants');

class XeniumProjectACLPoller extends XeniumBasePoller {
  constructor(prisma, xeniumPrisma, options = {}) {
    const aclDefaults = {
      pollIntervalMs: 10000,
      batchSize: 50,
      transactionTimeoutMs: 30000,
    };
    super(XENIUM_POLLER_NAMES.PROJECT_ACL, prisma, xeniumPrisma, {
      ...aclDefaults,
      ...options,
    });
  }

  getSourceModel() {
    return 'project';
  }

  /**
   * Sync ACL changes for a single xenium project row.
   *
   * Strategy:
   *   - Find the Bioloop project where xenium_id = row.id
   *   - If not found, skip with warning
   *   - Fetch project_user rows from xeniumPrisma where project_id = row.id
   *   - Fetch project_dataset rows from xeniumPrisma where project_id = row.id
   *   - Upsert project_user and project_dataset associations in Bioloop
   *     resolving user and dataset IDs via xenium_id lookups
   *
   * @param {Object} row - Xenium project row
   * @param {Object} tx  - Target Prisma transaction
   */
  async processRow(row, tx) {
    const bioloopProject = await tx.project.findFirst({
      where: { xenium_id: row.id },
    });

    if (!bioloopProject) {
      logger.debug(`[${this.pollerName}] Project not found for xenium_id=${row.id}, skipping`);
      return;
    }

    const [sourceProjectUsers, sourceProjectDatasets] = await Promise.all([
      this.xeniumPrisma.project_user.findMany({
        where: { project_id: row.id },
      }),
      this.xeniumPrisma.project_dataset.findMany({
        where: { project_id: row.id },
      }),
    ]);

    const sourceUserIds = sourceProjectUsers.map((x) => x.user_id);
    const sourceDatasetIds = sourceProjectDatasets.map((x) => x.dataset_id);

    const [targetUsers, targetDatasets] = await Promise.all([
      tx.user.findMany({
        where: { xenium_id: { in: sourceUserIds.length ? sourceUserIds : [-1] } },
        select: { id: true, xenium_id: true },
      }),
      tx.dataset.findMany({
        where: { xenium_id: { in: sourceDatasetIds.length ? sourceDatasetIds : [-1] } },
        select: { id: true, xenium_id: true },
      }),
    ]);

    const userByXeniumId = new Map(targetUsers.map((u) => [u.xenium_id, u.id]));
    const datasetByXeniumId = new Map(targetDatasets.map((d) => [d.xenium_id, d.id]));

    await tx.project_user.deleteMany({
      where: { project_id: bioloopProject.id },
    });
    await tx.project_dataset.deleteMany({
      where: { project_id: bioloopProject.id },
    });

    const userAssociations = sourceProjectUsers
      .map((x) => userByXeniumId.get(x.user_id))
      .filter(Boolean)
      .map((userId) => ({
        project_id: bioloopProject.id,
        user_id: userId,
      }));

    const datasetAssociations = sourceProjectDatasets
      .map((x) => datasetByXeniumId.get(x.dataset_id))
      .filter(Boolean)
      .map((datasetId) => ({
        project_id: bioloopProject.id,
        dataset_id: datasetId,
      }));

    if (userAssociations.length > 0) {
      await tx.project_user.createMany({
        data: userAssociations,
        skipDuplicates: true,
      });
    }

    if (datasetAssociations.length > 0) {
      await tx.project_dataset.createMany({
        data: datasetAssociations,
        skipDuplicates: true,
      });
    }
  }
}

module.exports = XeniumProjectACLPoller;
