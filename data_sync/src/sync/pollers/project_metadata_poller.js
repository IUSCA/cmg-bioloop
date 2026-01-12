const logger = require('../../logger');
const BasePoller = require('./base_poller');

/**
 * Project Metadata Poller
 *
 * Polls CMG projects collection for metadata changes.
 * Updates: name, description, browser_enabled, funding, metadata
 * Does NOT update: slug (derived from name), cmg_id (immutable)
 */
class ProjectMetadataPoller extends BasePoller {
  constructor(prisma, cmgDb, options = {}) {
    super('project_metadata', prisma, cmgDb, {
      pollIntervalMs: options.pollIntervalMs || 20000, // 20 seconds (less frequent)
      batchSize: options.batchSize || 100,
      ...options,
    });
  }

  getCollectionName() {
    return 'projects';
  }

  /**
   * Process a single project document
   * Updates: name, description, browser_enabled, funding, metadata
   */
  async processDocument(cmgProject, tx) {
    // Find project by cmg_id
    const bioloopProject = await tx.project.findFirst({
      where: { cmg_id: cmgProject._id.toString() },
    });

    if (!bioloopProject) {
      logger.debug(`[${this.pollerName}] Project not found for CMG ID: ${cmgProject._id}, skipping`);
      return;
    }

    // Prepare metadata update
    const existingMetadata = bioloopProject.metadata || {};

    await tx.project.update({
      where: { id: bioloopProject.id },
      data: {
        name: cmgProject.name,
        description: cmgProject.description || null,
        browser_enabled: cmgProject.igv_enabled || false,
        funding: cmgProject.funding || null,
        metadata: {
          ...existingMetadata,
          cmg_sync_state: {
            cmg_updated_at: cmgProject.updatedAt,
            last_sync_time: new Date(),
          },
        },
      },
    });

    logger.debug(`[${this.pollerName}] Updated metadata for project ${bioloopProject.id}`);
  }
}

module.exports = ProjectMetadataPoller;
