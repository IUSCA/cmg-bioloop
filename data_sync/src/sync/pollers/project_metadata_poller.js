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
   * Updates: name, description, browser_enabled, funding (if changed)
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

    // Extract new values
    const newName = cmgProject.name;
    const newDescription = cmgProject.description || null;
    const newBrowserEnabled = cmgProject.igv_enabled || false;
    const newFunding = cmgProject.funding || null;

    // Check if any fields actually changed
    const nameChanged = bioloopProject.name !== newName;
    const descriptionChanged = bioloopProject.description !== newDescription;
    const browserEnabledChanged = bioloopProject.browser_enabled !== newBrowserEnabled;
    const fundingChanged = bioloopProject.funding !== newFunding;

    if (!nameChanged && !descriptionChanged && !browserEnabledChanged && !fundingChanged) {
      logger.debug(`[${this.pollerName}] No changes detected for project ${bioloopProject.id}, skipping update`);
      return;
    }

    // Build update data with only changed fields
    const updateData = {};
    const changes = [];

    if (nameChanged) {
      updateData.name = newName;
      changes.push(`name: "${bioloopProject.name}" -> "${newName}"`);
    }

    if (descriptionChanged) {
      updateData.description = newDescription;
      changes.push(`description`);
    }

    if (browserEnabledChanged) {
      updateData.browser_enabled = newBrowserEnabled;
      changes.push(`browser_enabled: ${bioloopProject.browser_enabled} -> ${newBrowserEnabled}`);
    }

    if (fundingChanged) {
      updateData.funding = newFunding;
      changes.push(`funding`);
    }

    // Prepare metadata update
    const existingMetadata = bioloopProject.metadata || {};

    await tx.project.update({
      where: { id: bioloopProject.id },
      data: {
        ...updateData,
        metadata: {
          ...existingMetadata,
          cmg_sync_state: {
            cmg_updated_at: cmgProject.updatedAt,
            last_sync_time: new Date(),
          },
        },
      },
    });

    logger.debug(`[${this.pollerName}] Updated project ${bioloopProject.id}: ${changes.join(', ')}`);
  }
}

module.exports = ProjectMetadataPoller;
