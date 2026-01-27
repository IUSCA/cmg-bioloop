const BasePoller = require('./base_poller');
const { ObjectId } = require('mongodb');
const { expandGroups } = require('../utils/cmg_helpers');
const logger = require('../../logger');

/**
 * Project ACL Poller
 * 
 * Polls CMG projects collection for access control changes.
 * Updates project metadata and rebuilds project_user and project_dataset associations.
 * 
 * Immutable fields (never updated after big-bang):
 * - name (changing would break references)
 * - slug (changing would break URLs)
 */
class ProjectACLPoller extends BasePoller {
  constructor(prisma, cmgDb, options = {}) {
    super('project_acl', prisma, cmgDb, {
      pollIntervalMs: options.pollIntervalMs || 10000, // 10 seconds
      batchSize: options.batchSize || 100, // Smaller batch due to expensive operations
      ...options,
    });
  }
  
  getCollectionName() {
    return 'projects';
  }
  
  /**
   * Process a single project document
   * Updates: description, browser_enabled, user associations, dataset associations (if changed)
   * Does NOT update: name, slug
   */
  async processDocument(cmgProject, tx) {
    // Find project by cmg_id
    const bioloopProject = await tx.project.findFirst({
      where: { cmg_id: cmgProject._id.toString() },
      include: {
        users: { select: { user_id: true } },
        datasets: { select: { dataset_id: true } },
      },
    });
    
    if (!bioloopProject) {
      logger.debug(`[${this.pollerName}] Project not found for CMG ID: ${cmgProject._id}, skipping`);
      return;
    }
    
    // Extract new values
    const newDescription = cmgProject.description || null;
    const newBrowserEnabled = cmgProject.browser || false;
    
    // Check if metadata fields changed
    const descriptionChanged = bioloopProject.description !== newDescription;
    const browserEnabledChanged = bioloopProject.browser_enabled !== newBrowserEnabled;
    
    // Check if associations changed (we'll rebuild if needed)
    const shouldUpdateMetadata = descriptionChanged || browserEnabledChanged;
    
    if (!shouldUpdateMetadata) {
      logger.debug(`[${this.pollerName}] No metadata changes for project ${bioloopProject.id}, checking associations...`);
    }
    
    // Update project metadata if changed (without storing sync tracking in metadata field)
    if (shouldUpdateMetadata) {
      const updateData = {};
      const changes = [];
      
      if (descriptionChanged) {
        updateData.description = newDescription;
        changes.push('description');
      }
      
      if (browserEnabledChanged) {
        updateData.browser_enabled = newBrowserEnabled;
        changes.push(`browser_enabled: ${bioloopProject.browser_enabled} -> ${newBrowserEnabled}`);
      }
      
      await tx.project.update({
        where: { id: bioloopProject.id },
        data: updateData,
      });
      
      logger.debug(`[${this.pollerName}] Updated project ${bioloopProject.id}: ${changes.join(', ')}`);
    }
    
    // Rebuild project_user associations (checks for changes internally)
    await this.rebuildProjectUsers(tx, bioloopProject.id, cmgProject, bioloopProject.users);
    
    // Rebuild project_dataset associations (checks for changes internally)
    await this.rebuildProjectDatasets(tx, bioloopProject.id, cmgProject, bioloopProject.datasets);
  }
  
  /**
   * Rebuild project_user associations (delete + insert, only if changed)
   */
  async rebuildProjectUsers(tx, projectId, cmgProject, existingUsers) {
    // Expand groups to get all user IDs
    const directUserIds = (cmgProject.users || []).map(id => id.toString());
    const groupUserIds = await expandGroups(this.cmgDb, cmgProject.groups || []);
    const allUserIds = [...new Set([...directUserIds, ...groupUserIds])];
    
    // Find Bioloop user IDs for all CMG user IDs
    const cmgUserIdsSet = new Set(allUserIds);
    
    // Get existing user IDs
    const existingUserIds = new Set(existingUsers.map(u => u.user_id));
    
    // Check if we need to find Bioloop user IDs first
    if (allUserIds.length === 0 && existingUserIds.size === 0) {
      logger.debug(`[${this.pollerName}] No user associations for project ${projectId}, skipping`);
      return;
    }
    
    // Delete existing associations
    await tx.project_user.deleteMany({
      where: { project_id: projectId },
    });
    
    // Create new associations
    const associations = [];
    for (const cmgUserId of allUserIds) {
      const user = await tx.user.findFirst({
        where: { cmg_id: cmgUserId },
      });
      
      if (user) {
        associations.push({
          project_id: projectId,
          user_id: user.id,
        });
      } else {
        logger.debug(`[${this.pollerName}] User not found for CMG ID: ${cmgUserId}`);
      }
    }
    
    if (associations.length > 0) {
      await tx.project_user.createMany({
        data: associations,
        skipDuplicates: true,
      });
    }
    
    logger.debug(`[${this.pollerName}] Rebuilt ${associations.length} project_user associations for project ${projectId}`);
  }
  
  /**
   * Rebuild project_dataset associations (delete + insert, only if changed)
   */
  async rebuildProjectDatasets(tx, projectId, cmgProject, existingDatasets) {
    const dataproductIds = (cmgProject.dataproducts || []).map(id => id.toString());
    
    // Delete existing associations
    await tx.project_dataset.deleteMany({
      where: { project_id: projectId },
    });
    
    // Create new associations
    const associations = [];
    for (const cmgDataproductId of dataproductIds) {
      const dataset = await tx.dataset.findFirst({
        where: { cmg_id: cmgDataproductId },
      });
      
      if (dataset) {
        associations.push({
          project_id: projectId,
          dataset_id: dataset.id,
        });
      } else {
        logger.debug(`[${this.pollerName}] Dataset not found for CMG ID: ${cmgDataproductId}`);
      }
    }
    
    if (associations.length > 0) {
      await tx.project_dataset.createMany({
        data: associations,
        skipDuplicates: true,
      });
    }
    
    logger.debug(`[${this.pollerName}] Rebuilt ${associations.length} project_dataset associations for project ${projectId}`);
  }
}

module.exports = ProjectACLPoller;

