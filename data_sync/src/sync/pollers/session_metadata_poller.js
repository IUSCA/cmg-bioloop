const logger = require('../../logger');
const BasePoller = require('./base_poller');

/**
 * Session Metadata Poller
 *
 * Polls CMG sessions collection for genome browser session metadata changes.
 * Updates: title, access_count, is_public, staging fields
 * Does NOT update: genome, genome_type (immutable after creation)
 */
class SessionMetadataPoller extends BasePoller {
  constructor(prisma, cmgDb, options = {}) {
    super('session_metadata', prisma, cmgDb, {
      pollIntervalMs: options.pollIntervalMs || 30000, // 30 seconds (least frequent)
      batchSize: options.batchSize || 50,
      ...options,
    });
  }

  getCollectionName() {
    return 'sessions';
  }

  /**
   * Process a single session document
   * Updates: title, access_count, is_public, staging fields (if changed)
   */
  async processDocument(cmgSession, tx) {
    // Find session by cmg_id
    const bioloopSession = await tx.genome_browser_session.findFirst({
      where: { cmg_id: cmgSession._id.toString() },
    });

    if (!bioloopSession) {
      logger.debug(`[${this.pollerName}] Session not found for CMG ID: ${cmgSession._id}, skipping`);
      return;
    }

    // Extract new values
    const newTitle = cmgSession.title || null;
    const newAccessCount = cmgSession.access_count || 0;
    const newIsPublic = cmgSession.is_public || false;
    const newStagingRequested = cmgSession.staging_requested || null;
    const newStagingCompleted = cmgSession.staging_completed || false;
    const stagingRequestedBy = cmgSession.staging_requested_by || null;

    // If staging_requested_by is a username, look up the user_id
    let newStagingRequestedById = null;
    if (stagingRequestedBy) {
      const requestingUser = await tx.user.findFirst({
        where: {
          OR: [
            { username: stagingRequestedBy },
            { cmg_id: stagingRequestedBy },
          ],
        },
      });
      newStagingRequestedById = requestingUser ? requestingUser.id : null;
    }

    // Check if any fields actually changed
    const titleChanged = bioloopSession.title !== newTitle;
    const accessCountChanged = bioloopSession.access_count !== newAccessCount;
    const isPublicChanged = bioloopSession.is_public !== newIsPublic;
    const stagingRequestedChanged = bioloopSession.staging_requested?.getTime() !== newStagingRequested?.getTime();
    const stagingCompletedChanged = bioloopSession.staging_completed !== newStagingCompleted;
    const stagingRequestedByChanged = bioloopSession.staging_requested_by !== newStagingRequestedById;

    if (!titleChanged && !accessCountChanged && !isPublicChanged && 
        !stagingRequestedChanged && !stagingCompletedChanged && !stagingRequestedByChanged) {
      logger.debug(`[${this.pollerName}] No changes detected for session ${bioloopSession.id}, skipping update`);
      return;
    }

    // Build update data with only changed fields
    const updateData = {};
    const changes = [];

    if (titleChanged) {
      updateData.title = newTitle;
      changes.push(`title: "${bioloopSession.title}" -> "${newTitle}"`);
    }

    if (accessCountChanged) {
      updateData.access_count = newAccessCount;
      changes.push(`access_count: ${bioloopSession.access_count} -> ${newAccessCount}`);
    }

    if (isPublicChanged) {
      updateData.is_public = newIsPublic;
      changes.push(`is_public: ${bioloopSession.is_public} -> ${newIsPublic}`);
    }

    if (stagingRequestedChanged) {
      updateData.staging_requested = newStagingRequested;
      changes.push('staging_requested');
    }

    if (stagingCompletedChanged) {
      updateData.staging_completed = newStagingCompleted;
      changes.push(`staging_completed: ${bioloopSession.staging_completed} -> ${newStagingCompleted}`);
    }

    if (stagingRequestedByChanged) {
      updateData.staging_requested_by = newStagingRequestedById;
      changes.push('staging_requested_by');
    }

    await tx.genome_browser_session.update({
      where: { id: bioloopSession.id },
      data: updateData,
    });

    logger.debug(`[${this.pollerName}] Updated session ${bioloopSession.id}: ${changes.join(', ')}`);
  }
}

module.exports = SessionMetadataPoller;
