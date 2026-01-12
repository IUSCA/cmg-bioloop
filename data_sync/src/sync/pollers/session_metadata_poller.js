const logger = require('../logger');
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
   * Updates: title, access_count, is_public, staging fields
   */
  async processDocument(cmgSession, tx) {
    // Find session by cmg_id
    const bioloopSession = await tx.genome_browser_session.findUnique({
      where: { cmg_id: cmgSession._id.toString() },
    });

    if (!bioloopSession) {
      logger.debug(`[${this.pollerName}] Session not found for CMG ID: ${cmgSession._id}, skipping`);
      return;
    }

    // Extract staging information
    const stagingRequested = cmgSession.staging_requested || null;
    const stagingCompleted = cmgSession.staging_completed || false;
    const stagingRequestedBy = cmgSession.staging_requested_by || null;

    // If staging_requested_by is a username, look up the user_id
    let stagingRequestedById = null;
    if (stagingRequestedBy) {
      const requestingUser = await tx.user.findFirst({
        where: {
          OR: [
            { username: stagingRequestedBy },
            { cmg_id: stagingRequestedBy },
          ],
        },
      });
      stagingRequestedById = requestingUser ? requestingUser.id : null;
    }

    await tx.genome_browser_session.update({
      where: { id: bioloopSession.id },
      data: {
        title: cmgSession.title || null,
        access_count: cmgSession.access_count || 0,
        is_public: cmgSession.is_public || false,
        staging_requested: stagingRequested,
        staging_completed: stagingCompleted,
        staging_requested_by: stagingRequestedById,
      },
    });

    logger.debug(`[${this.pollerName}] Updated metadata for session ${bioloopSession.id}`);
  }
}

module.exports = SessionMetadataPoller;
