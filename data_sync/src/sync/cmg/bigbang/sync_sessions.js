const { ObjectId } = require('mongodb');
const { extractGenomicAttributes } = require('../utils/cmg_helpers');
const logger = require('../../../logger');

/**
 * Convert genome browser sessions from CMG to Bioloop
 * 
 * Note: 
 * - Sessions sync is optional (use --skip-sessions to skip)
 * - Only sessions with non-empty tracks array are migrated
 * - Tracks are NOT migrated from CMG (users create tracks in Bioloop UI)
 * - Only session metadata is migrated: title, genome, genome_type, owner
 */
async function syncSessions(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting genome browser sessions...');
  logger.info('[BIGBANG] Note: Tracks are NOT migrated (users create tracks in Bioloop UI)');
  
  const cmgSessions = await cmgDb.collection('sessions').find({}).toArray();
  logger.info(`[BIGBANG] Found ${cmgSessions.length} CMG sessions to convert`);
  
  let convertedCount = 0;
  let skippedCount = 0;
  
  for (const cmgSession of cmgSessions) {
    try {
      const result = await convertSession(prisma, cmgDb, cmgSession);
      if (result) {
        convertedCount++;
      } else {
        skippedCount++;
      }
    } catch (error) {
      logger.warn(`[BIGBANG] Failed to convert session ${cmgSession._id}: ${error.message}`);
      skippedCount++;
    }
  }
  
  logger.info(`[BIGBANG] Session conversion complete: ${convertedCount} succeeded, ${skippedCount} skipped`);
}

/**
 * Convert a single CMG session
 */
async function convertSession(prisma, cmgDb, cmgSession) {
  // Skip sessions with no tracks (empty sessions not useful in Bioloop)
  if (!cmgSession.tracks || !Array.isArray(cmgSession.tracks) || cmgSession.tracks.length === 0) {
    logger.debug(`[BIGBANG] Skipping session ${cmgSession._id}: no tracks`);
    return null;
  }
  
  // Map session owner
  let userId = null;
  if (cmgSession.user) {
    const user = await prisma.user.findFirst({
      where: { cmg_id: cmgSession.user.toString() },
    });
    
    if (!user) {
      logger.warn(`[BIGBANG] User not found for session ${cmgSession._id}, user CMG ID: ${cmgSession.user}`);
      return null;
    }
    userId = user.id;
  }
  
  const title = cmgSession.title || 'Genome Browser Session';
  const cmgSessionId = cmgSession._id.toString();
  
  // Extract genomic attributes using utility function to handle CMG field name variations
  // Note: Sessions use 'genome' field, but utility returns 'genome_value'
  const { genome_type, genome_value } = extractGenomicAttributes(cmgSession);
  const genome = genome_value; // Map genome_value to genome for session table
  const genomeType = genome_type;
  
  // Check if session already exists (idempotency)
  const existingSession = await prisma.genome_browser_session.findFirst({
    where: { cmg_id: cmgSessionId },
  });
  
  if (existingSession) {
    logger.debug(`[BIGBANG] Session ${cmgSessionId} already exists, skipping`);
    return existingSession;
  }
  
  // Extract unique CMG dataproduct IDs from tracks for later dataset lookup
  const dataproductIds = [];
  if (cmgSession.tracks && Array.isArray(cmgSession.tracks)) {
    cmgSession.tracks.forEach((track) => {
      if (track.dataproduct) {
        const dpId = track.dataproduct.toString();
        if (!dataproductIds.includes(dpId)) {
          dataproductIds.push(dpId);
        }
      }
    });
  }
  
  // Insert session with metadata containing CMG dataproduct IDs
  const session = await prisma.genome_browser_session.create({
    data: {
      title: title,
      genome: genome,
      genome_type: genomeType,
      user_id: userId,
      access_count: cmgSession.access_count || 0,
      cmg_id: cmgSessionId, // Track CMG session for provenance
      created_at: cmgSession.createdAt || new Date(), // Preserve CMG creation timestamp
      metadata: {
        origin: 'legacy',
        datasets: dataproductIds, // CMG dataproduct IDs for dataset lookup
      },
    },
  });
  
  logger.debug(`[BIGBANG] Session ${cmgSessionId} created with ${dataproductIds.length} associated dataproducts`);
  
  // Note: Tracks are NOT migrated from CMG
  // Tracks are created by users in Bioloop UI directly
  // CMG track data is not compatible with Bioloop's track model
  
  return session;
}

module.exports = {
  syncSessions,
};

