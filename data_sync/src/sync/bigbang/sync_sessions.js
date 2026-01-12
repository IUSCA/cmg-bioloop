const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Convert genome browser sessions from CMG to Bioloop
 * Equivalent to: db_conversion/src/convert/entity/session.py::convert_sessions()
 * 
 * Note: Sessions sync is optional and may skip many entries due to missing dataset_files
 */
async function syncSessions(prisma, cmgDb) {
  logger.info('[BIGBANG] Converting genome browser sessions...');
  logger.warn('[BIGBANG] Note: Session conversion may skip many entries due to missing dataset_file records');
  
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
  const genome = cmgSession.genome || null;
  const genomeType = cmgSession.genome_type || null;
  
  // Insert session
  const session = await prisma.genome_browser_session.create({
    data: {
      title: title,
      genome: genome,
      genome_type: genomeType,
      user_id: userId,
      access_count: cmgSession.access_count || 0,
    },
  });
  
  // Process tracks
  const tracks = cmgSession.tracks || [];
  const sessionTrackAssociations = [];
  
  for (const cmgTrack of tracks) {
    const cmgDataproductId = cmgTrack.dataproduct;
    const cmgFileName = cmgTrack.filename;
    
    if (!cmgDataproductId || !cmgFileName) {
      logger.warn(`[BIGBANG] Track missing dataproduct or filename in session ${cmgSession._id}`);
      continue;
    }
    
    // Find corresponding Bioloop DATA_PRODUCT
    const bioloopDataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgDataproductId.toString() },
    });
    
    if (!bioloopDataset) {
      logger.warn(`[BIGBANG] Dataset not found for track in session ${cmgSession._id}, dataproduct: ${cmgDataproductId}`);
      continue;
    }
    
    // Find dataset_file by name
    const datasetFile = await prisma.dataset_file.findFirst({
      where: {
        dataset_id: bioloopDataset.id,
        name: cmgFileName,
      },
    });
    
    if (!datasetFile) {
      // This is expected - we're not populating dataset_file table in bigbang
      continue;
    }
    
    // Create track
    const track = await prisma.track.create({
      data: {
        name: cmgTrack.title || cmgFileName,
        dataset_file_id: datasetFile.id,
      },
    });
    
    sessionTrackAssociations.push({
      session_id: session.id,
      track_id: track.id,
    });
  }
  
  // Create session_track associations
  if (sessionTrackAssociations.length > 0) {
    await prisma.session_track.createMany({
      data: sessionTrackAssociations,
    });
  }
  
  return session;
}

module.exports = {
  syncSessions,
};

