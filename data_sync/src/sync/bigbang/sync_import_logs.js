const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Convert CMG uploads to Bioloop dataset_import_log entries
 * CMG's "Upload" feature = Bioloop's "Import" feature (register from remote filesystem)
 * 
 * Equivalent to what would be created when using Bioloop's Import feature
 */
async function syncImportLogs(prisma, cmgDb, cmgUserId) {
  logger.info('[BIGBANG] Converting CMG upload history to Bioloop import logs...');
  
  const uploadsCollection = cmgDb.collection('uploads');
  
  // Get total count for progress tracking
  const totalCount = await uploadsCollection.countDocuments({});
  logger.info(`[BIGBANG] Found ${totalCount} CMG upload records to process`);
  
  let processedCount = 0;
  let createdCount = 0;
  let skippedCount = 0;
  const BATCH_SIZE = 100;
  
  // Use cursor to stream data instead of loading all at once
  const cursor = uploadsCollection.find({}).batchSize(BATCH_SIZE);
  
  for await (const cmgUpload of cursor) {
    let bioloopDataset = null; // Declare outside try block for error handler access
    
    try {
      // CMG uploads can reference either a 'dataset' (RAW_DATA) or 'dataproduct' (DATA_PRODUCT)
      // Find which one exists and use it
      let datasetCmgId = null;
      if (cmgUpload.dataset) {
        datasetCmgId = cmgUpload.dataset.toString();
      } else if (cmgUpload.dataproduct) {
        datasetCmgId = cmgUpload.dataproduct.toString();
      }
      
      // Skip if neither dataset nor dataproduct is specified
      if (!datasetCmgId) {
        logger.warn(`[BIGBANG] Skipping CMG upload ${cmgUpload._id}: no dataset or dataproduct reference`);
        skippedCount++;
        processedCount++;
        continue;
      }
      
      // Find the corresponding Bioloop dataset
      bioloopDataset = await prisma.dataset.findFirst({
        where: { cmg_id: datasetCmgId },
      });
      
      if (!bioloopDataset) {
        logger.warn(`[BIGBANG] No Bioloop dataset found for CMG upload ${cmgUpload._id} (cmg_id: ${datasetCmgId})`);
        skippedCount++;
        processedCount++;
        continue;
      }
      
      // Find the user who created this upload
      let userId = cmgUserId; // Default to CMG system user
      if (cmgUpload.user) {
        const bioloopUser = await prisma.user.findFirst({
          where: { cmg_id: cmgUpload.user.toString() },
        });
        if (bioloopUser) {
          userId = bioloopUser.id;
        }
      }
      
      // Check if an import log already exists for this dataset
      // (to support idempotent re-runs)
      const existingImportLog = await prisma.dataset_import_log.findFirst({
        where: {
          cmg_id: cmgUpload._id.toString(),
        },
      });
      
      if (existingImportLog) {
        logger.debug(`[BIGBANG] Import log already exists for CMG upload ${cmgUpload._id}`);
        skippedCount++;
        processedCount++;
        continue;
      }
      
      // Find or create the 'create' audit log entry for this dataset
      // The audit log with action='create' should have been created during dataset sync
      let auditLog = await prisma.dataset_audit.findFirst({
        where: {
          dataset_id: bioloopDataset.id,
          action: 'create',
        },
      });
      
      // If no 'create' audit log exists, create one
      if (!auditLog) {
        auditLog = await prisma.dataset_audit.create({
          data: {
            action: 'create',
            create_method: 'IMPORT',
            timestamp: cmgUpload.createdAt || new Date(),
            dataset_id: bioloopDataset.id,
            user_id: userId,
          },
        });
      } else {
        // Update existing audit log to mark it as an IMPORT
        if (!auditLog.create_method) {
          auditLog = await prisma.dataset_audit.update({
            where: { id: auditLog.id },
            data: { create_method: 'IMPORT' },
          });
        }
      }
      
      // Note: source_run (source dataset for derived products) is not tracked in CMG uploads
      // Set to null as this information is not available
      const sourceRun = null;
      
      // Create the import log entry
      await prisma.dataset_import_log.create({
        data: {
          cmg_id: cmgUpload._id.toString(),
          file_type: cmgUpload.file_type || null,
          genome_type: cmgUpload.genomeType || null,
          genome_value: cmgUpload.genomeValue || null,
          source_run: sourceRun,
          notes: cmgUpload.notes || null,
          metadata: {
            cmg_upload_id: cmgUpload._id.toString(),
            path: cmgUpload.path || null,
            status: cmgUpload.status || null,
            file_count: cmgUpload.file_count || null,
          },
          audit_log_id: auditLog.id,
          created_at: cmgUpload.createdAt || new Date(),
          updated_at: cmgUpload.updatedAt || new Date(),
        },
      });
      
      createdCount++;
      logger.debug(`[BIGBANG] Created import log for dataset ${bioloopDataset.name} from CMG upload ${cmgUpload._id}`);
      
    } catch (error) {
      logger.error(`[BIGBANG] Failed to process CMG upload ${cmgUpload._id.toString()}:`);
      logger.error(`  Error: ${error.message}`);
      logger.error(`  Code: ${error.code || 'N/A'}`);
      logger.error(`  Dataset: ${bioloopDataset ? bioloopDataset.name : 'N/A'}`);
      if (error.stack) {
        logger.debug(`  Stack: ${error.stack}`);
      }
      skippedCount++;
    }
    
    processedCount++;
    
    // Progress logging
    if (processedCount % 100 === 0) {
      logger.info(`[BIGBANG] Processed ${processedCount}/${totalCount} upload records (${Math.round(processedCount / totalCount * 100)}%)`);
    }
  }
  
  logger.info(`[BIGBANG] Import logs conversion complete: ${createdCount} created, ${skippedCount} skipped`);
}

module.exports = {
  syncImportLogs,
};

