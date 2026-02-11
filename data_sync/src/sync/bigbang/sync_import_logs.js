const { ObjectId } = require('mongodb');
const logger = require('../../logger');

/**
 * Convert CMG uploads to Bioloop dataset_import_log entries
 * CMG's "Upload" feature = Bioloop's "Import" feature (register from remote filesystem)
 * 
 * CRITICAL UNDERSTANDING OF CMG MODEL:
 * -----------------------------------
 * - Uploads CREATE new dataproducts (they don't just reference existing ones)
 * - upload.dataset = optional reference to SOURCE sequencing run (RAW_DATA) for context
 * - upload.dataproduct = optional reference to SOURCE dataproduct (parent lineage)
 * - dataproduct.upload = links the NEW dataproduct back to the upload that created it
 * 
 * Therefore, to find what an upload created:
 *   Find dataproduct WHERE dataproduct.upload = upload._id
 * 
 * One upload can create MULTIPLE dataproducts (e.g., uploading multiple files).
 * Each dataproduct created gets its own import log entry.
 * 
 * Equivalent to what would be created when using Bioloop's Import feature.
 */
async function syncImportLogs(prisma, cmgDb, cmgUserId) {
  logger.info('[BIGBANG] Converting CMG upload history to Bioloop import logs...');
  logger.info('[BIGBANG] CORRECTED APPROACH: Finding dataproducts created BY each upload');
  logger.info('[BIGBANG] (upload.dataset and upload.dataproduct are SOURCE references, not created targets)');
  
  const uploadsCollection = cmgDb.collection('uploads');
  const dataproductsCollection = cmgDb.collection('dataproducts');
  
  // Get total count for progress tracking
  const totalUploads = await uploadsCollection.countDocuments({});
  logger.info(`[BIGBANG] Found ${totalUploads} CMG upload records to process`);
  
  let processedUploads = 0;
  let totalCreatedImportLogs = 0;
  let totalSkippedDataproducts = 0;
  const skipReasons = {};  // Track reasons for skipping
  const BATCH_SIZE = 100;
  
  // Use cursor to stream data instead of loading all at once
  const cursor = uploadsCollection.find({}).batchSize(BATCH_SIZE);
  
  for await (const cmgUpload of cursor) {
    try {
      const uploadId = cmgUpload._id.toString();
      
      // CORRECT APPROACH: Find the dataproduct(s) that this upload CREATED
      // In CMG: dataproduct.upload points back to the upload that created it
      const createdDataproducts = await dataproductsCollection.find({
        upload: cmgUpload._id
      }).toArray();
      
      if (createdDataproducts.length === 0) {
        // This upload didn't create any dataproducts (orphaned or failed upload)
        if (totalSkippedDataproducts < 5) {
          logger.warn(`[BIGBANG] Upload ${uploadId} didn't create any dataproducts`);
          logger.warn(`  This suggests the upload failed or was never completed`);
        }
        
        skipReasons['no_dataproduct_created'] = (skipReasons['no_dataproduct_created'] || 0) + 1;
        totalSkippedDataproducts++;
        processedUploads++;
        continue;
      }
      
      logger.debug(`[BIGBANG] Upload ${uploadId} created ${createdDataproducts.length} dataproduct(s)`);
      
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
      
      // Determine source_run (source dataset for derived dataproducts)
      // In CMG: upload.dataset references the source RAW_DATA sequencing run
      let sourceRun = null;
      if (cmgUpload.dataset) {
        const sourceDataset = await prisma.dataset.findFirst({
          where: { cmg_id: cmgUpload.dataset.toString() },
        });
        if (sourceDataset) {
          sourceRun = sourceDataset.name;
        }
      }
      
      // Process each dataproduct created by this upload
      for (const createdDataproduct of createdDataproducts) {
        try {
          const dataproductCmgId = createdDataproduct._id.toString();
          
          // Find the corresponding Bioloop dataset (DATA_PRODUCT type)
          const bioloopDataset = await prisma.dataset.findFirst({
            where: { cmg_id: dataproductCmgId },
          });
          
          if (!bioloopDataset) {
            if (totalSkippedDataproducts < 10) {
              logger.warn(`[BIGBANG] Dataproduct ${dataproductCmgId} not found in Bioloop`);
              logger.warn(`  Created by upload ${uploadId}, name: ${createdDataproduct.name || 'N/A'}`);
            }
            
            skipReasons['dataproduct_not_in_bioloop'] = (skipReasons['dataproduct_not_in_bioloop'] || 0) + 1;
            totalSkippedDataproducts++;
            continue;
          }
          
          // Check if an import log already exists for this specific upload+dataproduct combination
          // (to support idempotent re-runs)
          const existingImportLog = await prisma.dataset_import_log.findFirst({
            where: {
              cmg_id: uploadId,
              metadata: {
                path: ['cmg_dataproduct_id'],
                equals: dataproductCmgId,
              },
            },
          });
          
          if (existingImportLog) {
            logger.debug(`[BIGBANG] Import log already exists for upload ${uploadId} → dataproduct ${dataproductCmgId}`);
            
            skipReasons['already_exists_idempotency'] = (skipReasons['already_exists_idempotency'] || 0) + 1;
            totalSkippedDataproducts++;
            continue;
          }
          
          // Ensure dataset has create_method set to IMPORT
          // This will be transferred from audit log during migration, but for new syncs we set it directly
          if (!bioloopDataset.create_method) {
            await prisma.dataset.update({
              where: { id: bioloopDataset.id },
              data: { create_method: 'IMPORT' },
            });
          }
          
          // Create or find 'create' audit log for this dataset (for user tracking)
          let auditLog = await prisma.dataset_audit.findFirst({
            where: {
              action: 'create',
              dataset_id: bioloopDataset.id,
            },
          });
          
          if (!auditLog) {
            // Create a 'create' audit log entry if it doesn't exist
            auditLog = await prisma.dataset_audit.create({
              data: {
                action: 'create',
                timestamp: cmgUpload.createdAt || new Date(),
                dataset_id: bioloopDataset.id,
                user_id: userId,
              },
            });
          }
          
          // Create the import log entry linked directly to dataset
          // Build upload_data object, filtering out undefined values (Prisma doesn't allow explicit undefined)
          const uploadData = {};
          if (cmgUpload.createdAt !== undefined) uploadData.createdAt = cmgUpload.createdAt;
          if (cmgUpload.updatedAt !== undefined) uploadData.updatedAt = cmgUpload.updatedAt;
          if (cmgUpload.selected !== undefined) uploadData.selected = cmgUpload.selected;
          if (cmgUpload.path !== undefined) uploadData.path = cmgUpload.path;
          if (cmgUpload.status !== undefined) uploadData.status = cmgUpload.status;
          if (cmgUpload.file_count !== undefined) uploadData.file_count = cmgUpload.file_count;
          
          await prisma.dataset_import_log.create({
            data: {
              cmg_id: uploadId,
              source_run: sourceRun,
              notes: cmgUpload.notes || null,
              metadata: {
                cmg_upload_id: uploadId,
                cmg_dataproduct_id: dataproductCmgId, // Track which dataproduct was created
                cmg_source_dataset: cmgUpload.dataset ? cmgUpload.dataset.toString() : null,
                cmg_source_dataproduct: cmgUpload.dataproduct ? cmgUpload.dataproduct.toString() : null,
                cmg_upload_data: uploadData,
                file_type: cmgUpload.file_type || null,
                genome_type: cmgUpload.genomeType || null,
                genome_value: cmgUpload.genomeValue || null,
              },
              dataset_id: bioloopDataset.id,
            },
          });
          
          totalCreatedImportLogs++;
          logger.debug(`[BIGBANG] Created import log: upload ${uploadId} → dataproduct ${dataproductCmgId} (${bioloopDataset.name})`);
          
        } catch (dataproductError) {
          logger.error(`[BIGBANG] Failed to process dataproduct ${createdDataproduct._id} from upload ${uploadId}:`);
          logger.error(`  Error: ${dataproductError.message}`);
          logger.error(`  Code: ${dataproductError.code || 'N/A'}`);
          
          totalSkippedDataproducts++;
        }
      } // End of for loop over created dataproducts
      
    } catch (uploadError) {
      logger.error(`[BIGBANG] Failed to process CMG upload ${cmgUpload._id}:`);
      logger.error(`  Error: ${uploadError.message}`);
      logger.error(`  Code: ${uploadError.code || 'N/A'}`);
      
      totalSkippedDataproducts++;
    }
    
    processedUploads++;
    
    // Log progress every 100 records
    if (processedUploads % 100 === 0) {
      logger.info(`[BIGBANG] Processed ${processedUploads}/${totalUploads} uploads (${Math.round(processedUploads / totalUploads * 100)}%)`);
      logger.info(`[BIGBANG]   Created ${totalCreatedImportLogs} import logs so far`);
    }
  }
  
  // Log detailed summary
  logger.info(`[BIGBANG] Import logs conversion complete:`);
  logger.info(`  - Processed uploads: ${processedUploads}`);
  logger.info(`  - Created import logs: ${totalCreatedImportLogs}`);
  logger.info(`  - Skipped dataproducts: ${totalSkippedDataproducts}`);
  
  // Show breakdown of skip reasons
  if (Object.keys(skipReasons).length > 0) {
    logger.info('[BIGBANG] Skip reasons breakdown:');
    Object.entries(skipReasons).forEach(([reason, count]) => {
      logger.info(`  - ${reason}: ${count} records`);
    });
  }
  
  // Provide context for common issues
  if (skipReasons['no_dataproduct_created']) {
    logger.warn('[BIGBANG] Many uploads have no associated dataproducts.');
    logger.warn('  This suggests uploads were incomplete, failed, or the dataproducts were deleted.');
  }
  
  if (skipReasons['dataproduct_not_in_bioloop']) {
    logger.warn('[BIGBANG] Some dataproducts referenced by uploads were not found in Bioloop.');
    logger.warn('  This could mean:');
    logger.warn('  1. The dataproducts failed to migrate in step 5');
    logger.warn('  2. The dataproducts were filtered out during migration');
    logger.warn('  3. The CMG data has integrity issues');
  }
}

module.exports = {
  syncImportLogs,
};
