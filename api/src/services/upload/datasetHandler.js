/**
 * Dataset Upload Handler
 * 
 * Entity-specific logic for dataset uploads.
 * Handles:
 * - Dataset validation
 * - Upload log updates
 * - Workflow triggering
 */

const path = require('path');
const fs = require('fs').promises;
const prisma = require('@/db');
const logger = require('@/services/logger');
const datasetService = require('@/services/dataset');
const CONSTANTS = require('@/constants');

/**
 * Called when a dataset upload is created (before bytes are uploaded)
 */
async function onUploadCreate(req, res, upload) {
  const datasetId = parseInt(upload.metadata.entity_id, 10);
  
  logger.info(`Dataset upload create: dataset_id=${datasetId}`);
  
  // Validate that dataset exists
  const dataset = await prisma.dataset.findUnique({
    where: { id: datasetId },
  });
  
  if (!dataset) {
    throw new Error(`Dataset ${datasetId} not found`);
  }
  
  // Optional: Check if dataset already has an active upload
  const existingUpload = await prisma.dataset_upload_log.findFirst({
    where: {
      audit_log: {
        dataset_id: datasetId,
        create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
      },
      status: {
        in: ['UPLOADING', 'PROCESSING'],
      },
    },
  });
  
  if (existingUpload) {
    logger.warn(`Dataset ${datasetId} already has an active upload`);
    // Allow it - could be resuming
  }
  
  logger.info(`Dataset ${datasetId} upload validation passed`);
}

/**
 * Called when dataset upload completes successfully
 */
async function onUploadFinish(req, res, upload, filePath, fileSize) {
  const datasetId = parseInt(upload.metadata.entity_id, 10);
  const selectionMode = upload.metadata.selection_mode || 'files';
  const relativePath = upload.metadata.relative_path;
  const directoryName = upload.metadata.directory_name;
  
  logger.info(
    `Dataset upload finish: dataset_id=${datasetId}, ` +
    `mode=${selectionMode}, path=${filePath}`
  );
  
  try {
    // For directory uploads, preserve directory structure
    let finalPath = filePath;
    
    if (selectionMode === 'directory' && relativePath) {
      // Construct final path with directory structure
      const datasetUploadDir = path.join(
        path.dirname(filePath), // Base upload directory
        directoryName || 'uploads',
      );
      
      finalPath = path.join(datasetUploadDir, relativePath);
      
      logger.info(`Preserving directory structure: ${filePath} -> ${finalPath}`);
      
      // Create parent directory if needed
      await fs.mkdir(path.dirname(finalPath), { recursive: true });
      
      // Move file to preserve structure
      await fs.rename(filePath, finalPath);
      
      logger.info(`File moved to ${finalPath}`);
    }
    
    // Update or create dataset_upload_log
    // Note: For multi-file uploads, this will be called multiple times
    // We track the first file's metadata and increment counters
    
    const uploadLog = await prisma.dataset_upload_log.findFirst({
      where: {
        audit_log: {
          dataset_id: datasetId,
          create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
        },
      },
    });
    
    if (!uploadLog) {
      logger.error(`No upload log found for dataset ${datasetId}`);
      throw new Error(`Upload log not found for dataset ${datasetId}`);
    }
    
    // Update upload log (this happens per-file, so we track cumulative info)
    const updateData = {
      status: 'UPLOADED',
      process_id: upload.id, // Store last TUS/upload ID
      selection_mode: selectionMode,
      directory_name: directoryName,
      updated_at: new Date(),
    };
    
    await prisma.dataset_upload_log.update({
      where: { id: uploadLog.id },
      data: updateData,
    });
    
    logger.info(`Updated dataset_upload_log for dataset ${datasetId}`);
    
    // Trigger processing workflow
    // Note: For multi-file uploads, this may be triggered multiple times
    // The workflow should be idempotent or we should track completion
    
    try {
      await triggerProcessWorkflow(datasetId);
      logger.info(`Triggered processing workflow for dataset ${datasetId}`);
    } catch (workflowError) {
      // Log but don't fail the upload - retry job will pick it up
      logger.error(
        `Failed to trigger workflow for dataset ${datasetId}:`,
        workflowError
      );
    }
    
  } catch (error) {
    logger.error(`Failed to process upload for dataset ${datasetId}:`, error);
    
    // Update upload log to failed status
    try {
      await prisma.dataset_upload_log.updateMany({
        where: {
          audit_log: {
            dataset_id: datasetId,
            create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
          },
        },
        data: {
          status: 'PROCESSING_FAILED',
          metadata: { failure_reason: error.message },
        },
      });
    } catch (updateError) {
      logger.error(`Failed to update upload log status:`, updateError);
    }
    
    throw error;
  }
}

module.exports = {
  onUploadCreate,
  onUploadFinish,
};
