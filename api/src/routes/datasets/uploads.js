const express = require('express');
const createError = require('http-errors');
const {
  query, param, body,
} = require('express-validator');
const _ = require('lodash/fp');
const { Prisma } = require('@prisma/client');
const config = require('config');
const fs = require('fs');
const path = require('path');

const asyncHandler = require('@/middleware/asyncHandler');
const { accessControl, authenticate } = require('@/middleware/auth');
const { validate } = require('@/middleware/validators');
const datasetService = require('@/services/dataset');
const CONSTANTS = require('@/constants');
const logger = require('@/services/logger');
const prisma = require('@/db');

const isPermittedTo = accessControl('datasets');

const router = express.Router();

// Used by:
//  - UI
//  - Workers
router.get(
  '/',
  validate([
    query('status').isIn(Object.values(CONSTANTS.UPLOAD_STATUSES)).optional(),
    query('dataset_name').optional().trim().isLength({ min: 1 }),
    query('limit').isInt({ min: 1 }).toInt().optional(),
    query('offset').isInt({ min: 0 }).toInt().optional(),
  ]),
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Retrieve past uploads'

    const {
      status, dataset_name, offset, limit,
    } = req.query;

    // Only show uploads that have a process_id (TUS upload was registered)
    // This hides incomplete/orphaned uploads from users
    const where = {
      process_id: { not: null },
    };
    if (status) {
      where.status = status;
    }
    if (dataset_name) {
      where.audit_log = {
        dataset: {
          name: {
            contains: dataset_name,
            mode: 'insensitive',
          },
        },
      };
    }

    const filter_query = {
      skip: offset ?? Prisma.skip,
      take: limit ?? Prisma.skip,
      where,
      orderBy: {
        audit_log: {
          timestamp: 'desc',
        },
      },
    };

    const [dataset_upload_logs, count] = await prisma.$transaction([
      prisma.dataset_upload_log.findMany({
        ...filter_query,
        include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
      }),
      prisma.dataset_upload_log.count({ where }),
    ]);

    res.json({ metadata: { count }, uploads: dataset_upload_logs });
  }),
);

// Used by UI
router.get(
  '/:username',
  validate([
    query('status').isIn(Object.values(CONSTANTS.UPLOAD_STATUSES)).optional(),
    query('dataset_name').optional().trim().isLength({ min: 1 }),
    query('limit').isInt({ min: 1 }).toInt().optional(),
    query('offset').isInt({ min: 0 }).toInt().optional(),
    param('username').trim().notEmpty(),
  ]),
  isPermittedTo('read', { checkOwnership: true }),
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Retrieve past uploads for a specific user'

    const {
      status, dataset_name, offset, limit,
    } = req.query;

    const where = {};
    if (status) {
      where.status = status;
    }
    if (dataset_name) {
      where.audit_log = {
        dataset: {
          name: {
            contains: dataset_name,
            mode: 'insensitive',
          },
        },
      };
    }
    where.audit_log = {
      user: {
        username: req.params.username,
      },
    };

    const filter_query = {
      skip: offset ?? Prisma.skip,
      take: limit ?? Prisma.skip,
      where,
      orderBy: {
        audit_log: {
          timestamp: 'desc',
        },
      },
    };

    const [dataset_upload_logs, count] = await prisma.$transaction([
      prisma.dataset_upload_log.findMany({
        ...filter_query,
        include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
      }),
      prisma.dataset_upload_log.count({ where }),
    ]);

    res.json({ metadata: { count }, uploads: dataset_upload_logs });
  }),
);

// - Register an uploaded dataset in the system (TUS Upload)
// - Used by UI
router.post(
  '/',
  isPermittedTo('create'),
  validate([
    body('type').trim().notEmpty().isIn(config.dataset_types),
    body('name').trim().notEmpty().isLength({ min: 3 }),
    body('src_dataset_id').optional().isInt().toInt(),
    body('source_data_product_id').optional().isInt().toInt(),
    body('project_id').optional(),
    body('src_instrument_id').optional(),
    body('file_type').optional(),
    body('genome_type').optional(),
    body('genome_value').optional(),
  ]),
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Register an uploaded dataset in the system (TUS Upload)'

    const {
      project_id, src_instrument_id, src_dataset_id, source_data_product_id, name, type,
      file_type, genome_type, genome_value,
    } = req.body;

    logger.info(`[UPLOAD-CREATE] Starting dataset upload registration`, {
      user: req.user?.username,
      user_id: req.user?.id,
      dataset_name: name,
      dataset_type: type,
      project_id,
      src_instrument_id,
      src_dataset_id,
    });

    try {
      const datasetCreateQuery = await datasetService.buildDatasetCreateQuery({
        name,
        type,
        project_id,
        user_id: req.user.id,
        src_instrument_id,
        src_dataset_id,
        source_data_product_id,
        create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
        file_type,
        genome_type,
        genome_value,
      });

      logger.info(`[UPLOAD-CREATE] Dataset create query built successfully for '${name}'`);

      const dataset_upload_log = await prisma.$transaction(async (tx) => {
        logger.info(`[UPLOAD-CREATE] Starting database transaction for '${name}'`);
        
        const createdDataset = await datasetService.create(tx, datasetCreateQuery);
        logger.info(`[UPLOAD-CREATE] Dataset created`, {
          dataset_id: createdDataset.id,
          dataset_name: createdDataset.name,
          dataset_type: createdDataset.type,
        });

        // Find the audit_log that was created by datasetService.create()
        const audit_log = await tx.dataset_audit.findUniqueOrThrow({
          where: {
            dataset_id_create_method: {
              dataset_id: createdDataset.id,
              create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
            },
          },
          select: {
            id: true,
          },
        });

        logger.info(`[UPLOAD-CREATE] Audit log found`, {
          audit_log_id: audit_log.id,
          dataset_id: createdDataset.id,
        });

        // Set origin_path to the predetermined location where files will be moved to
        // This is deterministic and doesn't depend on the /complete endpoint
        // Format: /uploads/{type}/{id}/{name}
        const uploadBasePath = config.get('upload.path');
        const datasetOriginPath = path.join(
          uploadBasePath,
          type.toLowerCase(),
          `${createdDataset.id}`,
          createdDataset.name,
        );
        
        await tx.dataset.update({
          where: { id: createdDataset.id },
          data: {
            origin_path: datasetOriginPath,
          },
        });

        logger.info(`[UPLOAD-CREATE] Origin path set`, {
          dataset_id: createdDataset.id,
          origin_path: datasetOriginPath,
        });

        // Create dataset_upload_log (TUS handles file tracking internally)
        const created_dataset_upload_log = await tx.dataset_upload_log.create({
          data: {
            status: CONSTANTS.UPLOAD_STATUSES.UPLOADING,
            audit_log: {
              connect: {
                id: audit_log.id,
              },
            },
          },
          select: {
            id: true,
          },
        });

        logger.info(`[UPLOAD-CREATE] Upload log created`, {
          upload_log_id: created_dataset_upload_log.id,
          dataset_id: createdDataset.id,
          initial_status: CONSTANTS.UPLOAD_STATUSES.UPLOADING,
        });

        const updated_dataset_upload_log = await tx.dataset_upload_log.findUnique({
          where: { id: created_dataset_upload_log.id },
          include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
        });
        
        logger.info(`[UPLOAD-CREATE] Transaction complete, returning upload log`, {
          upload_log_id: updated_dataset_upload_log.id,
          dataset_id: createdDataset.id,
        });
        
        return updated_dataset_upload_log;
      });

      logger.info(`[UPLOAD-CREATE] SUCCESS: Dataset upload registered`, {
        upload_log_id: dataset_upload_log.id,
        dataset_id: dataset_upload_log.audit_log.dataset.id,
        dataset_name: dataset_upload_log.audit_log.dataset.name,
        user: req.user?.username,
      });

      res.json(dataset_upload_log);
    } catch (error) {
      logger.error(`[UPLOAD-CREATE] FAILED: Error registering dataset upload`, {
        user: req.user?.username,
        dataset_name: name,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }),
);

// - Complete a TUS upload (called after UI finishes uploading)
// - Updates dataset_upload_log, moves files, prepares for workflow
router.post(
  '/:id/complete',
  isPermittedTo(
    'update',
    { checkOwnership: true },
    async (req, res, next) => { // resourceOwnerFn
      try {
        const dataset_creator = await datasetService.get_dataset_creator({ dataset_id: parseInt(req.params.id, 10) });
        return dataset_creator.username;
      } catch (error) {
        logger.error(error);
        return next(createError.InternalServerError());
      }
    },
  ),
  [
    body('process_id').isString().notEmpty(),
    body('selection_mode').optional().isString(),
    body('directory_name').optional().isString(),
    body('relative_path').optional().isString(),
    body('metadata').optional().isObject(),
  ],
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Mark TUS upload as complete and prepare for processing'

    const datasetId = parseInt(req.params.id, 10);
    const {
      process_id, selection_mode, directory_name, relative_path, metadata,
    } = req.body;

    logger.info(`[UPLOAD-COMPLETE] Starting upload completion`, {
      dataset_id: datasetId,
      process_id,
      selection_mode,
      directory_name,
      relative_path,
      has_metadata: !!metadata,
      user: req.user?.username,
    });

    try {
      // Find the upload log
      const uploadLog = await prisma.dataset_upload_log.findFirst({
        where: {
          audit_log: {
            dataset_id: datasetId,
            create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
          },
        },
        include: {
          audit_log: {
            include: {
              dataset: true,
            },
          },
        },
      });

      if (!uploadLog) {
        logger.error(`[UPLOAD-COMPLETE] FAILED: No upload log found`, {
          dataset_id: datasetId,
          process_id,
          user: req.user?.username,
        });
        return res.status(404).json({ error: 'Upload log not found' });
      }

      logger.info(`[UPLOAD-COMPLETE] Upload log found`, {
        upload_log_id: uploadLog.id,
        dataset_id: datasetId,
        current_status: uploadLog.status,
        dataset_name: uploadLog.audit_log.dataset.name,
      });

      // Idempotency: If already UPLOADED, return success immediately
      if (uploadLog.status === CONSTANTS.UPLOAD_STATUSES.UPLOADED) {
        logger.info(`[UPLOAD-COMPLETE] Idempotent request - already completed`, {
          dataset_id: datasetId,
          upload_log_id: uploadLog.id,
          process_id,
        });
        return res.json({
          success: true,
          upload_log: uploadLog,
        });
      }

      // Get dataset type for proper path structure
      const dataset = uploadLog.audit_log.dataset;
      const datasetType = dataset.type;

      // Get TUS upload info to verify completion and get file details
      const uploadPath = config.get('upload.path');
      const tusFilePath = path.join(uploadPath, process_id);
      // TUS stores metadata as .json (not .info)
      const tusInfoPath = `${tusFilePath}.json`;

      // Check if TUS files exist
      if (!fs.existsSync(tusFilePath)) {
        logger.error(`[UPLOAD-COMPLETE] FAILED: TUS file not found`, {
          dataset_id: datasetId,
          process_id,
          expected_path: tusFilePath,
        });
        return res.status(404).json({ error: 'Upload file not found' });
      }

      logger.info(`[UPLOAD-COMPLETE] TUS file found`, {
        dataset_id: datasetId,
        process_id,
        tus_file_path: tusFilePath,
      });

      // Read TUS metadata
      let tusMetadata = {};
      let fileSize = 0;
      let originalFilename = 'uploaded_file';

      if (fs.existsSync(tusInfoPath)) {
        const infoContent = fs.readFileSync(tusInfoPath, 'utf8');
        tusMetadata = JSON.parse(infoContent);
        // TUS stores metadata in lowercase 'metadata' field
        originalFilename = tusMetadata.metadata?.filename || tusMetadata.metadata?.name || originalFilename;
        logger.info(`[UPLOAD-COMPLETE] TUS metadata read`, {
          dataset_id: datasetId,
          process_id,
          filename: originalFilename,
          metadata: tusMetadata.metadata,
        });
      } else {
        logger.warn(`[UPLOAD-COMPLETE] TUS info file not found (using defaults)`, {
          dataset_id: datasetId,
          process_id,
          expected_path: tusInfoPath,
        });
      }

      // Get file size
      const stats = fs.statSync(tusFilePath);
      fileSize = stats.size;
      
      logger.info(`[UPLOAD-COMPLETE] File size determined`, {
        dataset_id: datasetId,
        process_id,
        file_size_bytes: fileSize,
        file_size_mb: (fileSize / (1024 * 1024)).toFixed(2),
      });

      // Use the origin_path that was set at dataset creation
      // (dataset-specific directory: /uploads/{type}/{id}/)
      const baseOriginPath = dataset.origin_path;

      // Determine final file path based on upload mode
      let finalPath;

      if (selection_mode === 'directory' && relative_path) {
        // Directory upload: preserve directory structure under dataset's origin_path
        const datasetUploadDir = path.join(baseOriginPath, directory_name || 'upload');
        finalPath = path.join(datasetUploadDir, relative_path);

        logger.info(`[UPLOAD-COMPLETE] Directory upload mode`, {
          dataset_id: datasetId,
          process_id,
          directory_name,
          relative_path,
          source: tusFilePath,
          destination: finalPath,
        });

        // Create parent directory if needed
        const parentDir = path.dirname(finalPath);
        if (!fs.existsSync(parentDir)) {
          logger.info(`[UPLOAD-COMPLETE] Creating parent directory`, {
            dataset_id: datasetId,
            parent_dir: parentDir,
          });
          fs.mkdirSync(parentDir, { recursive: true });
        }

        // Move file (idempotent: skip if already exists at destination)
        if (!fs.existsSync(finalPath)) {
          logger.info(`[UPLOAD-COMPLETE] Moving file`, {
            dataset_id: datasetId,
            source: tusFilePath,
            destination: finalPath,
          });
          fs.renameSync(tusFilePath, finalPath);
          logger.info(`[UPLOAD-COMPLETE] File moved successfully`, {
            dataset_id: datasetId,
            destination: finalPath,
          });
        } else {
          logger.info(`[UPLOAD-COMPLETE] File already exists at destination (idempotent)`, {
            dataset_id: datasetId,
            destination: finalPath,
          });
        }
      } else {
        // Single file upload: move to dataset's origin_path
        finalPath = path.join(baseOriginPath, originalFilename);

        logger.info(`[UPLOAD-COMPLETE] Single file upload mode`, {
          dataset_id: datasetId,
          process_id,
          filename: originalFilename,
          source: tusFilePath,
          destination: finalPath,
        });

        // Create dataset directory if needed
        if (!fs.existsSync(baseOriginPath)) {
          logger.info(`[UPLOAD-COMPLETE] Creating dataset directory`, {
            dataset_id: datasetId,
            directory: baseOriginPath,
          });
          fs.mkdirSync(baseOriginPath, { recursive: true });
        }

        // Move file (idempotent: skip if already exists at destination)
        if (!fs.existsSync(finalPath)) {
          logger.info(`[UPLOAD-COMPLETE] Moving file`, {
            dataset_id: datasetId,
            source: tusFilePath,
            destination: finalPath,
          });
          fs.renameSync(tusFilePath, finalPath);
          logger.info(`[UPLOAD-COMPLETE] File moved successfully`, {
            dataset_id: datasetId,
            destination: finalPath,
          });
        } else {
          logger.info(`[UPLOAD-COMPLETE] File already exists at destination (idempotent)`, {
            dataset_id: datasetId,
            destination: finalPath,
          });
        }
      }

      logger.info(`[UPLOAD-COMPLETE] File is ready`, {
        dataset_id: datasetId,
        final_path: finalPath,
        origin_path: baseOriginPath,
      });

      // Update upload log
      const updateData = {
        status: CONSTANTS.UPLOAD_STATUSES.UPLOADED,
        process_id,
        updated_at: new Date(),
      };

      // Add metadata if provided (e.g., checksum from UI)
      if (metadata) {
        logger.info(`[UPLOAD-COMPLETE] Merging metadata`, {
          dataset_id: datasetId,
          existing_metadata: uploadLog.metadata,
          new_metadata: metadata,
        });
        // Merge with existing metadata
        updateData.metadata = {
          ...(uploadLog.metadata || {}),
          ...metadata,
        };
      }

      logger.info(`[UPLOAD-COMPLETE] Updating upload log`, {
        dataset_id: datasetId,
        upload_log_id: uploadLog.id,
        new_status: CONSTANTS.UPLOAD_STATUSES.UPLOADED,
        process_id,
      });

      // Update upload log with file info (origin_path already set at dataset creation)
      const updatedLog = await prisma.dataset_upload_log.update({
        where: { id: uploadLog.id },
        data: updateData,
        include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
      });

      logger.info(`[UPLOAD-COMPLETE] SUCCESS: Upload completed`, {
        dataset_id: datasetId,
        upload_log_id: updatedLog.id,
        dataset_name: updatedLog.audit_log.dataset.name,
        status: updatedLog.status,
        process_id: updatedLog.process_id,
        user: req.user?.username,
      });

      res.json({
        success: true,
        upload_log: updatedLog,
      });
    } catch (error) {
      logger.error(`[UPLOAD-COMPLETE] FAILED: Error completing upload`, {
        dataset_id: datasetId,
        process_id,
        error: error.message,
        stack: error.stack,
        user: req.user?.username,
      });

      // Try to update status to failed
      try {
        logger.info(`[UPLOAD-COMPLETE] Attempting to mark upload as failed`, {
          dataset_id: datasetId,
        });
        
        // Get existing metadata first
        const existingLog = await prisma.dataset_upload_log.findFirst({
          where: {
            audit_log: {
              dataset_id: datasetId,
              create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
            },
          },
          select: { metadata: true },
        });

        await prisma.dataset_upload_log.updateMany({
          where: {
            audit_log: {
              dataset_id: datasetId,
              create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
            },
          },
          data: {
            status: CONSTANTS.UPLOAD_STATUSES.PROCESSING_FAILED,
            metadata: {
              ...(existingLog?.metadata || {}),
              failure_reason: error.message,
            },
          },
        });
        
        logger.info(`[UPLOAD-COMPLETE] Upload marked as PROCESSING_FAILED`, {
          dataset_id: datasetId,
        });
      } catch (updateError) {
        logger.error(`[UPLOAD-COMPLETE] Failed to update upload log status`, {
          dataset_id: datasetId,
          error: updateError.message,
        });
      }

      return res.status(500).json({ error: 'Failed to complete upload', details: error.message });
    }
  }),
);

// - Update the metadata related to a dataset upload event
// - Used by UI, workers
router.patch(
  '/:id',
  /**
     * A user can only update metadata related to a dataset upload if one of the
     * following two conditions are met:
     *   - The user has either the `admin` or the `operator` role
     *   - The user has the `user` role, and they are the one who uploaded this dataset.
     * This is checked by the `isPermittedTo` middleware.
     */
  isPermittedTo(
    'update',
    { checkOwnership: true },
    async (req, res, next) => { // resourceOwnerFn
      try {
        const dataset_creator = await datasetService.get_dataset_creator({ dataset_id: parseInt(req.params.id, 10) });
        return dataset_creator.username;
      } catch (error) {
        logger.error(error);
        return next(createError.InternalServerError());
      }
    },
  ),
  validate([
    body('status').optional().trim().isIn(Object.values(CONSTANTS.UPLOAD_STATUSES)),
    param('id').isInt().toInt(),
  ]),
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['uploads']
    // #swagger.summary = 'Update the metadata related to a dataset upload event'

    const { status } = req.body;
    const dataset_upload_log_update_query = _.omitBy(_.isUndefined)({
      status,
    });

    const dataset_upload_log = await prisma.$transaction(async (tx) => {
      const dataset_upload_audit_log = await tx.dataset_audit.findUniqueOrThrow({
        where: {
          dataset_id_create_method: {
            dataset_id: req.params.id,
            create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
          },
        },
      });

      let ds_upload_log = await tx.dataset_upload_log.findUniqueOrThrow({
        where: { audit_log_id: dataset_upload_audit_log.id },
      });

      if (Object.entries(dataset_upload_log_update_query).length > 0) {
        await tx.dataset_upload_log.update({
          where: { id: ds_upload_log.id },
          data: dataset_upload_log_update_query,
        });
      }

      ds_upload_log = await tx.dataset_upload_log.findUniqueOrThrow({
        where: { id: ds_upload_log.id },
        include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
      });

      return ds_upload_log;
    });

    res.json(dataset_upload_log);
  }),
);

// Get upload log for a dataset (used by workers)
router.get(
  '/:id/upload-log',
  authenticate,
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Get upload log for a dataset'

    const datasetId = parseInt(req.params.id, 10);

    const uploadLog = await prisma.dataset_upload_log.findFirst({
      where: {
        audit_log: {
          dataset_id: datasetId,
          create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
        },
      },
      include: {
        audit_log: {
          include: {
            dataset: {
              select: {
                id: true,
                name: true,
                type: true,
                origin_path: true,
              },
            },
          },
        },
      },
    });

    if (!uploadLog) {
      return res.status(404).json({ error: 'Upload log not found' });
    }

    res.json(uploadLog);
  }),
);

// Update upload log metadata (e.g., checksum) - used by UI and workers
router.patch(
  '/:id/upload-log',
  authenticate,
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Update dataset upload log metadata'

    const datasetId = parseInt(req.params.id, 10);
    const { metadata, status, retry_count } = req.body;

    logger.info(`[UPLOAD-LOG-UPDATE] Updating upload log`, {
      dataset_id: datasetId,
      has_metadata: !!metadata,
      new_status: status,
      new_retry_count: retry_count,
      user: req.user?.username,
    });

    // Find upload log for this dataset
    const uploadLog = await prisma.dataset_upload_log.findFirst({
      where: {
        audit_log: {
          dataset_id: datasetId,
          create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
        },
      },
    });

    if (!uploadLog) {
      logger.error(`[UPLOAD-LOG-UPDATE] FAILED: Upload log not found`, {
        dataset_id: datasetId,
        user: req.user?.username,
      });
      return res.status(404).json({ error: 'Upload log not found' });
    }

    logger.info(`[UPLOAD-LOG-UPDATE] Upload log found`, {
      upload_log_id: uploadLog.id,
      dataset_id: datasetId,
      current_status: uploadLog.status,
      current_retry_count: uploadLog.retry_count,
    });

    // Build update data
    const updateData = {};

    // Merge metadata (preserve existing fields)
    if (metadata) {
      const existingMetadata = uploadLog.metadata || {};
      updateData.metadata = { ...existingMetadata, ...metadata };
      logger.info(`[UPLOAD-LOG-UPDATE] Merging metadata`, {
        dataset_id: datasetId,
        existing_metadata: existingMetadata,
        new_metadata: metadata,
        merged_metadata: updateData.metadata,
      });
    }

    // Update status if provided
    if (status) {
      updateData.status = status;
      logger.info(`[UPLOAD-LOG-UPDATE] Updating status`, {
        dataset_id: datasetId,
        old_status: uploadLog.status,
        new_status: status,
      });
    }

    // Update retry_count if provided
    if (retry_count !== undefined) {
      updateData.retry_count = retry_count;
      logger.info(`[UPLOAD-LOG-UPDATE] Updating retry count`, {
        dataset_id: datasetId,
        old_retry_count: uploadLog.retry_count,
        new_retry_count: retry_count,
      });
    }

    const updated = await prisma.dataset_upload_log.update({
      where: { id: uploadLog.id },
      data: updateData,
      include: {
        audit_log: {
          include: {
            dataset: {
              select: {
                id: true,
                name: true,
              },
            },
          },
        },
      },
    });

    logger.info(`[UPLOAD-LOG-UPDATE] SUCCESS: Upload log updated`, {
      upload_log_id: updated.id,
      dataset_id: datasetId,
      dataset_name: updated.audit_log.dataset.name,
      new_status: updated.status,
      new_retry_count: updated.retry_count,
      user: req.user?.username,
    });

    res.json({ success: true, upload_log: updated });
  }),
);

module.exports = router;
