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
      project_id, src_instrument_id, src_dataset_id, name, type,
      file_type, genome_type, genome_value,
    } = req.body;

    const datasetCreateQuery = await datasetService.buildDatasetCreateQuery({
      name,
      type,
      project_id,
      user_id: req.user.id,
      src_instrument_id,
      src_dataset_id,
      create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
      file_type,
      genome_type,
      genome_value,
    });

    const dataset_upload_log = await prisma.$transaction(async (tx) => {
      const createdDataset = await datasetService.create(tx, datasetCreateQuery);

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

      const updated_dataset_upload_log = await tx.dataset_upload_log.findUnique({
        where: { id: created_dataset_upload_log.id },
        include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
      });
      return updated_dataset_upload_log;
    });

    res.json(dataset_upload_log);
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

    logger.info(`Complete upload request for dataset ${datasetId}, process_id: ${process_id}`);

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
        logger.error(`No upload log found for dataset ${datasetId}`);
        return res.status(404).json({ error: 'Upload log not found' });
      }

      // Idempotency: If already UPLOADED, return success immediately
      if (uploadLog.status === CONSTANTS.UPLOAD_STATUSES.UPLOADED) {
        logger.info(`Upload for dataset ${datasetId} already completed`);
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
        logger.error(`TUS file not found: ${tusFilePath}`);
        return res.status(404).json({ error: 'Upload file not found' });
      }

      // Read TUS metadata
      let tusMetadata = {};
      let fileSize = 0;
      let originalFilename = 'uploaded_file';

      if (fs.existsSync(tusInfoPath)) {
        const infoContent = fs.readFileSync(tusInfoPath, 'utf8');
        tusMetadata = JSON.parse(infoContent);
        // TUS stores metadata in lowercase 'metadata' field
        originalFilename = tusMetadata.metadata?.filename || tusMetadata.metadata?.name || originalFilename;
        logger.info(`TUS metadata: ${JSON.stringify(tusMetadata)}`);
      }

      // Get file size
      const stats = fs.statSync(tusFilePath);
      fileSize = stats.size;

      // Use the origin_path that was set at dataset creation
      // (dataset-specific directory: /uploads/{type}/{id}/)
      const baseOriginPath = dataset.origin_path;

      // Determine final file path based on upload mode
      let finalPath;

      if (selection_mode === 'directory' && relative_path) {
        // Directory upload: preserve directory structure under dataset's origin_path
        const datasetUploadDir = path.join(baseOriginPath, directory_name || 'upload');
        finalPath = path.join(datasetUploadDir, relative_path);

        logger.info(`Directory upload: ${tusFilePath} -> ${finalPath}`);

        // Create parent directory if needed
        const parentDir = path.dirname(finalPath);
        if (!fs.existsSync(parentDir)) {
          fs.mkdirSync(parentDir, { recursive: true });
        }

        // Move file (idempotent: skip if already exists at destination)
        if (!fs.existsSync(finalPath)) {
          fs.renameSync(tusFilePath, finalPath);
          logger.info(`File moved to ${finalPath}`);
        } else {
          logger.info(`File already exists at ${finalPath}, skipping move`);
        }
      } else {
        // Single file upload: move to dataset's origin_path
        finalPath = path.join(baseOriginPath, originalFilename);

        logger.info(`Single file upload: ${tusFilePath} -> ${finalPath}`);

        // Create dataset directory if needed
        if (!fs.existsSync(baseOriginPath)) {
          fs.mkdirSync(baseOriginPath, { recursive: true });
        }

        // Move file (idempotent: skip if already exists at destination)
        if (!fs.existsSync(finalPath)) {
          fs.renameSync(tusFilePath, finalPath);
          logger.info(`File moved to ${finalPath}`);
        } else {
          logger.info(`File already exists at ${finalPath}, skipping move`);
        }
      }

      logger.info(`File ready at ${finalPath} (using origin_path: ${baseOriginPath})`);

      // Update upload log
      const updateData = {
        status: CONSTANTS.UPLOAD_STATUSES.UPLOADED,
        process_id,
        selection_mode: selection_mode || 'files',
        directory_name,
        updated_at: new Date(),
      };

      // Add metadata if provided (e.g., checksum from UI)
      if (metadata) {
        // Merge with existing metadata
        updateData.metadata = {
          ...(uploadLog.metadata || {}),
          ...metadata,
        };
      }

      // Update upload log with file info (origin_path already set at dataset creation)
      const updatedLog = await prisma.dataset_upload_log.update({
        where: { id: uploadLog.id },
        data: updateData,
        include: CONSTANTS.INCLUDE_DATASET_UPLOAD_LOG_RELATIONS,
      });

      logger.info(`Updated dataset_upload_log for dataset ${datasetId}: status=${updatedLog.status}`);

      res.json({
        success: true,
        upload_log: updatedLog,
      });
    } catch (error) {
      logger.error(`Failed to complete upload for dataset ${datasetId}:`, error);

      // Try to update status to failed
      try {
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
      } catch (updateError) {
        logger.error('Failed to update upload log status:', updateError);
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
      return res.status(404).json({ error: 'Upload log not found' });
    }

    // Build update data
    const updateData = {};

    // Merge metadata (preserve existing fields)
    if (metadata) {
      const existingMetadata = uploadLog.metadata || {};
      updateData.metadata = { ...existingMetadata, ...metadata };
    }

    // Update status if provided
    if (status) {
      updateData.status = status;
    }

    // Update retry_count if provided
    if (retry_count !== undefined) {
      updateData.retry_count = retry_count;
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

    res.json({ success: true, upload_log: updated });
  }),
);

module.exports = router;
