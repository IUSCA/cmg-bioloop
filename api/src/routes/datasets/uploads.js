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
const { accessControl } = require('@/middleware/auth');
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

      // Set origin_path for the dataset
      await tx.dataset.update({
        where: { id: createdDataset.id },
        data: {
          origin_path: datasetService.getUploadedDatasetPath({ datasetId: createdDataset.id, datasetType: type }),
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
    body('tus_id').isString().notEmpty(),
    body('selection_mode').optional().isString(),
    body('directory_name').optional().isString(),
    body('relative_path').optional().isString(),
  ],
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Mark TUS upload as complete and prepare for processing'

    const datasetId = parseInt(req.params.id, 10);
    const { tus_id, selection_mode, directory_name, relative_path } = req.body;

    logger.info(`Complete upload request for dataset ${datasetId}, tus_id: ${tus_id}`);

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

      // Get TUS upload info to verify completion and get file details
      const uploadPath = config.get('upload.path');
      const tusFilePath = path.join(uploadPath, tus_id);
      const tusInfoPath = `${tusFilePath}.info`;

      // Check if TUS files exist
      if (!fs.existsSync(tusFilePath)) {
        logger.error(`TUS file not found: ${tusFilePath}`);
        return res.status(404).json({ error: 'Upload file not found' });
      }

      // Read TUS metadata
      let tusMetadata = {};
      let fileSize = 0;
      
      if (fs.existsSync(tusInfoPath)) {
        const infoContent = fs.readFileSync(tusInfoPath, 'utf8');
        tusMetadata = JSON.parse(infoContent);
        logger.info(`TUS metadata: ${JSON.stringify(tusMetadata)}`);
      }

      // Get file size
      const stats = fs.statSync(tusFilePath);
      fileSize = stats.size;

      // Determine final file path
      let finalPath = tusFilePath;
      
      // For directory uploads, preserve directory structure
      if (selection_mode === 'directory' && relative_path) {
        const datasetUploadDir = path.join(
          uploadPath,
          `dataset_${datasetId}`,
          directory_name || 'upload',
        );
        
        finalPath = path.join(datasetUploadDir, relative_path);
        
        logger.info(`Preserving directory structure: ${tusFilePath} -> ${finalPath}`);
        
        // Create parent directory if needed
        const parentDir = path.dirname(finalPath);
        if (!fs.existsSync(parentDir)) {
          fs.mkdirSync(parentDir, { recursive: true });
        }
        
        // Move file to preserve structure
        fs.renameSync(tusFilePath, finalPath);
        
        logger.info(`File moved to ${finalPath}`);
      }

      // Update upload log
      const updateData = {
        status: CONSTANTS.UPLOAD_STATUSES.UPLOADED,
        tus_id,
        file_path: finalPath,
        file_size: BigInt(fileSize),
        selection_mode: selection_mode || 'files',
        directory_name,
        updated_at: new Date(),
      };

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
        await prisma.dataset_upload_log.updateMany({
          where: {
            audit_log: {
              dataset_id: datasetId,
              create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
            },
          },
          data: {
            status: CONSTANTS.UPLOAD_STATUSES.PROCESSING_FAILED,
            failure_reason: error.message,
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

router.post(
  '/:id/workflow/:wf',
  // Verify if this user is allowed to initiate the requested workflow on
  // the requested dataset.
  datasetService.workflow_access_check,
  validate([
    param('id').isInt().toInt(),
    param('wf').isIn([
      CONSTANTS.WORKFLOWS.PROCESS_DATASET_UPLOAD,
    ]),
  ]),
  asyncHandler(async (req, res, next) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = Create and start a workflow to process a Dataset's upload, and associate it with the uploaded
    // Dataset.

    const wf_name = req.params.wf;

    const dataset = await datasetService.get_dataset({
      id: req.params.id,
      workflows: true,
    });

    if (!dataset) {
      return next(createError(404, 'Dataset not found'));
    }

    if (wf_name !== CONSTANTS.WORKFLOWS.PROCESS_DATASET_UPLOAD) {
      return next(createError(400, 'Invalid workflow name'));
    }

    datasetService.initiateUploadWorkflow({
      dataset,
      requestedWorkflow: wf_name,
      user: req.user,
    }).then(({ workflowInitiated, workflowInitiationError }) => {
      if (workflowInitiated) {
        return res.json(workflowInitiated);
      }
      next(createError.InternalServerError(workflowInitiationError));
    }).catch(next);
  }),
);

module.exports = router;
